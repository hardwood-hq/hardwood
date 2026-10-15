/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;

/// What parquet-format states about a `FILE`-annotated group (`LogicalTypes.md`, "FILE"): the
/// children it may define, and the rules a value must satisfy to resolve to some bytes. The
/// schema check, the reader, the row writer and the columnar writer all read the rules from
/// here, so they cannot disagree about which values are valid.
public final class FileReferences {

    /// A child a `FILE` group may define, with the one type the format gives it. "Every field is
    /// optional both in the schema and in the data", so each is `OPTIONAL`; "a group need only
    /// define the fields it uses", in any order, since fields are "identified by name case
    /// sensitively, not by field order".
    public enum Child {
        URI("uri", PhysicalType.BYTE_ARRAY, LogicalType.string()),
        OFFSET("offset", PhysicalType.INT64, null),
        SIZE("size", PhysicalType.INT64, null),
        CONTENT_TYPE("content_type", PhysicalType.BYTE_ARRAY, LogicalType.string()),
        CHECKSUM("checksum", PhysicalType.BYTE_ARRAY, LogicalType.string()),
        INLINE("inline", PhysicalType.BYTE_ARRAY, null);

        private static final Child[] VALUES = values();

        private final String fieldName;
        private final PhysicalType type;
        private final LogicalType annotation;

        Child(String fieldName, PhysicalType type, LogicalType annotation) {
            this.fieldName = fieldName;
            this.type = type;
            this.annotation = annotation;
        }

        /// The child's field name in the group.
        public String fieldName() {
            return fieldName;
        }

        /// The child's physical type.
        public PhysicalType type() {
            return type;
        }

        /// The child's annotation, `null` for none.
        public LogicalType annotation() {
            return annotation;
        }

        /// The child named `name`, `null` for a name the format does not define.
        public static Child named(String name) {
            for (Child child : VALUES) {
                if (child.fieldName.equals(name)) {
                    return child;
                }
            }
            return null;
        }

        /// Whether a leaf carrying `logicalType` holds this child. An `INT64` child also admits
        /// `INT(64, signed)`, which annotates the values an unannotated `INT64` holds.
        boolean admits(LogicalType logicalType) {
            return Objects.equals(logicalType, annotation)
                    || type == PhysicalType.INT64 && logicalType instanceof LogicalType.IntType integer
                            && integer.bitWidth() == 64 && integer.isSigned();
        }

        private String describe() {
            if (type == PhysicalType.INT64) {
                return "an OPTIONAL INT64 with no annotation or annotated INT_64";
            }
            return "an OPTIONAL " + type + (annotation != null ? " annotated " + annotation : " with no annotation");
        }
    }

    private static final String CHILD_NAMES = String.join(", ",
            Arrays.stream(Child.values()).map(Child::fieldName).toList());

    private FileReferences() {
    }

    /// Checks that a `FILE` group defines only children the format names, each once and of its
    /// type. A group may define none: "a writer may omit any field from the group definition".
    ///
    /// @param path the group's dotted path, for the message
    /// @throws IllegalArgumentException naming the group and the offending child
    public static void validateGroup(SchemaNode.GroupNode group, String path) {
        Set<Child> seen = EnumSet.noneOf(Child.class);
        for (SchemaNode node : group.children()) {
            Child child = Child.named(node.name());
            if (child == null) {
                throw new IllegalArgumentException("FILE group '" + path + "' has a child '" + node.name()
                        + "' the format does not define; a FILE group's children are " + CHILD_NAMES);
            }
            if (!seen.add(child)) {
                throw new IllegalArgumentException("FILE group '" + path + "' has two children named '"
                        + node.name() + "'");
            }
            if (!(node instanceof SchemaNode.PrimitiveNode leaf)
                    || leaf.repetitionType() != RepetitionType.OPTIONAL
                    || leaf.type() != child.type
                    || !child.admits(leaf.logicalType())) {
                throw new IllegalArgumentException("FILE group '" + path + "' child '" + node.name()
                        + "' must be " + child.describe() + ", found " + describe(node));
            }
        }
    }

    /// The rule a `FILE` value breaks, or `null` if it resolves. A field is set when its child
    /// exists and holds a non-null value; a string field holding the empty string is not set.
    ///
    /// @param inlineSet whether `inline` is set
    /// @param uriSet whether `uri` is set
    /// @param offsetSet whether `offset` is set
    /// @param offset the offset, read only when `offsetSet`
    /// @param sizeSet whether `size` is set
    /// @param size the size, read only when `sizeSet`
    public static String violation(boolean inlineSet, boolean uriSet, boolean offsetSet, long offset,
                                   boolean sizeSet, long size) {
        // "It resolves only if inline or uri is set"
        if (!inlineSet && !uriSet) {
            return "neither 'inline' nor 'uri' is set";
        }
        // "offset may only be set together with uri"; "size must be set whenever offset is set"
        if (offsetSet && !uriSet) {
            return "'offset' is set without 'uri'";
        }
        if (offsetSet && !sizeSet) {
            return "'offset' is set without 'size'";
        }
        // "fields set alongside inline must form a locator valid on its own", and a size alone
        // locates nothing: offset and size "apply only to data referenced by uri"
        if (sizeSet && !uriSet) {
            return "'size' is set without 'uri'";
        }
        // "offset must not be < 0"; size "must be zero or a positive integer if set"
        if (offsetSet && offset < 0) {
            return "'offset' is negative: " + offset;
        }
        if (sizeSet && size < 0) {
            return "'size' is negative: " + size;
        }
        return null;
    }

    /// The rule a `checksum` token breaks, or `null` if it is well formed. A token is
    /// `<algorithm>:<digest>`; for the algorithms the format recognizes with a recomputable digest,
    /// the digest is lowercase hex of the algorithm's length. `ETAG` is opaque, and an algorithm
    /// the format does not recognize is accepted as given, since "readers should ignore unknown
    /// algorithms".
    public static String checksumViolation(String token) {
        int colon = token.indexOf(':');
        if (colon <= 0 || colon == token.length() - 1) {
            return "'checksum' is not of the form <algorithm>:<digest>: " + token;
        }
        int hexLength = switch (token.substring(0, colon)) {
            case "MD5" -> 32;
            case "CRC32", "CRC32C" -> 8;
            case "SHA-256" -> 64;
            default -> -1;
        };
        if (hexLength < 0) {
            return null;
        }
        String digest = token.substring(colon + 1);
        if (digest.length() != hexLength || !isLowercaseHex(digest)) {
            return "'checksum' digest of " + token.substring(0, colon) + " must be " + hexLength
                    + " lowercase hex characters: " + token;
        }
        return null;
    }

    private static boolean isLowercaseHex(String digest) {
        for (int i = 0; i < digest.length(); i++) {
            char c = digest.charAt(i);
            if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
                return false;
            }
        }
        return true;
    }

    private static String describe(SchemaNode node) {
        return switch (node) {
            case SchemaNode.PrimitiveNode leaf -> leaf.repetitionType() + " " + leaf.type()
                    + (leaf.logicalType() != null ? " annotated " + leaf.logicalType() : " with no annotation");
            case SchemaNode.GroupNode group -> "a " + group.repetitionType() + " group";
        };
    }
}
