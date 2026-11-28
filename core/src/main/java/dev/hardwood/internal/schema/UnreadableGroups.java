/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.LogicalType;

/// The groups of a file's schema whose columns the reader cannot serve under the schema the file
/// declares, each with the leaf ordinals below it: a list of variants in the bare repeated form
/// ([BareRepeatedGroups]) and a `VARIANT` group of a specification version the reader does not
/// implement ([VariantSpecVersions]). The footer parse finds them as it walks the schema elements
/// ([Collector]), and a read checks the columns it touches against them ([#refuseTouched]).
public final class UnreadableGroups {

    /// A group's leaf ordinals, `[firstLeaf, endLeaf)`, and why a read of one is refused.
    private record Group(int firstLeaf, int endLeaf, String message) {
    }

    private final Group[] groups;

    private UnreadableGroups(Group[] groups) {
        this.groups = groups;
    }

    /// Refuses a read that touches a column below one of the groups: the first bare repeated
    /// `VARIANT` group in schema order that it touches, else the first `VARIANT` group of an
    /// unsupported version.
    ///
    /// @param fileName the file the schema was read from, for the message; may be `null`
    /// @param touchedColumns the leaf ordinals, in the schema, the read projects or filters on
    /// @throws UnsupportedOperationException if a touched column lies below an unreadable group
    public void refuseTouched(String fileName, BitSet touchedColumns) {
        for (Group group : groups) {
            int touched = touchedColumns.nextSetBit(group.firstLeaf());
            if (touched >= 0 && touched < group.endLeaf()) {
                throw new UnsupportedOperationException(ExceptionContext.filePrefix(fileName) + group.message());
            }
        }
    }

    /// Gathers the unreadable groups as a walk visits a schema's elements in their order:
    /// [#leaf] for each primitive, [#open] as it enters a group and [#close] as it leaves it, so
    /// the leaves counted in between are the group's.
    public static final class Collector {

        private final List<Group> bareVariants = new ArrayList<>();
        private final List<Group> unsupportedVersions = new ArrayList<>();
        private int leaves;

        /// A group [#open] entered: its slot in each list it belongs to, `-1` for a list it is not in.
        public record Opened(int bareSlot, int versionSlot) {
        }

        /// A group in neither list.
        private static final Opened NEITHER = new Opened(-1, -1);

        /// The walk passed a primitive.
        void leaf() {
            leaves++;
        }

        /// The walk entered the group at `names`, from the root, `bare` where it is
        /// repeated outside a `LIST` or `MAP` group, annotated `logicalType`. A group is listed
        /// where it is entered, so each list holds its groups in schema order.
        Opened open(Deque<String> names, boolean bare, LogicalType logicalType) {
            if (!(logicalType instanceof LogicalType.VariantType variant)) {
                return NEITHER;
            }
            String path = String.join(".", names);
            int bareSlot = bare ? reserve(bareVariants, "Repeated group '" + path + "' is annotated " + variant
                    + " outside a LIST or MAP group; a list of variants in this form is not supported") : -1;
            int versionSlot = variant.specVersion() != VariantSpecVersions.SUPPORTED
                    ? reserve(unsupportedVersions, "Group '" + path + "' is annotated " + variant + "; version "
                            + variant.specVersion() + " of the Variant specification is not supported, the"
                            + " supported version is " + VariantSpecVersions.SUPPORTED)
                    : -1;
            return new Opened(bareSlot, versionSlot);
        }

        /// The walk left the group `opened` entered.
        void close(Opened opened) {
            complete(bareVariants, opened.bareSlot());
            complete(unsupportedVersions, opened.versionSlot());
        }

        /// The groups collected, the bare repeated `VARIANT` groups first; a group without a
        /// leaf, which no read touches, is left out.
        public UnreadableGroups build() {
            List<Group> groups = new ArrayList<>(bareVariants.size() + unsupportedVersions.size());
            addWithLeaves(groups, bareVariants);
            addWithLeaves(groups, unsupportedVersions);
            return new UnreadableGroups(groups.toArray(new Group[0]));
        }

        private int reserve(List<Group> list, String message) {
            list.add(new Group(leaves, -1, message));
            return list.size() - 1;
        }

        private void complete(List<Group> list, int slot) {
            if (slot >= 0) {
                Group group = list.get(slot);
                list.set(slot, new Group(group.firstLeaf(), leaves, group.message()));
            }
        }

        private static void addWithLeaves(List<Group> target, List<Group> source) {
            for (Group group : source) {
                if (group.firstLeaf() < group.endLeaf()) {
                    target.add(group);
                }
            }
        }
    }
}
