/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.schema.FileReferences;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.row.PqFile;

/// Builds a [PqFile] from the struct flyweight over a present `FILE` group, so every accessor
/// that reaches one (the top-level row, a nested struct, a list element, a map value) reads it
/// the same way and applies the same rules.
final class FileValues {

    private FileValues() {
    }

    /// The struct descriptor of a field read as `FILE`, rejecting a field whose group is not
    /// annotated `FILE`.
    static TopLevelFieldMap.FieldDesc.Struct fileDesc(TopLevelFieldMap.FieldDesc desc) {
        return fileDesc(desc, "Field '" + desc.name() + "'");
    }

    /// The struct descriptor of a list element or map value read as `FILE`, `desc` being `null`
    /// for a primitive one.
    ///
    /// @param subject what `desc` describes, for the message
    static TopLevelFieldMap.FieldDesc.Struct fileDesc(TopLevelFieldMap.FieldDesc desc, String subject) {
        if (desc instanceof TopLevelFieldMap.FieldDesc.Struct struct && struct.schema().isFile()) {
            return struct;
        }
        throw new IllegalArgumentException(subject + " is not annotated as FILE");
    }

    /// Reads the reference `group`'s children store.
    ///
    /// @param group the struct flyweight over the present group; its fields are the group's
    ///        children, all projected
    /// @param desc the group's descriptor, which names it in the message
    /// @param batch the batch being read, which supplies the file name and the group's path
    /// @throws ParquetReadException if the stored value is not a valid reference
    static PqFile read(PqStructImpl group, TopLevelFieldMap.FieldDesc.Struct desc, NestedBatchIndex batch) {
        String uri = null;
        String contentType = null;
        String checksum = null;
        BinaryBatchValues inlineValues = null;
        int inlineIndex = 0;
        boolean offsetSet = false;
        long offset = 0;
        boolean sizeSet = false;
        long size = 0;
        int fieldCount = group.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (group.isNull(i)) {
                continue;
            }
            // FileSchema admitted only the children the format names, so every field resolves.
            switch (FileReferences.Child.named(group.getFieldName(i))) {
                case URI -> uri = textOrNull(group.getString(i));
                case OFFSET -> {
                    offsetSet = true;
                    offset = group.getLong(i);
                }
                case SIZE -> {
                    sizeSet = true;
                    size = group.getLong(i);
                }
                case CONTENT_TYPE -> contentType = textOrNull(group.getString(i));
                case CHECKSUM -> checksum = textOrNull(group.getString(i));
                case INLINE -> {
                    inlineValues = group.binaryValuesAt(i);
                    inlineIndex = group.binaryIndexAt(i);
                }
            }
        }
        String violation = FileReferences.violation(inlineValues != null, uri != null, offsetSet, offset, sizeSet, size);
        if (violation != null) {
            throw new ParquetReadException(ExceptionContext.filePrefix(batch.fileName)
                    + "FILE value of column '" + path(desc, batch) + "' is invalid: " + violation);
        }
        // The inline bytes stay in the batch until inline() copies them out.
        return new PqFileView(uri, offsetSet ? offset : PqFileImpl.NOT_SET, sizeSet ? size : PqFileImpl.NOT_SET,
                contentType, checksum, inlineValues, inlineIndex);
    }

    /// The group's dotted path in the file schema, found from its first child's column; every child
    /// of a `FILE` group is a leaf. A group with no children is named by its own name.
    private static String path(TopLevelFieldMap.FieldDesc.Struct desc, NestedBatchIndex batch) {
        if (desc.children().length == 0) {
            // A group with no children has no column to find its path from.
            return desc.schema().name();
        }
        int column = ((TopLevelFieldMap.FieldDesc.Primitive) desc.children()[0]).schema().columnIndex();
        List<String> elements = batch.projectedSchema.getOriginalSchema().getColumn(column).fieldPath().elements();
        return String.join(".", elements.subList(0, elements.size() - 1));
    }

    /// A string field holding the empty string is not set: "a field is *set* when it is present
    /// in the `FILE` group and its value is non-null (and, for string fields, non-empty)".
    private static String textOrNull(String value) {
        return value.isEmpty() ? null : value;
    }
}
