/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.footer.FileColumnOrdinals;
import dev.hardwood.internal.schema.BareRepeatedGroups;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// The schema checks of a read: the reference schema's own statement of the columns the read
/// touches ([#validateReferenceColumns]), each later file's columns against it
/// ([#validateSchemaCompatibility]), and each row group's chunk list against its file's schema
/// ([ChunkPathCheck]).
final class SchemaCompatibility {

    private final FileSchema referenceSchema;
    /// Reference schema leaf ordinals this read touches: every projected column plus
    /// every column the filter tests — a predicate column need not be projected, but
    /// pruning still indexes into each file's metadata for it. Exactly the set that
    /// is validated per file and that [FileColumnOrdinals] resolves an ordinal for.
    private final BitSet touchedColumns;

    SchemaCompatibility(FileSchema referenceSchema, ProjectedSchema projected, ResolvedPredicate filter) {
        this.referenceSchema = referenceSchema;
        this.touchedColumns = touchedColumns(projected, filter, referenceSchema.getColumnCount());
    }

    /// Validates the reference schema's own statement of the columns this read touches,
    /// before any of them is planned for or decoded.
    ///
    /// [#validateSchemaCompatibility] cross-checks every file from the second onwards
    /// against the reference schema, but the first file *is* the reference schema and so
    /// is never cross-checked. What it declares therefore has to hold on its own terms:
    /// a fixed-width column whose width the footer omits cannot be decoded by any of the
    /// consumers downstream of here, and a read of a single file would otherwise reach
    /// them with nothing having said so.
    ///
    /// @throws SchemaIncompatibleException if a touched column cannot be decoded under
    ///         the schema the file declares for it
    /// @throws UnsupportedOperationException if a touched column lies below a list of
    ///         variants in the bare repeated form ([BareRepeatedGroups#refuseTouchedVariants])
    void validateReferenceColumns(String fileName) {
        for (int originalIndex = touchedColumns.nextSetBit(0); originalIndex >= 0;
                originalIndex = touchedColumns.nextSetBit(originalIndex + 1)) {
            FixedWidthValidator.validate(fileName, referenceSchema.getColumn(originalIndex));
        }
        BareRepeatedGroups.refuseTouchedVariants(fileName, referenceSchema, touchedColumns);
    }

    /// The reference leaf ordinals a read with this projection and filter touches.
    private static BitSet touchedColumns(ProjectedSchema projected, ResolvedPredicate filter,
                                         int referenceColumnCount) {
        BitSet touched = new BitSet(referenceColumnCount);
        int projectedColumnCount = projected.getProjectedColumnCount();
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            touched.set(projected.toOriginalIndex(projectedIndex));
        }
        if (filter != null) {
            ResolvedPredicate.collectColumnIndices(filter, touched);
        }
        return touched;
    }

    /// Validates the columns this read touches ([#touchedColumns]) against the
    /// reference schema and returns where each of them sits in `fileSchema`.
    ///
    /// Untouched columns map to `-1`; a file is free to differ in columns nobody reads.
    ///
    /// @return this file's leaf ordinal per reference leaf ordinal, `-1` where unresolved
    /// @throws SchemaIncompatibleException if a touched column is missing or its leaf
    ///         differs in a way that changes how its pages decode
    int[] validateSchemaCompatibility(InputFile inputFile, FileSchema fileSchema) {
        int referenceColumnCount = referenceSchema.getColumnCount();
        int[] fileOrdinals = new int[referenceColumnCount];
        Arrays.fill(fileOrdinals, -1);

        BitSet touchedInFile = new BitSet(fileSchema.getColumnCount());
        for (int originalIndex = touchedColumns.nextSetBit(0); originalIndex >= 0;
                originalIndex = touchedColumns.nextSetBit(originalIndex + 1)) {
            fileOrdinals[originalIndex] = validateColumn(inputFile, fileSchema, originalIndex);
            touchedInFile.set(fileOrdinals[originalIndex]);
        }
        BareRepeatedGroups.refuseTouchedVariants(inputFile.name(), fileSchema, touchedInFile);
        return fileOrdinals;
    }

    /// Validates one reference column against its counterpart in `fileSchema`,
    /// resolved by field path.
    ///
    /// @return the column's leaf ordinal in `fileSchema`
    private int validateColumn(InputFile inputFile, FileSchema fileSchema, int originalIndex) {
        ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

        ColumnSchema fileColumn;
        try {
            fileColumn = fileSchema.getColumn(refColumn.fieldPath());
        }
        catch (IllegalArgumentException e) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' not found");
        }

        PhysicalType refType = refColumn.type();
        PhysicalType fileType = fileColumn.type();
        if (refType != fileType) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible type"
                            + ": expected " + refType + " but found " + fileType);
        }

        LogicalType refLogical = refColumn.logicalType();
        LogicalType fileLogical = fileColumn.logicalType();
        if (!Objects.equals(refLogical, fileLogical)) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible logical type"
                            + ": expected " + refLogical + " but found " + fileLogical);
        }

        RepetitionType refRep = refColumn.repetitionType();
        RepetitionType fileRep = fileColumn.repetitionType();
        if (refRep != fileRep) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible repetition type"
                            + ": expected " + refRep + " but found " + fileRep);
        }

        // The FLBA width drives every decode of the column's bytes.
        Integer refLength = refColumn.typeLength();
        Integer fileLength = fileColumn.typeLength();
        if (!Objects.equals(refLength, fileLength)) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath() + "' has incompatible type length"
                            + ": expected " + refLength + " but found " + fileLength);
        }

        // The leaf's own repetition type matching does not imply its ancestors' do,
        // and both levels are read from the per-file leaf when a page is decoded but
        // from the reference leaf when records are assembled from it (DecodedColumn).
        // The same leaf path under an optional rather than a required ancestor group
        // misreads nulls; under a repeated rather than a non-repeated one it misreads
        // record boundaries.
        int refMaxDef = refColumn.maxDefinitionLevel();
        int fileMaxDef = fileColumn.maxDefinitionLevel();
        if (refMaxDef != fileMaxDef) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath()
                            + "' has incompatible maximum definition level"
                            + ": expected " + refMaxDef + " but found " + fileMaxDef);
        }

        int refMaxRep = refColumn.maxRepetitionLevel();
        int fileMaxRep = fileColumn.maxRepetitionLevel();
        if (refMaxRep != fileMaxRep) {
            throw new SchemaIncompatibleException(
                    ExceptionContext.filePrefix(inputFile.name())
                            + "Column '" + refColumn.fieldPath()
                            + "' has incompatible maximum repetition level"
                            + ": expected " + refMaxRep + " but found " + fileMaxRep);
        }

        return fileColumn.columnIndex();
    }

    /// Where each column this read touches sits in one file, and the path the chunk
    /// at that ordinal must carry. Both are properties of the file, while the chunk
    /// list that restates them belongs to each row group, so this is resolved once
    /// per file and each row group is scanned against it by [#verify].
    ///
    /// @param fileOrdinals this file's leaf ordinal per touched column
    /// @param schemaPaths the schema path each of those ordinals was resolved from
    record ChunkPathCheck(int[] fileOrdinals, FieldPath[] schemaPaths) {

        /// Asserts that the column chunk found at a touched leaf's ordinal is the
        /// chunk for that leaf, by comparing the chunk's own `path_in_schema`
        /// against the schema path the ordinal was resolved from.
        ///
        /// The two are independent statements of the same fact — the footer's column
        /// list is positionally aligned with the flattened schema leaves — so a
        /// disagreement means the file's metadata is internally inconsistent and any
        /// bytes read for this column would decode under the wrong column's schema.
        /// Writers that omit the (Thrift-required) path leave nothing to compare, and
        /// are left to the type checks alone.
        ///
        /// @throws SchemaIncompatibleException if a chunk disagrees with the schema
        void verify(RowGroup rowGroup, int rowGroupIndex, InputFile inputFile) {
            List<ColumnChunk> chunks = rowGroup.columns();
            int chunkCount = chunks.size();
            for (int i = 0; i < fileOrdinals.length; i++) {
                int fileOrdinal = fileOrdinals[i];
                FieldPath schemaPath = schemaPaths[i];
                if (fileOrdinal >= chunkCount) {
                    throw new SchemaIncompatibleException(
                            ExceptionContext.filePrefix(inputFile.name())
                                    + "Row group " + rowGroupIndex + " lists " + chunkCount
                                    + " column chunks, but the schema declares '"
                                    + schemaPath + "' at index " + fileOrdinal);
                }
                FieldPath chunkPath = chunks.get(fileOrdinal).metaData().pathInSchema();
                if (chunkPath.isEmpty() || chunkPath.equals(schemaPath)) {
                    continue;
                }
                throw new SchemaIncompatibleException(
                        ExceptionContext.filePrefix(inputFile.name())
                                + "Row group " + rowGroupIndex + " lists column '" + chunkPath
                                + "' where the schema declares '" + schemaPath + "'");
            }
        }
    }

    /// Resolves the file-level side of the chunk-path cross-check for one file.
    ChunkPathCheck chunkPathCheck(FileSchema fileSchema, FileColumnOrdinals columnOrdinals) {
        int touchedCount = touchedColumns.cardinality();
        int[] fileOrdinals = new int[touchedCount];
        FieldPath[] schemaPaths = new FieldPath[touchedCount];
        int touched = 0;
        for (int refOrdinal = touchedColumns.nextSetBit(0); refOrdinal >= 0;
                refOrdinal = touchedColumns.nextSetBit(refOrdinal + 1)) {
            int fileOrdinal = columnOrdinals.fileOrdinal(refOrdinal);
            fileOrdinals[touched] = fileOrdinal;
            schemaPaths[touched] = fileSchema.getColumn(fileOrdinal).fieldPath();
            touched++;
        }
        return new ChunkPathCheck(fileOrdinals, schemaPaths);
    }
}
