/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FieldPath;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Independent column processing record assembler for Parquet data.
 *
 * <p>Each column is processed completely independently - the repetition levels alone
 * determine the exact "coordinates" (indices) where each value belongs in the nested
 * structure. Sibling columns are guaranteed to have parallel structure, so processing
 * them separately and merging by index is guaranteed to align correctly.</p>
 *
 * <h2>Algorithm</h2>
 * <pre>
 * For each column:
 *   indices = [0, 0, ...]  // One index per repetition level
 *   For each value in column (for current record):
 *     updateIndices(r)  // Compute position from rep level
 *     insertAtPath(record, path, indices, d, value)
 *
 * updateIndices(r):
 *   // Reset everything deeper than r to 0
 *   for i = r+1 to maxRepLevel: indices[i] = 0
 *   // Increment at level r (except r=0 which starts new record)
 *   if r > 0: indices[r]++
 * </pre>
 *
 * <h2>Example: [[1, 2], [3], [4, 5, 6]]</h2>
 * <pre>
 * Value | r | indices after  | Position
 * ------|---|----------------|----------
 *   1   | 0 | [0, 0]         | [0][0]
 *   2   | 2 | [0, 1]         | [0][1]
 *   3   | 1 | [1, 0]         | [1][0]
 *   4   | 1 | [2, 0]         | [2][0]
 *   5   | 2 | [2, 1]         | [2][1]
 *   6   | 2 | [2, 2]         | [2][2]
 * </pre>
 */
public class RecordAssembler {

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;

    public RecordAssembler(FileSchema schema, ProjectedSchema projectedSchema) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
    }

    /**
     * Assemble a row from prefetched column data at the given record index.
     *
     * @param prefetchedColumns list of prefetched data, one per projected column
     * @param recordIndex       the record index within the prefetched batch
     * @return the assembled row
     */
    public MutableStruct assembleRow(List<NestedColumnData> prefetchedColumns, int recordIndex) {
        int rootSize = schema.getRootNode().children().size();
        MutableStruct record = new MutableStruct(rootSize);

        for (int projectedIdx = 0; projectedIdx < prefetchedColumns.size(); projectedIdx++) {
            NestedColumnData columnData = prefetchedColumns.get(projectedIdx);
            // Get the original column index to look up the field path
            int originalColIndex = projectedSchema.toOriginalIndex(projectedIdx);
            processPrefetchedColumn(columnData, recordIndex, schema.getFieldPaths().get(originalColIndex), record);
        }

        return record;
    }

    /**
     * Process a single column's prefetched data for a specific record.
     */
    private void processPrefetchedColumn(NestedColumnData columnData, int recordIndex,
                                          FieldPath path, MutableStruct record) {
        ColumnSchema column = columnData.column();
        int maxRepLevel = column.maxRepetitionLevel();

        int startOffset = columnData.getStartOffset(recordIndex);
        int valueCount = columnData.getValueCount(recordIndex);

        if (valueCount == 0) {
            return;
        }

        // Fast path for flat columns (no repetition, single primitive step)
        if (maxRepLevel == 0 && path.steps().length == 1 && !path.steps()[0].isContainer()) {
            int d = columnData.getDefLevel(startOffset);
            Object value = columnData.getValue(startOffset);
            if (d == path.maxDefLevel()) {
                record.setChild(path.leafFieldIndex(), value);
            }
            return;
        }

        // General path for nested/repeated columns
        int[] indices = new int[maxRepLevel + 1];

        for (int i = 0; i < valueCount; i++) {
            int offset = startOffset + i;
            int r = columnData.getRepLevel(offset);
            int d = columnData.getDefLevel(offset);
            Object value = columnData.getValue(offset);

            updateIndices(indices, r);
            insertAtPath(record, path, indices, d, value);
        }
    }

    /**
     * Update indices based on repetition level.
     *
     * <p>The repetition level r means "repeating at level r":</p>
     * <ul>
     *   <li>Reset everything deeper than r to 0 (new element starts fresh)</li>
     *   <li>Increment at level r (except r=0 which starts a new record)</li>
     * </ul>
     */
    private void updateIndices(int[] indices, int r) {
        // Reset all levels deeper than r
        for (int i = r + 1; i < indices.length; i++) {
            indices[i] = 0;
        }

        // Increment at level r (except for r=0 which starts a new record)
        if (r > 0) {
            indices[r]++;
        }
    }

    /**
     * Insert a value into the record at the position determined by the path and indices.
     */
    private void insertAtPath(MutableStruct record, FieldPath path, int[] indices,
                              int defLevel, Object value) {
        MutableContainer current = record;
        int indexPtr = 0;

        FieldPath.PathStep[] steps = path.steps();
        for (int level = 0; level < steps.length; level++) {
            FieldPath.PathStep step = steps[level];

            if (step.definitionLevel() > defLevel) {
                return;
            }

            if (step.isRepeated()) {
                int idx = indices[++indexPtr];

                if (step.isContainer()) {
                    current = current.getOrCreateChild(idx, step);
                }
                else {
                    // Primitive element - set value and return
                    if (defLevel == path.maxDefLevel()) {
                        current.setChild(idx, value);
                    }
                    return;
                }
            }
            else if (step.isContainer()) {
                // Skip nested container if previous element step already created it
                if ((step.isList() || step.isMap()) && level > 0) {
                    FieldPath.PathStep prev = steps[level - 1];
                    if (prev.isRepeated() && (prev.isList() || prev.isMap())) {
                        continue;
                    }
                }
                current = current.getOrCreateChild(step.fieldIndex(), step);
            }
        }

        if (defLevel == path.maxDefLevel()) {
            current.setChild(path.leafFieldIndex(), value);
        }
    }
}
