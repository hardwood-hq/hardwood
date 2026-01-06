/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Assembles rows from flat column data using definition and repetition levels.
 * Implements the record assembly part of the Dremel algorithm.
 *
 * <h2>Background</h2>
 * <p>Parquet stores nested data in a columnar format by flattening the structure into
 * separate columns for each primitive field. To reconstruct the original nested structure,
 * each value is annotated with two levels:</p>
 * <ul>
 *   <li><b>Definition level (def)</b>: How many optional/repeated fields in the path are defined.
 *       Used to distinguish null values at different nesting levels.</li>
 *   <li><b>Repetition level (rep)</b>: Which repeated field in the path has repeated.
 *       Used to determine when a new record or list element starts.</li>
 * </ul>
 *
 * <h2>Example: Multiple Lists of Structs</h2>
 * <p>Consider this schema with two lists of items:</p>
 * <pre>
 * message schema {
 *   required int32 id;                            // maxDef=0, maxRep=0
 *   optional group items (LIST) {                 // maxDef=1
 *     repeated group list {                       // maxDef=2, maxRep=1
 *       optional group element {                  // maxDef=3
 *         optional binary name (STRING);          // maxDef=4, maxRep=1
 *         optional int32 quantity;                // maxDef=4, maxRep=1
 *       }
 *     }
 *   }
 *   optional group reservedItems (LIST) {         // maxDef=1
 *     repeated group list {                       // maxDef=2, maxRep=1
 *       optional group element {                  // maxDef=3
 *         optional binary name (STRING);          // maxDef=4, maxRep=1
 *         optional int32 quantity;                // maxDef=4, maxRep=1
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>With this data:</p>
 * <pre>
 * Row 0: {id: 1, items: [{apple, 5}, {banana, 10}], reservedItems: [{cherry, 2}]}
 * Row 1: {id: 2, items: [{orange, 3}],              reservedItems: []}
 * Row 2: {id: 3, items: [],                         reservedItems: null}
 * </pre>
 *
 * <p>The columns store these values with levels:</p>
 * <pre>
 * items.name column:              reservedItems.name column:
 * | Value    | Def | Rep |        | Value    | Def | Rep |
 * |----------|-----|-----|        |----------|-----|-----|
 * | "apple"  |  4  |  0  |        | "cherry" |  4  |  0  |
 * | "banana" |  4  |  1  |        | null     |  1  |  0  |
 * | "orange" |  4  |  0  |        | null     |  0  |  0  |
 * | null     |  1  |  0  |
 * </pre>
 *
 * <h2>Level Interpretation</h2>
 * <ul>
 *   <li><b>rep=0</b>: Start of a new record (row)</li>
 *   <li><b>rep=1</b>: New element in the same list</li>
 *   <li><b>def=4</b>: Value is present (all levels defined)</li>
 *   <li><b>def=1</b>: List exists but is empty (only the LIST group defined)</li>
 *   <li><b>def=0</b>: List is null (LIST group not defined)</li>
 * </ul>
 *
 * <p>Key insight: Each column's rep/def levels are independent. The {@code rep=0} in
 * {@code reservedItems.name} doesn't indicate a new row—it indicates the start of the
 * first element in that column for each row. Row boundaries are determined by correlating
 * {@code rep=0} values across all columns that share the same parent repetition level.</p>
 *
 * <p>This class correlates values across multiple columns using these levels to
 * reconstruct the original nested structure.</p>
 */
public class RecordAssembler {

    private final FileSchema schema;

    public RecordAssembler(FileSchema schema) {
        this.schema = schema;
    }

    /**
     * Assemble values from all columns into a row based on the schema structure.
     */
    public Object[] assembleRow(List<ColumnBatch> batches, int batchPosition) {
        SchemaNode.GroupNode root = schema.getRootNode();
        Object[] result = new Object[root.children().size()];

        int columnIndex = 0;
        for (int i = 0; i < result.length; i++) {
            SchemaNode field = root.children().get(i);
            result[i] = assembleValue(field, batches, batchPosition, columnIndex);
            columnIndex += countPrimitiveColumns(field);
        }
        return result;
    }

    private Object assembleValue(SchemaNode node, List<ColumnBatch> batches, int batchPosition, int startColumn) {
        if (node instanceof SchemaNode.PrimitiveNode) {
            return ((SimpleColumnBatch) batches.get(startColumn)).get(batchPosition);
        }

        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
        return group.isList()
                ? assembleList(group, batches, batchPosition, startColumn)
                : assembleStruct(group, batches, batchPosition, startColumn);
    }

    private Map<String, Object> assembleStruct(SchemaNode.GroupNode structNode, List<ColumnBatch> batches,
                                               int batchPosition, int startColumn) {
        Map<String, Object> result = new HashMap<>();
        int columnIndex = startColumn;
        boolean allNull = true;

        for (SchemaNode child : structNode.children()) {
            Object value = assembleValue(child, batches, batchPosition, columnIndex);
            result.put(child.name(), value);
            columnIndex += countPrimitiveColumns(child);
            if (value != null)
                allNull = false;
        }
        return allNull ? null : result;
    }

    @SuppressWarnings("unchecked")
    private List<Object> assembleList(SchemaNode.GroupNode listNode, List<ColumnBatch> batches,
                                      int batchPosition, int startColumn) {
        SchemaNode element = listNode.getListElement();

        // List of structs needs multi-column assembly
        if (element instanceof SchemaNode.GroupNode elementGroup && !elementGroup.isList()) {
            return assembleListOfStruct(listNode, elementGroup, batches, batchPosition, startColumn);
        }

        // List of primitives - use pre-assembled value
        Object value = ((SimpleColumnBatch) batches.get(startColumn)).get(batchPosition);
        if (value == null)
            return null;
        if (value instanceof List)
            return (List<Object>) value;
        return List.of(value);
    }

    private List<Object> assembleListOfStruct(SchemaNode.GroupNode listNode, SchemaNode.GroupNode elementSchema,
                                              List<ColumnBatch> batches, int batchPosition, int startColumn) {
        int numColumns = countPrimitiveColumns(elementSchema);
        if (numColumns == 0)
            return new ArrayList<>();

        // Get raw values from all columns for this record
        List<List<ColumnBatch.ValueWithLevels>> columnRawValues = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            ColumnBatch batch = batches.get(startColumn + i);
            if (!(batch instanceof RawColumnBatch rawBatch))
                return null;
            columnRawValues.add(extractValuesFromBatch(rawBatch, batchPosition));
        }

        List<ColumnBatch.ValueWithLevels> firstColValues = columnRawValues.get(0);
        if (firstColValues.isEmpty())
            return new ArrayList<>();

        // Check null/empty list
        ColumnBatch.ValueWithLevels firstValue = firstColValues.get(0);
        if (firstValue.defLevel() < listNode.maxDefinitionLevel())
            return null;

        RawColumnBatch firstBatch = (RawColumnBatch) batches.get(startColumn);
        int elementMaxDefLevel = firstBatch.getColumn().getMaxDefinitionLevel();
        if (firstValue.defLevel() < elementMaxDefLevel - 1 && firstColValues.size() == 1) {
            return new ArrayList<>();
        }

        int listRepLevel = firstBatch.getColumn().getMaxRepetitionLevel();

        // Build list of structs
        List<Object> result = new ArrayList<>();
        for (int elemIdx = 0; elemIdx < firstColValues.size(); elemIdx++) {
            if (firstColValues.get(elemIdx).defLevel() < elementMaxDefLevel - 1)
                continue;

            Map<String, Object> struct = buildStruct(elementSchema, columnRawValues, batches,
                    startColumn, elemIdx, listRepLevel);
            result.add(struct);
        }
        return result;
    }

    private Map<String, Object> buildStruct(SchemaNode.GroupNode elementSchema,
                                            List<List<ColumnBatch.ValueWithLevels>> columnRawValues,
                                            List<ColumnBatch> batches, int startColumn,
                                            int elemIdx, int listRepLevel) {
        Map<String, Object> structValues = new HashMap<>();
        int childColOffset = 0;

        for (SchemaNode child : elementSchema.children()) {
            int colCount = countPrimitiveColumns(child);

            if (child instanceof SchemaNode.PrimitiveNode primChild) {
                addPrimitiveValue(structValues, primChild.name(), columnRawValues.get(childColOffset),
                        batches.get(startColumn + childColOffset).getColumn().getMaxDefinitionLevel(), elemIdx);
            }
            else if (child instanceof SchemaNode.GroupNode groupChild && groupChild.isList()) {
                List<Object> nestedList = assembleNestedList(groupChild, batches, startColumn + childColOffset,
                        columnRawValues, childColOffset, elemIdx, listRepLevel);
                structValues.put(groupChild.name(), nestedList);
            }
            childColOffset += colCount;
        }
        return structValues;
    }

    private void addPrimitiveValue(Map<String, Object> struct, String name,
                                   List<ColumnBatch.ValueWithLevels> colValues, int maxDefLevel, int elemIdx) {
        if (elemIdx < colValues.size()) {
            ColumnBatch.ValueWithLevels val = colValues.get(elemIdx);
            if (val.defLevel() == maxDefLevel) {
                struct.put(name, val.value());
            }
        }
    }

    private List<Object> assembleNestedList(SchemaNode.GroupNode listNode, List<ColumnBatch> batches,
                                            int startColumn, List<List<ColumnBatch.ValueWithLevels>> allRawValues,
                                            int colOffset, int parentElemIdx, int parentRepLevel) {
        int nestedColCount = countPrimitiveColumns(listNode);
        if (nestedColCount == 0 || colOffset >= allRawValues.size())
            return new ArrayList<>();

        // Extract values belonging to this parent element
        List<List<ColumnBatch.ValueWithLevels>> nestedColValues = new ArrayList<>();
        for (int i = 0; i < nestedColCount; i++) {
            nestedColValues.add(extractValues(allRawValues.get(colOffset + i), parentElemIdx, parentRepLevel));
        }

        if (nestedColValues.get(0).isEmpty())
            return new ArrayList<>();

        ColumnBatch.ValueWithLevels firstValue = nestedColValues.get(0).get(0);
        int nestedMaxDefLevel = batches.get(startColumn).getColumn().getMaxDefinitionLevel();

        if (firstValue.defLevel() < listNode.maxDefinitionLevel())
            return null;
        if (firstValue.defLevel() < nestedMaxDefLevel - 1 && nestedColValues.get(0).size() == 1) {
            return new ArrayList<>();
        }

        SchemaNode element = listNode.getListElement();
        List<Object> result = new ArrayList<>();

        // List of primitives
        if (!(element instanceof SchemaNode.GroupNode elementGroup) || elementGroup.isList()) {
            for (ColumnBatch.ValueWithLevels val : nestedColValues.get(0)) {
                if (val.defLevel() == nestedMaxDefLevel)
                    result.add(val.value());
            }
            return result;
        }

        // List of structs
        for (int elemIdx = 0; elemIdx < nestedColValues.get(0).size(); elemIdx++) {
            if (nestedColValues.get(0).get(elemIdx).defLevel() < nestedMaxDefLevel - 1)
                continue;

            Map<String, Object> structValues = new HashMap<>();
            int childIdx = 0;
            for (SchemaNode child : elementGroup.children()) {
                if (child instanceof SchemaNode.PrimitiveNode primChild) {
                    addPrimitiveValue(structValues, primChild.name(), nestedColValues.get(childIdx),
                            batches.get(startColumn + childIdx).getColumn().getMaxDefinitionLevel(), elemIdx);
                    childIdx++;
                }
            }
            result.add(structValues);
        }
        return result;
    }

    /**
     * Extract values for a specific record from a RawColumnBatch.
     * Records are delimited by rep == 0.
     */
    private List<ColumnBatch.ValueWithLevels> extractValuesFromBatch(RawColumnBatch batch, int recordIndex) {
        List<ColumnBatch.ValueWithLevels> result = new ArrayList<>();
        int currentRecord = 0;
        boolean first = true;

        for (int i = 0; i < batch.getRawValueCount(); i++) {
            int repLevel = batch.getRepetitionLevel(i);

            if (!first && repLevel == 0) {
                currentRecord++;
                if (currentRecord > recordIndex)
                    break;
            }
            first = false;

            if (currentRecord == recordIndex) {
                result.add(new ColumnBatch.ValueWithLevels(
                        batch.getRawValue(i),
                        batch.getDefinitionLevel(i),
                        repLevel));
            }
        }
        return result;
    }

    /**
     * Extract values for a specific element from an already-extracted values list.
     * Elements are delimited by rep <= boundaryRepLevel.
     */
    private List<ColumnBatch.ValueWithLevels> extractValues(List<ColumnBatch.ValueWithLevels> allValues,
                                                            int elementIndex, int boundaryRepLevel) {
        List<ColumnBatch.ValueWithLevels> result = new ArrayList<>();
        int currentElement = 0;
        boolean first = true;

        for (ColumnBatch.ValueWithLevels val : allValues) {
            if (!first && val.repLevel() <= boundaryRepLevel) {
                currentElement++;
                if (currentElement > elementIndex)
                    break;
            }
            first = false;

            if (currentElement == elementIndex)
                result.add(val);
        }
        return result;
    }

    private int countPrimitiveColumns(SchemaNode node) {
        if (node instanceof SchemaNode.PrimitiveNode)
            return 1;

        int count = 0;
        for (SchemaNode child : ((SchemaNode.GroupNode) node).children()) {
            count += countPrimitiveColumns(child);
        }
        return count;
    }
}
