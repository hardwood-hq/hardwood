/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;

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
            ColumnBatch batch = batches.get(startColumn);
            if (batch instanceof SimpleColumnBatch simple) {
                return simple.get(batchPosition);
            }
            // RawColumnBatch - extract single value for this record
            RawColumnBatch raw = (RawColumnBatch) batch;
            List<ColumnBatch.ValueWithLevels> values = extractValuesFromBatch(raw, batchPosition);
            if (values.isEmpty()) {
                return null;
            }
            ColumnBatch.ValueWithLevels firstVal = values.get(0);
            if (firstVal.defLevel() == raw.getColumn().getMaxDefinitionLevel()) {
                return firstVal.value();
            }
            return null;
        }

        SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
        if (group.isList()) {
            return assembleList(group, batches, batchPosition, startColumn);
        }
        if (group.isMap()) {
            return assembleMap(group, batches, batchPosition, startColumn);
        }
        return assembleStruct(group, batches, batchPosition, startColumn);
    }

    private Object[] assembleStruct(SchemaNode.GroupNode structNode, List<ColumnBatch> batches,
                                    int batchPosition, int startColumn) {
        // First check if the struct itself is null by examining the definition level
        // of the first child column. If defLevel < struct's maxDefLevel, struct is null.
        ColumnBatch firstBatch = batches.get(startColumn);
        if (firstBatch instanceof RawColumnBatch raw) {
            List<ColumnBatch.ValueWithLevels> values = extractValuesFromBatch(raw, batchPosition);
            if (!values.isEmpty()) {
                int defLevel = values.get(0).defLevel();
                int structMaxDef = structNode.maxDefinitionLevel();
                // If def level is less than the struct's max definition level,
                // the struct itself is null (not just empty with null children)
                if (defLevel < structMaxDef) {
                    return null;
                }
            }
        }
        // For SimpleColumnBatch, we don't have definition levels, so we can't distinguish
        // a null struct from a struct with all null children. Since we now use raw mode
        // for optional structs, this path should rarely be hit.

        Object[] result = new Object[structNode.children().size()];
        int columnIndex = startColumn;

        for (int i = 0; i < result.length; i++) {
            SchemaNode child = structNode.children().get(i);
            Object value = assembleValue(child, batches, batchPosition, columnIndex);
            result[i] = value;
            columnIndex += countPrimitiveColumns(child);
        }
        // Return the struct even if all children are null - the struct itself is present
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Object> assembleList(SchemaNode.GroupNode listNode, List<ColumnBatch> batches,
                                      int batchPosition, int startColumn) {
        SchemaNode element = listNode.getListElement();

        // List of structs needs multi-column assembly
        if (element instanceof SchemaNode.GroupNode elementGroup && !elementGroup.isList() && !elementGroup.isMap()) {
            return assembleListOfStruct(listNode, elementGroup, batches, batchPosition, startColumn);
        }

        // List of maps needs special handling
        if (element instanceof SchemaNode.GroupNode elementGroup && elementGroup.isMap()) {
            return assembleListOfMaps(listNode, elementGroup, batches, batchPosition, startColumn);
        }

        // List of primitives - check batch type
        ColumnBatch batch = batches.get(startColumn);
        if (batch instanceof SimpleColumnBatch simple) {
            Object value = simple.get(batchPosition);
            if (value == null)
                return null;
            if (value instanceof List)
                return (List<Object>) value;
            return List.of(value);
        }

        // RawColumnBatch - assemble list from raw values
        RawColumnBatch raw = (RawColumnBatch) batch;
        List<ColumnBatch.ValueWithLevels> values = extractValuesFromBatch(raw, batchPosition);
        if (values.isEmpty()) {
            return new ArrayList<>();
        }

        // Check null/empty
        ColumnBatch.ValueWithLevels firstVal = values.get(0);
        if (firstVal.defLevel() < listNode.maxDefinitionLevel()) {
            return null; // list is null
        }
        int maxDefLevel = raw.getColumn().getMaxDefinitionLevel();
        if (firstVal.defLevel() < maxDefLevel && values.size() == 1) {
            return new ArrayList<>(); // empty list
        }

        // Build list of primitives
        List<Object> result = new ArrayList<>();
        for (ColumnBatch.ValueWithLevels val : values) {
            if (val.defLevel() == maxDefLevel) {
                result.add(val.value());
            }
        }
        return result;
    }

    /**
     * Assemble a MAP from column batches.
     * MAPs are stored as repeated key_value groups with key and value children.
     * Returns a list of key-value pairs represented as Object[] arrays.
     */
    private List<Object> assembleMap(SchemaNode.GroupNode mapNode, List<ColumnBatch> batches,
                                     int batchPosition, int startColumn) {
        // MAP structure: MAP -> repeated key_value -> (key, value)
        // Get the key_value group
        if (mapNode.children().isEmpty()) {
            return null;
        }

        SchemaNode keyValueNode = mapNode.children().get(0);
        if (!(keyValueNode instanceof SchemaNode.GroupNode keyValueGroup)) {
            return null;
        }

        // Use assembleListOfStruct since MAP key_value is essentially a list of key-value structs
        return assembleListOfStruct(mapNode, keyValueGroup, batches, batchPosition, startColumn);
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

        // Empty container: def level equals container's def level (container exists but no elements)
        if (firstValue.defLevel() == listNode.maxDefinitionLevel() && firstColValues.size() == 1) {
            return new ArrayList<>();
        }

        int listRepLevel = firstBatch.getColumn().getMaxRepetitionLevel();

        // Determine the minimum def level needed for an element to be present
        // For MAP key_value: elements exist when key is defined (def >= repeated group's level + 1)
        // For LIST element: elements exist when element struct is defined
        int repeatedGroupDefLevel = listNode.maxDefinitionLevel() + 1;

        // Build list of structs
        List<Object> result = new ArrayList<>();
        for (int elemIdx = 0; elemIdx < firstColValues.size(); elemIdx++) {
            if (firstColValues.get(elemIdx).defLevel() < repeatedGroupDefLevel)
                continue;

            Object[] struct = buildStruct(elementSchema, columnRawValues, batches,
                    startColumn, elemIdx, listRepLevel);
            result.add(struct);
        }
        return result;
    }

    /**
     * Assemble a LIST of MAPs from column batches.
     * Groups map entries by list element using repetition levels.
     */
    private List<Object> assembleListOfMaps(SchemaNode.GroupNode listNode, SchemaNode.GroupNode mapSchema,
                                            List<ColumnBatch> batches, int batchPosition, int startColumn) {
        int numColumns = countPrimitiveColumns(mapSchema);
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

        // Empty list
        if (firstValue.defLevel() == listNode.maxDefinitionLevel() && firstColValues.size() == 1) {
            return new ArrayList<>();
        }

        RawColumnBatch firstBatch = (RawColumnBatch) batches.get(startColumn);
        // List rep level is where list elements are separated
        int listRepLevel = listNode.maxDefinitionLevel(); // rep level for list element boundary

        // Get the key_value group from the MAP
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapSchema.children().get(0);

        // Group values by list element (using list rep level boundary)
        List<Object> result = new ArrayList<>();
        int elemStart = 0;

        for (int i = 0; i <= firstColValues.size(); i++) {
            // Check if this is a new list element boundary or end of values
            boolean isBoundary = (i == firstColValues.size()) ||
                    (i > 0 && firstColValues.get(i).repLevel() <= listRepLevel);

            if (isBoundary && i > elemStart) {
                // Build map from entries [elemStart, i)
                List<Object> mapEntries = buildMapEntries(keyValueGroup, columnRawValues, batches,
                        startColumn, elemStart, i, mapSchema.maxDefinitionLevel());
                result.add(mapEntries);
                elemStart = i;
            }
        }

        return result;
    }

    /**
     * Build a single map's entries from a range of raw values.
     */
    private List<Object> buildMapEntries(SchemaNode.GroupNode keyValueGroup,
                                         List<List<ColumnBatch.ValueWithLevels>> columnRawValues,
                                         List<ColumnBatch> batches, int startColumn,
                                         int rangeStart, int rangeEnd, int mapDefLevel) {
        List<Object> entries = new ArrayList<>();
        int repeatedGroupDefLevel = mapDefLevel + 1;

        for (int entryIdx = rangeStart; entryIdx < rangeEnd; entryIdx++) {
            if (columnRawValues.get(0).get(entryIdx).defLevel() < repeatedGroupDefLevel) {
                continue;
            }

            // Build key-value struct for this entry
            Object[] keyValue = new Object[keyValueGroup.children().size()];
            int childColOffset = 0;

            for (int i = 0; i < keyValue.length; i++) {
                SchemaNode child = keyValueGroup.children().get(i);
                int colCount = countPrimitiveColumns(child);

                if (child instanceof SchemaNode.PrimitiveNode) {
                    List<ColumnBatch.ValueWithLevels> colValues = columnRawValues.get(childColOffset);
                    int maxDefLevel = batches.get(startColumn + childColOffset).getColumn().getMaxDefinitionLevel();
                    if (entryIdx < colValues.size()) {
                        ColumnBatch.ValueWithLevels val = colValues.get(entryIdx);
                        if (val.defLevel() == maxDefLevel) {
                            keyValue[i] = val.value();
                        }
                    }
                }
                childColOffset += colCount;
            }
            entries.add(keyValue);
        }

        return entries;
    }

    private Object[] buildStruct(SchemaNode.GroupNode elementSchema,
                                 List<List<ColumnBatch.ValueWithLevels>> columnRawValues,
                                 List<ColumnBatch> batches, int startColumn,
                                 int elemIdx, int listRepLevel) {
        Object[] structValues = new Object[elementSchema.children().size()];
        int childColOffset = 0;

        for (int i = 0; i < structValues.length; i++) {
            SchemaNode child = elementSchema.children().get(i);
            int colCount = countPrimitiveColumns(child);

            if (child instanceof SchemaNode.PrimitiveNode) {
                structValues[i] = getPrimitiveValue(columnRawValues.get(childColOffset),
                        batches.get(startColumn + childColOffset).getColumn().getMaxDefinitionLevel(), elemIdx);
            }
            else if (child instanceof SchemaNode.GroupNode groupChild) {
                if (groupChild.isList()) {
                    structValues[i] = assembleNestedList(groupChild, batches, startColumn + childColOffset,
                            columnRawValues, childColOffset, elemIdx, listRepLevel);
                }
                else if (groupChild.isMap()) {
                    structValues[i] = assembleNestedMap(groupChild, batches, startColumn + childColOffset,
                            columnRawValues, childColOffset, elemIdx, listRepLevel);
                }
                else {
                    // Nested struct
                    structValues[i] = assembleNestedStruct(groupChild, batches, startColumn + childColOffset,
                            columnRawValues, childColOffset, elemIdx, listRepLevel);
                }
            }
            childColOffset += colCount;
        }
        return structValues;
    }

    private Object getPrimitiveValue(List<ColumnBatch.ValueWithLevels> colValues, int maxDefLevel, int elemIdx) {
        if (elemIdx < colValues.size()) {
            ColumnBatch.ValueWithLevels val = colValues.get(elemIdx);
            if (val.defLevel() == maxDefLevel) {
                return val.value();
            }
        }
        return null;
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
        // Empty list: def level equals container's def level
        if (firstValue.defLevel() == listNode.maxDefinitionLevel() && nestedColValues.get(0).size() == 1) {
            return new ArrayList<>();
        }

        SchemaNode element = listNode.getListElement();
        List<Object> result = new ArrayList<>();
        int repeatedGroupDefLevel = listNode.maxDefinitionLevel() + 1;

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
            if (nestedColValues.get(0).get(elemIdx).defLevel() < repeatedGroupDefLevel)
                continue;

            Object[] structValues = new Object[elementGroup.children().size()];
            int childIdx = 0;
            for (int i = 0; i < structValues.length; i++) {
                SchemaNode child = elementGroup.children().get(i);
                if (child instanceof SchemaNode.PrimitiveNode) {
                    structValues[i] = getPrimitiveValue(nestedColValues.get(childIdx),
                            batches.get(startColumn + childIdx).getColumn().getMaxDefinitionLevel(), elemIdx);
                    childIdx++;
                }
            }
            result.add(structValues);
        }
        return result;
    }

    /**
     * Assemble a nested MAP within a struct element.
     */
    private List<Object> assembleNestedMap(SchemaNode.GroupNode mapNode, List<ColumnBatch> batches,
                                           int startColumn, List<List<ColumnBatch.ValueWithLevels>> allRawValues,
                                           int colOffset, int parentElemIdx, int parentRepLevel) {
        // MAP structure: MAP -> key_value (REPEATED) -> (key, value)
        if (mapNode.children().isEmpty()) {
            return null;
        }

        SchemaNode keyValueNode = mapNode.children().get(0);
        if (!(keyValueNode instanceof SchemaNode.GroupNode keyValueGroup)) {
            return null;
        }

        int mapColCount = countPrimitiveColumns(mapNode);
        if (mapColCount == 0 || colOffset >= allRawValues.size()) {
            return new ArrayList<>();
        }

        // Extract values belonging to this parent element
        List<List<ColumnBatch.ValueWithLevels>> mapColValues = new ArrayList<>();
        for (int i = 0; i < mapColCount; i++) {
            mapColValues.add(extractValues(allRawValues.get(colOffset + i), parentElemIdx, parentRepLevel));
        }

        if (mapColValues.get(0).isEmpty()) {
            return new ArrayList<>();
        }

        ColumnBatch.ValueWithLevels firstValue = mapColValues.get(0).get(0);
        int mapMaxDefLevel = batches.get(startColumn).getColumn().getMaxDefinitionLevel();

        // Check null map
        if (firstValue.defLevel() < mapNode.maxDefinitionLevel()) {
            return null;
        }

        // Check empty map: def level equals container's def level
        if (firstValue.defLevel() == mapNode.maxDefinitionLevel() && mapColValues.get(0).size() == 1) {
            return new ArrayList<>();
        }

        // Build list of key-value pairs
        int mapRepLevel = batches.get(startColumn).getColumn().getMaxRepetitionLevel();
        int repeatedGroupDefLevel = mapNode.maxDefinitionLevel() + 1;
        List<Object> result = new ArrayList<>();

        for (int entryIdx = 0; entryIdx < mapColValues.get(0).size(); entryIdx++) {
            if (mapColValues.get(0).get(entryIdx).defLevel() < repeatedGroupDefLevel) {
                continue;
            }

            Object[] keyValueStruct = buildStruct(keyValueGroup, mapColValues, batches,
                    startColumn, entryIdx, mapRepLevel);
            result.add(keyValueStruct);
        }
        return result;
    }

    /**
     * Assemble a nested struct within a list/map element.
     */
    private Object[] assembleNestedStruct(SchemaNode.GroupNode structNode, List<ColumnBatch> batches,
                                          int startColumn, List<List<ColumnBatch.ValueWithLevels>> allRawValues,
                                          int colOffset, int parentElemIdx, int parentRepLevel) {
        int structColCount = countPrimitiveColumns(structNode);
        if (structColCount == 0 || colOffset >= allRawValues.size()) {
            return null;
        }

        // Extract values belonging to this parent element
        List<List<ColumnBatch.ValueWithLevels>> structColValues = new ArrayList<>();
        for (int i = 0; i < structColCount; i++) {
            structColValues.add(extractValues(allRawValues.get(colOffset + i), parentElemIdx, parentRepLevel));
        }

        if (structColValues.get(0).isEmpty()) {
            return null;
        }

        ColumnBatch.ValueWithLevels firstValue = structColValues.get(0).get(0);
        int structMaxDefLevel = batches.get(startColumn).getColumn().getMaxDefinitionLevel();

        // Check null struct
        if (firstValue.defLevel() < structNode.maxDefinitionLevel()) {
            return null;
        }

        // Build struct values
        Object[] structValues = new Object[structNode.children().size()];
        int childColOffset = 0;

        for (int i = 0; i < structValues.length; i++) {
            SchemaNode child = structNode.children().get(i);
            int colCount = countPrimitiveColumns(child);

            if (child instanceof SchemaNode.PrimitiveNode) {
                structValues[i] = getPrimitiveValue(structColValues.get(childColOffset),
                        batches.get(startColumn + childColOffset).getColumn().getMaxDefinitionLevel(), 0);
            }
            childColOffset += colCount;
        }

        // Check if all values are null
        boolean allNull = true;
        for (Object val : structValues) {
            if (val != null) {
                allNull = false;
                break;
            }
        }
        return allNull ? null : structValues;
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
