/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

/**
 * Pre-computed path from root to a primitive column.
 *
 * @param steps          Path steps from root to the parent of this column
 * @param leafFieldIndex Index of this column's field in its parent struct
 * @param maxDefLevel    Maximum definition level for this column
 */
public record FieldPath(
        PathStep[] steps,
        int leafFieldIndex,
        int maxDefLevel) {

    /**
     * A single step in the path from root to a column.
     *
     * @param name            Field name (for debugging)
     * @param fieldIndex      Index in parent struct (ignored if isRepeated)
     * @param definitionLevel Definition level for NULL checks
     * @param isRepeated      True if this step navigates into a repeated element
     *                        (uses repetition index)
     * @param isList          True if this step creates a LIST container
     * @param isMap           True if this step creates a MAP container
     * @param numChildren     Number of children (for struct creation)
     */
    public record PathStep(
            String name,
            int fieldIndex,
            int definitionLevel,
            boolean isRepeated,
            boolean isList,
            boolean isMap,
            int numChildren) {

        public static PathStep forGroup(String name, int fieldIndex, int defLevel, boolean isRepeated, boolean isList,
                boolean isMap, int numChildren) {
            return new PathStep(name, fieldIndex, defLevel, isRepeated, isList, isMap, numChildren);
        }

        public static PathStep forPrimitive(String name, int fieldIndex, int defLevel, boolean isRepeated) {
            return new PathStep(name, fieldIndex, defLevel, isRepeated, false, false, 0);
        }

        public boolean isStruct() {
            return !isMap() && numChildren > 0;
        }

        public boolean isContainer() {
            return isMap() || isList() || isStruct();
        }

        @Override
        public String toString() {
            String type = isList ? "LIST" : isMap ? "MAP" : isRepeated ? "REPEATED" : isStruct() ? "STRUCT" : "PRIMITIVE";
            return name + "(" + type + (isRepeated ? "" : ", idx=" + fieldIndex) + ", def=" + definitionLevel
                    + (isStruct() ? ", children=" + numChildren : "") + ")";
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FieldPath[");
        for (int i = 0; i < steps.length; i++) {
            if (i > 0) {
                sb.append(" -> ");
            }
            sb.append(steps[i]);
        }
        sb.append(", leaf=").append(leafFieldIndex);
        sb.append(", maxDef=").append(maxDefLevel);
        sb.append("]");
        return sb.toString();
    }
}
