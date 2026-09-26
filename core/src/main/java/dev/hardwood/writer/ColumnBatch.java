/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import dev.hardwood.Experimental;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.BinaryArrayColumnSource;
import dev.hardwood.internal.writer.BooleanArrayColumnSource;
import dev.hardwood.internal.writer.ColumnSource;
import dev.hardwood.internal.writer.DoubleArrayColumnSource;
import dev.hardwood.internal.writer.FloatArrayColumnSource;
import dev.hardwood.internal.writer.IntArrayColumnSource;
import dev.hardwood.internal.writer.LogicalTypeValueRange;
import dev.hardwood.internal.writer.LongArrayColumnSource;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// One aligned slice of a file's columns. Every column outside a `LIST` or `MAP` must have
/// the same number of values, which is the batch's row count; a leaf under a `LIST` or `MAP`
/// holds the concatenated entries its offsets account for.
///
/// A batch is not constructed directly: [ColumnWriter#writeBatch] creates it, bound
/// to the schema, hands it to a filler that populates the columns, then submits it — so
/// there is no separate build or submit step to forget. Columns are addressed by index or
/// by name, and the schema binding lets every identifier be validated as it is added — an
/// unknown name, an out-of-range index, a column of the wrong physical type, or setting the same column
/// twice (whether by index or name) all fail eagerly rather than at write time.
///
/// The values are checked as eagerly. A value outside the range its column's annotation
/// declares — 300 on a `UINT_8` column, an unscaled value of more digits than a `DECIMAL`'s
/// precision — is rejected where it is handed over, because writing it produces a file whose
/// values fall outside the range its own annotation declares. The values at rows a column's
/// [Validity] marks null are never encoded, and so are never checked.
///
/// ```java
/// columns.writeBatch(b -> b
///         .ints(0, idColumn)
///         .ints("value", valueColumn));
/// ```
///
/// An `OPTIONAL` column carries its nulls as a [Validity] alongside its values. The values
/// array is full length — one slot per row — and the entry at a null row is ignored. The
/// [Validity] uses the null-centric polarity the reader exposes (`Validity#isNull`), so a
/// value read back as null is written by marking that row null. Because [Validity] is an
/// interface, the caller picks the representation through its factory: [Validity#NO_NULLS]
/// for none, [Validity#ofNulls] to bridge a plain `boolean[]` mask, [Validity#of] for a
/// packed present-bitmap — and, in the future, a sparse form — all consumed identically by
/// the writer.
///
/// ```java
/// columns.writeBatch(b -> b
///         .ints(0, idColumn)
///         .ints("value", valueColumn, valueNulls));       // boolean[] nulls, true = null
/// ```
///
/// The [#ints(int, int[], boolean[])] overload is convenience sugar over
/// [Validity#ofNulls]. The mask-less [#ints(int, int[])] setter is the all-present form for
/// both `REQUIRED` and `OPTIONAL` columns; a null mask is only accepted for an `OPTIONAL`
/// column.
public final class ColumnBatch {

    private final FileSchema schema;

    /// The range each column's annotation declares, resolved once per file by the writer.
    private final LogicalTypeValueRange[] ranges;

    private final ColumnSource[] sources;
    private final Validity[] validities;
    private final Map<String, Validity> structValidities = new HashMap<>();
    private final Map<String, int[]> listOffsets = new HashMap<>();
    private final Map<String, Validity> listValidities = new HashMap<>();
    private int rowCount = -1;
    private boolean consumed;

    ColumnBatch(FileSchema schema, LogicalTypeValueRange[] ranges) {
        this.schema = schema;
        this.ranges = ranges;
        this.sources = new ColumnSource[schema.getColumnCount()];
        this.validities = new Validity[schema.getColumnCount()];
    }

    /// Sets the per-instance nulls of an `OPTIONAL` `struct` group, addressed by its
    /// dot-separated path. Omitting the call leaves every instance of the group present.
    /// The values of leaf columns beneath a null struct instance are ignored.
    ///
    /// @param structPath the group's path (e.g. `"address"`)
    /// @param nulls the group's nulls; `nulls.isNull(i)` marks instance `i` absent
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name an `OPTIONAL` `struct`
    ///         group, or the group is already set in this batch
    @Experimental
    public ColumnBatch struct(String structPath, Validity nulls) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for struct " + structPath
                    + "; omit the struct(...) call for an all-present group");
        }
        resolveStruct(structPath);
        if (structValidities.putIfAbsent(structPath, nulls) != null) {
            throw new IllegalArgumentException("Struct " + structPath + " is already set in this batch");
        }
        return this;
    }

    /// Resolves a dot-separated path to the `OPTIONAL` plain-`struct` group it names,
    /// rejecting a missing path, a leaf, a `REQUIRED` group (no null bit to set), and a
    /// `LIST`/`MAP` group (set through their own verbs).
    private SchemaNode.GroupNode resolveStruct(String structPath) {
        SchemaNode node = schema.getRootNode();
        for (String segment : structPath.split("\\.", -1)) {
            if (!(node instanceof SchemaNode.GroupNode group)) {
                throw new IllegalArgumentException("No struct at path " + structPath);
            }
            node = null;
            for (SchemaNode child : group.children()) {
                if (child.name().equals(segment)) {
                    node = child;
                    break;
                }
            }
            if (node == null) {
                throw new IllegalArgumentException("No struct at path " + structPath);
            }
        }
        if (!(node instanceof SchemaNode.GroupNode group) || !group.isStruct()) {
            throw new IllegalArgumentException("Path " + structPath + " does not name a struct group");
        }
        if (group.repetitionType() != RepetitionType.OPTIONAL) {
            throw new IllegalArgumentException("Struct " + structPath + " is " + group.repetitionType()
                    + "; nulls are only valid for an OPTIONAL struct");
        }
        return group;
    }

    /// Sets the entry offsets of a `LIST`, addressed by the list group's dot-separated path.
    /// `offsets` has length `parentCount + 1`; `offsets[i+1] - offsets[i]` is the number of
    /// entries of list `i`, and a zero delta is an empty list. The list's element leaf is
    /// filled separately through the setter for its type, its values holding the concatenated
    /// entries.
    ///
    /// @param listPath the list group's path (e.g. `"phones"`)
    /// @param offsets the entry offsets
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name a `LIST`, the list is
    ///         already set, or `offsets` is empty
    @Experimental
    public ColumnBatch list(String listPath, int[] offsets) {
        storeRepeated("List", listPath, offsets, null, resolveRepeated(listPath, false));
        return this;
    }

    /// Sets the entry offsets of a `LIST` together with its per-instance nulls (which lists
    /// are themselves absent), distinct from an empty list carrying a zero-delta offset.
    ///
    /// @param listPath the list group's path
    /// @param offsets the entry offsets
    /// @param nulls the list nulls; `nulls.isNull(i)` marks list `i` absent
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name an `OPTIONAL` `LIST`, the
    ///         list is already set, or `offsets` is empty
    @Experimental
    public ColumnBatch list(String listPath, int[] offsets, Validity nulls) {
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for list " + listPath
                    + "; use the two-argument list(...) for an all-present list");
        }
        storeRepeated("List", listPath, offsets, nulls, resolveRepeated(listPath, false));
        return this;
    }

    /// Sets the entry offsets of a `MAP`, addressed by the map group's dot-separated path.
    /// A thin alias over [#list(String, int[])]: the map's `key` and `value` leaves share
    /// this one offsets array, `offsets[i+1] - offsets[i]` being the number of entries of
    /// map `i`, and a zero delta an empty map. The key and value leaves are filled through the
    /// setters for their types at `<mapPath>.key_value.key` and `<mapPath>.key_value.value`.
    ///
    /// @param mapPath the map group's path (e.g. `"props"`)
    /// @param offsets the entry offsets
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name a `MAP`, the map is
    ///         already set, or `offsets` is empty
    @Experimental
    public ColumnBatch map(String mapPath, int[] offsets) {
        storeRepeated("Map", mapPath, offsets, null, resolveRepeated(mapPath, true));
        return this;
    }

    /// Sets the entry offsets of a `MAP` together with its per-instance nulls (which maps
    /// are themselves absent), distinct from an empty map carrying a zero-delta offset.
    ///
    /// @param mapPath the map group's path
    /// @param offsets the entry offsets
    /// @param nulls the map nulls; `nulls.isNull(i)` marks map `i` absent
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name an `OPTIONAL` `MAP`, the
    ///         map is already set, or `offsets` is empty
    @Experimental
    public ColumnBatch map(String mapPath, int[] offsets, Validity nulls) {
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for map " + mapPath
                    + "; use the two-argument map(...) for an all-present map");
        }
        storeRepeated("Map", mapPath, offsets, nulls, resolveRepeated(mapPath, true));
        return this;
    }

    /// Stores the offsets and optional nulls of a `LIST` or `MAP` layer, which share one
    /// offsets-plus-validity representation keyed by the group's path. `kind` names the
    /// shape in error messages ("List" / "Map"); `group` is the already-resolved node.
    private void storeRepeated(String kind, String path, int[] offsets, Validity nulls,
                               SchemaNode.GroupNode group) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        if (offsets == null || offsets.length == 0) {
            throw new IllegalArgumentException("offsets must be non-empty for " + kind.toLowerCase(Locale.ROOT) + " " + path);
        }
        if (nulls != null && group.repetitionType() != RepetitionType.OPTIONAL) {
            throw new IllegalArgumentException(kind + " " + path + " is " + group.repetitionType()
                    + "; nulls are only valid for an OPTIONAL " + kind.toLowerCase(Locale.ROOT));
        }
        if (listOffsets.putIfAbsent(path, offsets) != null) {
            throw new IllegalArgumentException(kind + " " + path + " is already set in this batch");
        }
        if (nulls != null) {
            listValidities.put(path, nulls);
        }
    }

    /// Resolves a dot-separated path to the `LIST` (when `expectMap` is false) or `MAP`
    /// group it names, rejecting a missing path, a leaf, and a group of the other kind.
    private SchemaNode.GroupNode resolveRepeated(String path, boolean expectMap) {
        String kind = expectMap ? "map" : "list";
        SchemaNode node = schema.getRootNode();
        for (String segment : path.split("\\.", -1)) {
            if (!(node instanceof SchemaNode.GroupNode group)) {
                throw new IllegalArgumentException("No " + kind + " at path " + path);
            }
            node = null;
            for (SchemaNode child : group.children()) {
                if (child.name().equals(segment)) {
                    node = child;
                    break;
                }
            }
            if (node == null) {
                throw new IllegalArgumentException("No " + kind + " at path " + path);
            }
        }
        if (!(node instanceof SchemaNode.GroupNode group) || (expectMap ? !group.isMap() : !group.isList())) {
            throw new IllegalArgumentException("Path " + path + " does not name a " + kind + " group");
        }
        return group;
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by index.
    ///
    /// The array is referenced, not copied, so it must not be mutated until the batch
    /// has been written. Every physical type has the same setter shapes (`longs`, `floats`,
    /// `doubles`, `booleans`, …): by index or name, all-present or nullable ([Validity] or
    /// `boolean[]` mask).
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @return this batch, for chaining
    /// @throws IndexOutOfBoundsException if `columnIndex` is not in `[0, leaf column count)`
    /// @throws IllegalArgumentException if the column is not `INT32`, the column is already set,
    ///         a value falls outside the range the column's annotation declares, or the length
    ///         does not match the other columns in this batch
    public ColumnBatch ints(int columnIndex, int[] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateIntValues(idx, values, null);
        store(idx, PhysicalType.INT32, new IntArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch ints(String columnName, int[] values) {
        // getColumn(String) throws on an unknown name, so the identifier is validated here.
        return ints(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by index. The values array is
    /// full length — one slot per row — and the entry at a null row is ignored.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @param nulls the column's nulls; `nulls.isNull(i)` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IndexOutOfBoundsException if `columnIndex` is not in `[0, leaf column count)`
    /// @throws IllegalArgumentException if the column is not `OPTIONAL INT32`, the column is
    ///         already set, or a value at a present row falls outside the range the column's
    ///         annotation declares
    @Experimental
    public ColumnBatch ints(int columnIndex, int[] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateIntValues(idx, values, nulls);
        storeNullable(idx, PhysicalType.INT32, new IntArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch ints(String columnName, int[] values, Validity nulls) {
        return ints(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by index, with nulls given as a
    /// plain mask. Convenience sugar over [#ints(int, int[], Validity)] and [Validity#ofNulls];
    /// unlike the [Validity] form, the mask length is checked against the values.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @param nulls the per-row null mask; `nulls[i] == true` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IndexOutOfBoundsException if `columnIndex` is not in `[0, leaf column count)`
    /// @throws IllegalArgumentException if the column is not `OPTIONAL INT32`, the column is
    ///         already set, or the lengths do not agree
    @Experimental
    public ColumnBatch ints(int columnIndex, int[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        Validity validity = maskToValidity(idx, values.length, nulls);
        validateIntValues(idx, values, validity);
        storeNullable(idx, PhysicalType.INT32, new IntArrayColumnSource(values), values.length, validity);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by name, with nulls given as a
    /// plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch ints(String columnName, int[] values, boolean[] nulls) {
        return ints(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED INT64` column, addressed by index.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch longs(int columnIndex, long[] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateLongValues(idx, values, null);
        store(idx, PhysicalType.INT64, new LongArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED INT64` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch longs(String columnName, long[] values) {
        return longs(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL INT64` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch longs(int columnIndex, long[] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateLongValues(idx, values, nulls);
        storeNullable(idx, PhysicalType.INT64, new LongArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT64` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch longs(String columnName, long[] values, Validity nulls) {
        return longs(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL INT64` column, addressed by index, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch longs(int columnIndex, long[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        Validity validity = maskToValidity(idx, values.length, nulls);
        validateLongValues(idx, values, validity);
        storeNullable(idx, PhysicalType.INT64, new LongArrayColumnSource(values), values.length, validity);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT64` column, addressed by name, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch longs(String columnName, long[] values, boolean[] nulls) {
        return longs(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED FLOAT` column, addressed by index.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch floats(int columnIndex, float[] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        store(idx, PhysicalType.FLOAT, new FloatArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED FLOAT` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch floats(String columnName, float[] values) {
        return floats(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL FLOAT` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch floats(int columnIndex, float[] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.FLOAT, new FloatArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL FLOAT` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch floats(String columnName, float[] values, Validity nulls) {
        return floats(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL FLOAT` column, addressed by index, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch floats(int columnIndex, float[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.FLOAT, new FloatArrayColumnSource(values), values.length,
                maskToValidity(idx, values.length, nulls));
        return this;
    }

    /// Adds the values for an `OPTIONAL FLOAT` column, addressed by name, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch floats(String columnName, float[] values, boolean[] nulls) {
        return floats(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED DOUBLE` column, addressed by index.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch doubles(int columnIndex, double[] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        store(idx, PhysicalType.DOUBLE, new DoubleArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED DOUBLE` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch doubles(String columnName, double[] values) {
        return doubles(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL DOUBLE` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch doubles(int columnIndex, double[] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.DOUBLE, new DoubleArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL DOUBLE` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch doubles(String columnName, double[] values, Validity nulls) {
        return doubles(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL DOUBLE` column, addressed by index, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch doubles(int columnIndex, double[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.DOUBLE, new DoubleArrayColumnSource(values), values.length,
                maskToValidity(idx, values.length, nulls));
        return this;
    }

    /// Adds the values for an `OPTIONAL DOUBLE` column, addressed by name, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch doubles(String columnName, double[] values, boolean[] nulls) {
        return doubles(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED BOOLEAN` column, addressed by index.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch booleans(int columnIndex, boolean[] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        store(idx, PhysicalType.BOOLEAN, new BooleanArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED BOOLEAN` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch booleans(String columnName, boolean[] values) {
        return booleans(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL BOOLEAN` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch booleans(int columnIndex, boolean[] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.BOOLEAN, new BooleanArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL BOOLEAN` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch booleans(String columnName, boolean[] values, Validity nulls) {
        return booleans(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL BOOLEAN` column, addressed by index, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch booleans(int columnIndex, boolean[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        storeNullable(idx, PhysicalType.BOOLEAN, new BooleanArrayColumnSource(values), values.length,
                maskToValidity(idx, values.length, nulls));
        return this;
    }

    /// Adds the values for an `OPTIONAL BOOLEAN` column, addressed by name, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch booleans(String columnName, boolean[] values, boolean[] nulls) {
        return booleans(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED BYTE_ARRAY` column, addressed by index. Each value is a
    /// `byte[]`; the arrays are referenced, not copied.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch bytes(int columnIndex, byte[][] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateBinaryValues(idx, values, null, false);
        store(idx, PhysicalType.BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED BYTE_ARRAY` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch bytes(String columnName, byte[][] values) {
        return bytes(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL BYTE_ARRAY` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch bytes(int columnIndex, byte[][] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateBinaryValues(idx, values, nulls, false);
        storeNullable(idx, PhysicalType.BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL BYTE_ARRAY` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch bytes(String columnName, byte[][] values, Validity nulls) {
        return bytes(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL BYTE_ARRAY` column, addressed by index, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch bytes(int columnIndex, byte[][] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        Validity validity = maskToValidity(idx, values.length, nulls);
        validateBinaryValues(idx, values, validity, false);
        storeNullable(idx, PhysicalType.BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, validity);
        return this;
    }

    /// Adds the values for an `OPTIONAL BYTE_ARRAY` column, addressed by name, with a plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch bytes(String columnName, byte[][] values, boolean[] nulls) {
        return bytes(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for a `REQUIRED FIXED_LEN_BYTE_ARRAY` column, addressed by index. Every
    /// present value must be exactly the column's declared type length.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch fixed(int columnIndex, byte[][] values) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateBinaryValues(idx, values, null, true);
        store(idx, PhysicalType.FIXED_LEN_BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, null);
        return this;
    }

    /// Adds the values for a `REQUIRED FIXED_LEN_BYTE_ARRAY` column, addressed by name.
    ///
    /// @see #ints(int, int[])
    public ColumnBatch fixed(String columnName, byte[][] values) {
        return fixed(schema.getColumn(columnName).columnIndex(), values);
    }

    /// Adds the values for an `OPTIONAL FIXED_LEN_BYTE_ARRAY` column, addressed by index.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch fixed(int columnIndex, byte[][] values, Validity nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        validateBinaryValues(idx, values, nulls, true);
        storeNullable(idx, PhysicalType.FIXED_LEN_BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL FIXED_LEN_BYTE_ARRAY` column, addressed by name.
    ///
    /// @see #ints(int, int[], Validity)
    @Experimental
    public ColumnBatch fixed(String columnName, byte[][] values, Validity nulls) {
        return fixed(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Adds the values for an `OPTIONAL FIXED_LEN_BYTE_ARRAY` column, addressed by index, with a
    /// plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch fixed(int columnIndex, byte[][] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        requireValues(idx, values == null);
        Validity validity = maskToValidity(idx, values.length, nulls);
        validateBinaryValues(idx, values, validity, true);
        storeNullable(idx, PhysicalType.FIXED_LEN_BYTE_ARRAY, new BinaryArrayColumnSource(values), values.length, validity);
        return this;
    }

    /// Adds the values for an `OPTIONAL FIXED_LEN_BYTE_ARRAY` column, addressed by name, with a
    /// plain mask.
    ///
    /// @see #ints(int, int[], boolean[])
    @Experimental
    public ColumnBatch fixed(String columnName, byte[][] values, boolean[] nulls) {
        return fixed(schema.getColumn(columnName).columnIndex(), values, nulls);
    }

    /// Validates a binary column's present values: none may be `null`, for a
    /// `FIXED_LEN_BYTE_ARRAY` (`fixed` true) each must be exactly the column's type length, and
    /// under a `DECIMAL` annotation each must be an unscaled value the declared precision holds.
    /// A null-row value (per `validity`) is ignored. Failing here, at the public boundary, beats
    /// a late error at encode time.
    private void validateBinaryValues(int columnIndex, byte[][] values, Validity validity, boolean fixed) {
        Integer typeLength = schema.getColumn(columnIndex).typeLength();
        LogicalTypeValueRange range = ranges[columnIndex];
        for (int i = 0; i < values.length; i++) {
            if (validity != null && validity.isNull(i)) {
                continue;
            }
            if (values[i] == null) {
                throw new IllegalArgumentException(
                        "Column " + describe(columnIndex) + " has a null value at present row " + i);
            }
            // A null typeLength here means the column is not FIXED_LEN_BYTE_ARRAY; leave the
            // wrong-type report to store(), which names the actual type, rather than unboxing null.
            if (fixed && typeLength != null && values[i].length != typeLength) {
                throw new IllegalArgumentException("Column " + describe(columnIndex)
                        + " has a value of length " + values[i].length + " at row " + i
                        + " but the FIXED_LEN_BYTE_ARRAY type length is " + typeLength);
            }
            if (range.isBounded() && !range.containsUnscaled(values[i])) {
                throw new IllegalArgumentException("Column " + describe(columnIndex)
                        + " has a value at row " + i + " that is not an unscaled value the column's "
                        + range.annotation() + " can hold");
            }
        }
    }

    /// Checks an `INT32` column's present values against the range its annotation declares. A
    /// column of another physical type is left to [#store], which names the mismatch, and an
    /// unannotated or unbounded column is not scanned at all.
    private void validateIntValues(int columnIndex, int[] values, Validity validity) {
        LogicalTypeValueRange range = ranges[columnIndex];
        if (!range.isBounded() || schema.getColumn(columnIndex).type() != PhysicalType.INT32) {
            return;
        }
        for (int i = 0; i < values.length; i++) {
            if (validity != null && validity.isNull(i)) {
                continue;
            }
            if (!range.contains(values[i])) {
                throw outOfRange(columnIndex, values[i], i, range);
            }
        }
    }

    /// Checks an `INT64` column's present values against the range its annotation declares.
    ///
    /// @see #validateIntValues
    private void validateLongValues(int columnIndex, long[] values, Validity validity) {
        LogicalTypeValueRange range = ranges[columnIndex];
        if (!range.isBounded() || schema.getColumn(columnIndex).type() != PhysicalType.INT64) {
            return;
        }
        for (int i = 0; i < values.length; i++) {
            if (validity != null && validity.isNull(i)) {
                continue;
            }
            if (!range.contains(values[i])) {
                throw outOfRange(columnIndex, values[i], i, range);
            }
        }
    }

    private IllegalArgumentException outOfRange(int columnIndex, long value, int row, LogicalTypeValueRange range) {
        return new IllegalArgumentException("Column " + describe(columnIndex) + " has value " + value
                + " at row " + row + ", out of range for a " + range.annotation() + " column");
    }

    /// Bounds-checks a leaf-column index, which is an index error rather than a bad argument:
    /// [StructBuilder]'s field index and the reader's positional accessors both report one as
    /// [IndexOutOfBoundsException], so addressing a column out of range reports the same way
    /// whichever API the caller is holding.
    private int checkedIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= sources.length) {
            throw new IndexOutOfBoundsException(
                    "Column index " + columnIndex + " is out of range [0, " + sources.length + ")");
        }
        return columnIndex;
    }

    private void requireValues(int columnIndex, boolean isNull) {
        if (isNull) {
            throw new IllegalArgumentException("values must not be null for column " + describe(columnIndex));
        }
    }

    private Validity maskToValidity(int columnIndex, int valueCount, boolean[] nulls) {
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for column " + describe(columnIndex)
                    + "; use the mask-less setter for an all-present column");
        }
        if (valueCount != nulls.length) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " has " + valueCount
                    + " values but " + nulls.length + " null flags");
        }
        return Validity.ofNulls(nulls);
    }

    private void storeNullable(int columnIndex, PhysicalType expected, ColumnSource source, int valueCount,
                              Validity nulls) {
        ColumnSchema column = schema.getColumn(columnIndex);
        if (column.repetitionType() != RepetitionType.OPTIONAL) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is "
                    + column.repetitionType() + "; a null mask is only valid for an OPTIONAL column");
        }
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for column " + describe(columnIndex)
                    + "; use the mask-less setter for an all-present column");
        }
        store(columnIndex, expected, source, valueCount, nulls);
    }

    private void store(int columnIndex, PhysicalType expected, ColumnSource source, int valueCount,
                       Validity validity) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        ColumnSchema column = schema.getColumn(columnIndex);
        requireAllNull(columnIndex, valueCount, validity);
        if (column.type() != expected) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is " + column.type()
                    + ", not " + expected);
        }
        if (sources[columnIndex] != null) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is already set in this batch");
        }
        // A repeated leaf's values count its concatenated elements, not the batch's records,
        // so it stays out of the record-count agreement the flat columns must satisfy.
        if (column.maxRepetitionLevel() == 0) {
            if (rowCount < 0) {
                rowCount = valueCount;
            }
            else if (rowCount != valueCount) {
                throw new IllegalArgumentException("Ragged batch: column " + describe(columnIndex) + " has "
                        + valueCount + " values but the batch row count is " + rowCount);
            }
        }
        sources[columnIndex] = source;
        validities[columnIndex] = validity;
    }

    /// Checks that a column annotated `UNKNOWN` carries nothing but nulls. The annotation says
    /// the column holds no values at all, so a value written under it is one the reader refuses
    /// to materialize — the same defect a value outside a declared range produces, at the one
    /// annotation whose declared range is empty.
    private void requireAllNull(int columnIndex, int valueCount, Validity validity) {
        if (!ranges[columnIndex].holdsNoValue()) {
            return;
        }
        if (validity == null) {
            throw new IllegalArgumentException("Column " + describe(columnIndex)
                    + " is annotated UNKNOWN, which holds only nulls; set it through a setter taking a null mask");
        }
        for (int i = 0; i < valueCount; i++) {
            if (!validity.isNull(i)) {
                throw new IllegalArgumentException("Column " + describe(columnIndex)
                        + " is annotated UNKNOWN, which holds only nulls, but row " + i + " is not null");
            }
        }
    }

    private String describe(int columnIndex) {
        return columnIndex + " (" + schema.getColumn(columnIndex).name() + ")";
    }

    /// Marks the batch written, so a filler that stashed a reference and mutates it after
    /// `writeBatch` returns fails loudly instead of silently doing nothing.
    void markConsumed() {
        consumed = true;
    }

    /// The number of rows in this batch (zero if no columns were added).
    int rowCount() {
        return rowCount < 0 ? 0 : rowCount;
    }

    /// The value sources in column order, after checking every column was set.
    ColumnSource[] completedSources() {
        for (int c = 0; c < sources.length; c++) {
            if (sources[c] == null) {
                throw new IllegalArgumentException("Batch is missing column " + describe(c));
            }
        }
        return sources;
    }

    /// The per-column nulls in column order, parallel to [#completedSources]. An entry is
    /// `null` for an all-present column (`REQUIRED`, or `OPTIONAL` set without a mask).
    Validity[] validities() {
        return validities;
    }

    /// The `STRUCT`-layer nulls set in this batch, keyed by group path. A path absent from
    /// the map is an all-present group.
    Map<String, Validity> structValidities() {
        return structValidities;
    }

    /// The `LIST` and `MAP` entry offsets set in this batch, keyed by group path. A map
    /// reuses this storage — its `key` and `value` leaves share the one offsets array.
    Map<String, int[]> listOffsets() {
        return listOffsets;
    }

    /// The `LIST`- and `MAP`-layer nulls set in this batch, keyed by group path. A path
    /// absent from the map is an all-present list or map.
    Map<String, Validity> listValidities() {
        return listValidities;
    }
}
