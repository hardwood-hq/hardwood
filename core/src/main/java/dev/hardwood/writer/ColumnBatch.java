/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.HashMap;
import java.util.Map;

import dev.hardwood.Experimental;
import dev.hardwood.Validity;
import dev.hardwood.internal.writer.IntArrayColumnSource;
import dev.hardwood.internal.writer.IntColumnSource;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// One aligned slice of a file's columns. Every column in a batch must have the same
/// number of values, which is the batch's row count.
///
/// A batch is not constructed directly: [ParquetFileWriter#writeBatch] creates it, bound
/// to the schema, hands it to a filler that populates the columns, then submits it — so
/// there is no separate build or submit step to forget. Columns are addressed by index or
/// by name, and the schema binding lets every identifier be validated as it is added — an
/// unknown name, an out-of-range index, a non-`INT32` column, or setting the same column
/// twice (whether by index or name) all fail eagerly rather than at write time.
///
/// ```java
/// writer.writeBatch(b -> b
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
/// writer.writeBatch(b -> b
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
    private final IntColumnSource[] sources;
    private final Validity[] validities;
    private final Map<String, Validity> structValidities = new HashMap<>();
    private final Map<String, int[]> listOffsets = new HashMap<>();
    private final Map<String, Validity> listValidities = new HashMap<>();
    private int rowCount = -1;
    private boolean consumed;

    ColumnBatch(FileSchema schema) {
        this.schema = schema;
        this.sources = new IntColumnSource[schema.getColumnCount()];
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
    /// filled separately through [#ints], its values holding the concatenated entries.
    ///
    /// @param listPath the list group's path (e.g. `"phones"`)
    /// @param offsets the entry offsets
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the path does not name a `LIST`, the list is
    ///         already set, or `offsets` is empty
    @Experimental
    public ColumnBatch list(String listPath, int[] offsets) {
        storeList(listPath, offsets, null);
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
        storeList(listPath, offsets, nulls);
        return this;
    }

    private void storeList(String listPath, int[] offsets, Validity nulls) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        if (offsets == null || offsets.length == 0) {
            throw new IllegalArgumentException("offsets must be non-empty for list " + listPath);
        }
        SchemaNode.GroupNode group = resolveList(listPath);
        if (nulls != null && group.repetitionType() != RepetitionType.OPTIONAL) {
            throw new IllegalArgumentException("List " + listPath + " is " + group.repetitionType()
                    + "; nulls are only valid for an OPTIONAL list");
        }
        if (listOffsets.putIfAbsent(listPath, offsets) != null) {
            throw new IllegalArgumentException("List " + listPath + " is already set in this batch");
        }
        if (nulls != null) {
            listValidities.put(listPath, nulls);
        }
    }

    /// Resolves a dot-separated path to the `LIST` group it names, rejecting a missing path,
    /// a leaf, and a non-`LIST` group.
    private SchemaNode.GroupNode resolveList(String listPath) {
        SchemaNode node = schema.getRootNode();
        for (String segment : listPath.split("\\.", -1)) {
            if (!(node instanceof SchemaNode.GroupNode group)) {
                throw new IllegalArgumentException("No list at path " + listPath);
            }
            node = null;
            for (SchemaNode child : group.children()) {
                if (child.name().equals(segment)) {
                    node = child;
                    break;
                }
            }
            if (node == null) {
                throw new IllegalArgumentException("No list at path " + listPath);
            }
        }
        if (!(node instanceof SchemaNode.GroupNode group) || !group.isList()) {
            throw new IllegalArgumentException("Path " + listPath + " does not name a list group");
        }
        return group;
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by index.
    ///
    /// The array is referenced, not copied, so it must not be mutated until the batch
    /// has been written.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the index is out of range, the column is
    ///         already set, or the length does not match the other columns in this batch
    public ColumnBatch ints(int columnIndex, int[] values) {
        store(checkedIndex(columnIndex), values, null);
        return this;
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by name.
    ///
    /// The array is referenced, not copied, so it must not be mutated until the batch
    /// has been written.
    ///
    /// @param columnName the column's name
    /// @param values the column's values for this batch
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if no column has that name, the column is already
    ///         set (by name or index), or the length does not match the other columns
    public ColumnBatch ints(String columnName, int[] values) {
        // getColumn(String) throws on an unknown name, so the identifier is validated here.
        store(schema.getColumn(columnName).columnIndex(), values, null);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by index.
    ///
    /// The values array is referenced, not copied, so it must not be mutated until the
    /// batch has been written; it is full length — one slot per row — and the entry at a
    /// null row is ignored.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @param nulls the column's nulls; `nulls.isNull(i)` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the index is out of range, the column is not
    ///         `OPTIONAL`, or the column is already set
    @Experimental
    public ColumnBatch ints(int columnIndex, int[] values, Validity nulls) {
        storeNullable(checkedIndex(columnIndex), values, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by name.
    ///
    /// @param columnName the column's name
    /// @param values the column's values for this batch
    /// @param nulls the column's nulls; `nulls.isNull(i)` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if no column has that name, the column is not
    ///         `OPTIONAL`, or the column is already set
    @Experimental
    public ColumnBatch ints(String columnName, int[] values, Validity nulls) {
        storeNullable(schema.getColumn(columnName).columnIndex(), values, nulls);
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by index, with nulls given
    /// as a plain mask. Convenience sugar over [#ints(int, int[], Validity)] and
    /// [Validity#ofNulls]; unlike the [Validity] form, the mask length is checked against
    /// the values.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @param nulls the per-row null mask; `nulls[i] == true` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the index is out of range, the column is not
    ///         `OPTIONAL`, the column is already set, or the lengths do not agree
    public ColumnBatch ints(int columnIndex, int[] values, boolean[] nulls) {
        int idx = checkedIndex(columnIndex);
        storeNullable(idx, values, maskToValidity(idx, values, nulls));
        return this;
    }

    /// Adds the values for an `OPTIONAL INT32` column, addressed by name, with nulls given
    /// as a plain mask. Convenience sugar over [#ints(String, int[], Validity)] and
    /// [Validity#ofNulls].
    ///
    /// @param columnName the column's name
    /// @param values the column's values for this batch
    /// @param nulls the per-row null mask; `nulls[i] == true` marks row `i` null
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if no column has that name, the column is not
    ///         `OPTIONAL`, the column is already set, or the lengths do not agree
    public ColumnBatch ints(String columnName, int[] values, boolean[] nulls) {
        int idx = schema.getColumn(columnName).columnIndex();
        storeNullable(idx, values, maskToValidity(idx, values, nulls));
        return this;
    }

    private int checkedIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= sources.length) {
            throw new IllegalArgumentException(
                    "Column index " + columnIndex + " is out of range [0, " + sources.length + ")");
        }
        return columnIndex;
    }

    private Validity maskToValidity(int columnIndex, int[] values, boolean[] nulls) {
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for column " + describe(columnIndex)
                    + "; use the mask-less ints(...) for an all-present column");
        }
        if (values != null && values.length != nulls.length) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " has " + values.length
                    + " values but " + nulls.length + " null flags");
        }
        return Validity.ofNulls(nulls);
    }

    private void storeNullable(int columnIndex, int[] values, Validity nulls) {
        ColumnSchema column = schema.getColumn(columnIndex);
        if (column.repetitionType() != RepetitionType.OPTIONAL) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is "
                    + column.repetitionType() + "; a null mask is only valid for an OPTIONAL column");
        }
        if (nulls == null) {
            throw new IllegalArgumentException("nulls must not be null for column " + describe(columnIndex)
                    + "; use the mask-less ints(...) for an all-present column");
        }
        store(columnIndex, values, nulls);
    }

    private void store(int columnIndex, int[] values, Validity validity) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null for column " + describe(columnIndex));
        }
        ColumnSchema column = schema.getColumn(columnIndex);
        if (column.type() != PhysicalType.INT32) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is " + column.type()
                    + ", not INT32");
        }
        if (sources[columnIndex] != null) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is already set in this batch");
        }
        // A repeated leaf's values count its concatenated elements, not the batch's records,
        // so it stays out of the record-count agreement the flat columns must satisfy.
        if (column.maxRepetitionLevel() == 0) {
            if (rowCount < 0) {
                rowCount = values.length;
            }
            else if (rowCount != values.length) {
                throw new IllegalArgumentException("Ragged batch: column " + describe(columnIndex) + " has "
                        + values.length + " values but the batch row count is " + rowCount);
            }
        }
        sources[columnIndex] = new IntArrayColumnSource(values);
        validities[columnIndex] = validity;
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
    IntColumnSource[] completedSources() {
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

    /// The `LIST` entry offsets set in this batch, keyed by list group path.
    Map<String, int[]> listOffsets() {
        return listOffsets;
    }

    /// The `LIST`-layer nulls set in this batch, keyed by list group path. A path absent
    /// from the map is an all-present list.
    Map<String, Validity> listValidities() {
        return listValidities;
    }
}
