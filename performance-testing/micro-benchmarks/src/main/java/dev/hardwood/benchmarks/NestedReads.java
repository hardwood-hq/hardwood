/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.file.Path;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.Validity;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqStruct;

/// Read paths shared by the nested-read benchmarks and their correctness gates. Every
/// fold reduces a fixture to a single `double` — present numeric leaves summed, longs
/// and ints widened — so the [ColumnReader] (real-items) path, the [RowReader]
/// (all-items) path, and the flat floor all produce the same value when they read the
/// same leaf stream. Null leaves (marked by leaf validity, or by [PqList#isNull]) are
/// skipped on every path so the sums agree.
public final class NestedReads {

    private NestedReads() {
    }

    public static ParquetFileReader open(Path path, HardwoodContext context) throws IOException {
        return ParquetFileReader.open(InputFile.of(path), context);
    }

    // ==================== Column (real-items) folds ====================

    private static double foldInt(ColumnReader col) {
        double sum = 0;
        while (col.nextBatch()) {
            int n = col.getValueCount();
            int[] values = col.getInts();
            Validity validity = col.getLeafValidity();
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < n; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }

    private static double foldLong(ColumnReader col) {
        double sum = 0;
        while (col.nextBatch()) {
            int n = col.getValueCount();
            long[] values = col.getLongs();
            Validity validity = col.getLeafValidity();
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < n; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }

    private static double foldDouble(ColumnReader col) {
        double sum = 0;
        while (col.nextBatch()) {
            int n = col.getValueCount();
            double[] values = col.getDoubles();
            Validity validity = col.getLeafValidity();
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < n; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
        return sum;
    }

    public static double sumIntColumn(ParquetFileReader reader, String leaf) throws IOException {
        try (ColumnReader col = reader.columnReader(leaf)) {
            return foldInt(col);
        }
    }

    public static double sumLongColumn(ParquetFileReader reader, String leaf) throws IOException {
        try (ColumnReader col = reader.columnReader(leaf)) {
            return foldLong(col);
        }
    }

    public static double sumLongColumn(ParquetFileReader reader, int columnIndex) throws IOException {
        try (ColumnReader col = reader.columnReader(columnIndex)) {
            return foldLong(col);
        }
    }

    public static double sumDoubleColumn(ParquetFileReader reader, String leaf) throws IOException {
        try (ColumnReader col = reader.columnReader(leaf)) {
            return foldDouble(col);
        }
    }

    public static double sumDoubleColumn(ParquetFileReader reader, int columnIndex) throws IOException {
        try (ColumnReader col = reader.columnReader(columnIndex)) {
            return foldDouble(col);
        }
    }

    /// Folds a single numeric leaf of a standalone file through the column reader,
    /// selecting the accessor by element type. Opens and closes its own reader.
    public static double sumColumn(Path path, String leaf, Elem elem, HardwoodContext context)
            throws IOException {
        try (ParquetFileReader reader = open(path, context)) {
            return elem == Elem.INT64 ? sumLongColumn(reader, leaf) : sumDoubleColumn(reader, leaf);
        }
    }

    /// Folds several numeric leaves of one file, each through its own
    /// [ColumnReader], summing all of them on the calling (consumer) thread. The
    /// per-column workers decode and reconstruct in parallel while this one thread
    /// folds every column — the shape that exposes consumer-side reconstruction
    /// cost a single-column scan hides. Serves both the list-leaf columns and their
    /// flat twin (same call, different leaf names).
    public static double sumColumns(Path path, String[] leaves, Elem elem, HardwoodContext context)
            throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = open(path, context)) {
            ColumnReader[] columns = new ColumnReader[leaves.length];
            try {
                for (int i = 0; i < leaves.length; i++) {
                    columns[i] = reader.columnReader(leaves[i]);
                }
                for (ColumnReader column : columns) {
                    sum += elem == Elem.INT64 ? foldLong(column) : foldDouble(column);
                }
            }
            finally {
                for (ColumnReader column : columns) {
                    if (column != null) {
                        column.close();
                    }
                }
            }
        }
        return sum;
    }

    /// Folds several top-level `LIST<primitive>` fields of one file through the row
    /// reader, materializing each field's [PqList] per row. The all-items twin of
    /// [#sumColumns] for the multi-column fixture.
    public static double sumRowsMultiList(Path path, String[] fields, Elem elem, HardwoodContext context)
            throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = open(path, context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                for (String field : fields) {
                    PqList vec = rows.getList(field);
                    if (vec == null) {
                        continue;
                    }
                    int size = vec.size();
                    if (elem == Elem.INT64) {
                        PqLongList longs = vec.longs();
                        for (int i = 0; i < size; i++) {
                            if (!vec.isNull(i)) {
                                sum += longs.get(i);
                            }
                        }
                    }
                    else {
                        PqDoubleList doubles = vec.doubles();
                        for (int i = 0; i < size; i++) {
                            if (!vec.isNull(i)) {
                                sum += doubles.get(i);
                            }
                        }
                    }
                }
            }
        }
        return sum;
    }

    // ==================== Row (all-items) folds ====================

    /// Folds the elements of a top-level `LIST<primitive>` through the row reader,
    /// materializing a [PqList] per row and reading its elements without boxing.
    public static double sumRowsList(Path path, String field, Elem elem, HardwoodContext context)
            throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = open(path, context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(field);
                if (vec == null) {
                    continue;
                }
                int size = vec.size();
                if (elem == Elem.INT64) {
                    PqLongList longs = vec.longs();
                    for (int i = 0; i < size; i++) {
                        if (!vec.isNull(i)) {
                            sum += longs.get(i);
                        }
                    }
                }
                else {
                    PqDoubleList doubles = vec.doubles();
                    for (int i = 0; i < size; i++) {
                        if (!vec.isNull(i)) {
                            sum += doubles.get(i);
                        }
                    }
                }
            }
        }
        return sum;
    }

    /// Folds a `LIST<LIST<double>>` through the row reader (depth guard).
    public static double sumRowsListOfList(Path path, String field, HardwoodContext context)
            throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = open(path, context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList outer = rows.getList(field);
                if (outer == null) {
                    continue;
                }
                for (PqList inner : outer.lists()) {
                    if (inner == null) {
                        continue;
                    }
                    PqDoubleList doubles = inner.doubles();
                    int size = inner.size();
                    for (int i = 0; i < size; i++) {
                        if (!inner.isNull(i)) {
                            sum += doubles.get(i);
                        }
                    }
                }
            }
        }
        return sum;
    }

    /// Folds the `a` (long) and `b` (double) fields of a `LIST<STRUCT>` through the
    /// row reader (depth guard).
    public static double sumRowsListOfStruct(Path path, String field, HardwoodContext context)
            throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = open(path, context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(field);
                if (vec == null) {
                    continue;
                }
                for (PqStruct element : vec.structs()) {
                    if (element == null) {
                        continue;
                    }
                    sum += element.getLong("a");
                    sum += element.getDouble("b");
                }
            }
        }
        return sum;
    }
}
