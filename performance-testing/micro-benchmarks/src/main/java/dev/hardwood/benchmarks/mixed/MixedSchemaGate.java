/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.mixed;

import java.io.IOException;
import java.nio.file.Path;

import dev.hardwood.HardwoodContext;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.NestedReads;
import dev.hardwood.reader.ParquetFileReader;

/// Correctness gate for [MixedSchemaReadBenchmark]. Generates the mixed-schema corpus
/// and asserts the paths that read the same values agree before any timing is trusted:
/// the 12 scalar columns fold identically whether or not the file also holds list
/// columns; the struct leaves match their flat twin; and the depth files agree across
/// the column and row paths.
///
/// Run through the benchmarks uberjar:
/// ```
/// java -cp benchmarks.jar dev.hardwood.benchmarks.mixed.MixedSchemaGate [dataDir]
/// ```
public final class MixedSchemaGate {

    private static final double EPSILON = 1e-6;

    private MixedSchemaGate() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        MixedSchemaFileGenerator.generateAll(dir, BenchmarkData.rows(), BenchmarkData.totalValues());

        System.out.println("Mixed-schema correctness gate:");
        try (HardwoodContext context = HardwoodContext.create()) {
            gateMixed(dir, context);
            gateStruct(dir, context);
            gateDepth(dir, context);
        }
        System.out.println("Gate passed — every path agrees on the values it reads.");
    }

    /// The 12 scalar columns must fold identically whether or not the file also holds
    /// list columns.
    private static void gateMixed(Path dir, HardwoodContext context) throws IOException {
        double mixed = sumScalars(MixedSchemaFileGenerator.mixedFile(dir), context);
        double flat = sumScalars(MixedSchemaFileGenerator.flatScalarsFile(dir), context);
        requireClose("mixed scalars vs flat scalars", mixed, flat);
        System.out.printf("  OK  mixed scalars sum=%s%n", mixed);
    }

    private static void gateStruct(Path dir, HardwoodContext context) throws IOException {
        double nested = sumStruct(MixedSchemaFileGenerator.structFile(dir), "s.a", "s.b", "s.c", context);
        double flat = sumStruct(MixedSchemaFileGenerator.structFlatFile(dir), "a", "b", "c", context);
        requireClose("struct vs struct flat", nested, flat);
        System.out.printf("  OK  struct sum=%s%n", nested);
    }

    /// The multi-repetition-layer files: the column and row paths must agree.
    private static void gateDepth(Path dir, HardwoodContext context) throws IOException {
        Path listOfList = MixedSchemaFileGenerator.listOfListFile(dir);
        double lolColumn;
        try (ParquetFileReader reader = NestedReads.open(listOfList, context)) {
            lolColumn = NestedReads.sumDoubleColumn(reader, 0);
        }
        double lolRow = NestedReads.sumRowsListOfList(listOfList, "vec", context);
        requireClose("list-of-list row vs column", lolRow, lolColumn);
        System.out.printf("  OK  list-of-list sum=%s%n", lolColumn);

        Path listOfStruct = MixedSchemaFileGenerator.listOfStructFile(dir);
        double losColumn;
        try (ParquetFileReader reader = NestedReads.open(listOfStruct, context)) {
            losColumn = NestedReads.sumLongColumn(reader, 0) + NestedReads.sumDoubleColumn(reader, 1);
        }
        double losRow = NestedReads.sumRowsListOfStruct(listOfStruct, "vec", context);
        requireClose("list-of-struct row vs column", losRow, losColumn);
        System.out.printf("  OK  list-of-struct sum=%s%n", losColumn);
    }

    private static double sumScalars(Path path, HardwoodContext context) throws IOException {
        double sum = 0;
        try (ParquetFileReader reader = NestedReads.open(path, context)) {
            for (int c = 0; c < 4; c++) {
                sum += NestedReads.sumIntColumn(reader, "i" + c);
            }
            for (int c = 0; c < 4; c++) {
                sum += NestedReads.sumLongColumn(reader, "l" + c);
            }
            for (int c = 0; c < 4; c++) {
                sum += NestedReads.sumDoubleColumn(reader, "d" + c);
            }
        }
        return sum;
    }

    private static double sumStruct(Path path, String a, String b, String c, HardwoodContext context)
            throws IOException {
        try (ParquetFileReader reader = NestedReads.open(path, context)) {
            return NestedReads.sumLongColumn(reader, a)
                    + NestedReads.sumDoubleColumn(reader, b)
                    + NestedReads.sumIntColumn(reader, c);
        }
    }

    private static void requireClose(String what, double a, double b) {
        double scale = Math.max(1.0, Math.max(Math.abs(a), Math.abs(b)));
        if (Math.abs(a - b) > EPSILON * scale) {
            throw new IllegalStateException("Mismatch (" + what + "): " + a + " vs " + b);
        }
    }
}
