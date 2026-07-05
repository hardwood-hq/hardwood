/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.nested;

import java.io.IOException;
import java.nio.file.Path;

import dev.hardwood.HardwoodContext;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.Elem;
import dev.hardwood.benchmarks.NestedReads;
import dev.hardwood.benchmarks.nested.NestedListFileGenerator.NullDensity;

/// Correctness gate for [NestedListReadBenchmark]. Generates the list corpus and, for
/// each `(elem, density)`, folds the column real-items path, the row all-items path,
/// and the flat floor, asserting all three agree before any timing is trusted — they
/// read the identical leaf stream.
///
/// Run through the benchmarks uberjar:
/// ```
/// java -cp benchmarks.jar dev.hardwood.benchmarks.nested.NestedListGate [dataDir]
/// ```
public final class NestedListGate {

    private static final double EPSILON = 1e-6;

    private NestedListGate() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        NestedListFileGenerator.generateAll(dir, BenchmarkData.totalValues());

        System.out.println("Nested-list correctness gate:");
        try (HardwoodContext context = HardwoodContext.create()) {
            for (Elem elem : Elem.values()) {
                for (NullDensity density : NullDensity.values()) {
                    Path listPath = NestedListFileGenerator.listFile(dir, elem, density);
                    Path flatPath = NestedListFileGenerator.listFlatFile(dir, elem, density);
                    double column = NestedReads.sumColumn(listPath, NestedListFileGenerator.LIST_LEAF, elem, context);
                    double row = NestedReads.sumRowsList(listPath, "vec", elem, context);
                    double flat = NestedReads.sumColumn(flatPath, "value", elem, context);
                    requireClose("list " + elem + "/" + density + " column vs flat floor", column, flat);
                    requireClose("list " + elem + "/" + density + " row vs column", row, column);
                    System.out.printf("  OK  list elem=%-8s density=%-7s sum=%s%n",
                            elem.token(), density.token(), column);
                }
            }
        }
        System.out.println("Gate passed — every path agrees on the values it reads.");
    }

    private static void requireClose(String what, double a, double b) {
        double scale = Math.max(1.0, Math.max(Math.abs(a), Math.abs(b)));
        if (Math.abs(a - b) > EPSILON * scale) {
            throw new IllegalStateException("Mismatch (" + what + "): " + a + " vs " + b);
        }
    }
}
