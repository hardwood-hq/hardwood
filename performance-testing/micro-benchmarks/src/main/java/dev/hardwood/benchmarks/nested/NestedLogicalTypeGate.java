/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks.nested;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.nested.NestedListFileGenerator.NullDensity;
import dev.hardwood.benchmarks.nested.NestedLogicalTypeFileGenerator.LogicalElem;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;

/// Correctness gate for [NestedLogicalTypeReadBenchmark]. Generates the logical-type
/// list corpus and, for each `(elem, density)`, folds the same leaves through the
/// typed accessor (`dates()` / `timestamps()` / `decimals()`) and through the generic
/// `values()`, asserting both the fold and the null count agree before any timing is
/// trusted.
///
/// The two arms reach the value by different routes — the typed one reads the stored
/// primitive out of the column array, the generic one decodes a boxed value through
/// `LogicalTypeConverter` — so a decode that goes wrong on one side shows up here as
/// a mismatch rather than in the benchmark as a faster number.
///
/// Run through the benchmarks uberjar:
/// ```
/// java -cp benchmarks.jar dev.hardwood.benchmarks.nested.NestedLogicalTypeGate [dataDir]
/// ```
public final class NestedLogicalTypeGate {

    private static final String LIST_FIELD = "vec";

    private NestedLogicalTypeGate() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : BenchmarkData.dir());
        long totalValues = BenchmarkData.totalValues();

        System.out.println("Nested logical-type correctness gate:");
        try (HardwoodContext context = HardwoodContext.create()) {
            for (LogicalElem elem : LogicalElem.values()) {
                for (NullDensity density : NullDensity.values()) {
                    NestedLogicalTypeFileGenerator.ensureList(dir, elem, density, totalValues);
                    Path path = NestedLogicalTypeFileGenerator.listFile(dir, elem, density);
                    Fold typed = foldTyped(path, elem, context);
                    Fold generic = foldGeneric(path, elem, context);
                    require(elem + "/" + density + " typed vs generic", typed, generic);
                    System.out.printf("  OK  elem=%-10s density=%-7s values=%,d nulls=%,d sum=%d%n",
                            elem.token(), density.token(), typed.values(), typed.nulls(), typed.sum());
                }
            }
        }
        System.out.println("Gate passed — the typed and generic routes agree on every leaf.");
    }

    /// What one pass over the corpus saw: how many elements it visited, how many of
    /// them were null, and the sum the non-null ones fold to.
    private record Fold(long values, long nulls, long sum) {}

    private static Fold foldTyped(Path path, LogicalElem elem, HardwoodContext context) throws IOException {
        long values = 0;
        long nulls = 0;
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(LIST_FIELD);
                if (vec == null) {
                    continue;
                }
                List<?> decoded = switch (elem) {
                    case DATE -> vec.dates();
                    case TIMESTAMP -> vec.timestamps();
                    case DECIMAL -> vec.decimals();
                };
                for (int i = 0, n = decoded.size(); i < n; i++) {
                    values++;
                    Object value = decoded.get(i);
                    if (value == null) {
                        nulls++;
                    }
                    else {
                        sum += fold(value);
                    }
                }
            }
        }
        return new Fold(values, nulls, sum);
    }

    private static Fold foldGeneric(Path path, LogicalElem elem, HardwoodContext context) throws IOException {
        long values = 0;
        long nulls = 0;
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(LIST_FIELD);
                if (vec == null) {
                    continue;
                }
                List<Object> decoded = vec.values();
                for (int i = 0, n = decoded.size(); i < n; i++) {
                    values++;
                    Object value = decoded.get(i);
                    if (value == null) {
                        nulls++;
                    }
                    else {
                        sum += fold(value);
                    }
                }
            }
        }
        return new Fold(values, nulls, sum);
    }

    /// One decoded value as the `long` both arms fold, so the comparison is over the
    /// value each route arrived at rather than over its identity.
    private static long fold(Object value) {
        return switch (value) {
            case LocalDate date -> date.toEpochDay();
            case Instant instant -> instant.toEpochMilli();
            case BigDecimal decimal -> decimal.unscaledValue().longValue();
            default -> throw new IllegalStateException(
                    "Unexpected decoded type: " + value.getClass() + " (" + value + ")");
        };
    }

    private static void require(String what, Fold typed, Fold generic) {
        if (!typed.equals(generic)) {
            throw new IllegalStateException("Mismatch (" + what + "): " + typed + " vs " + generic);
        }
        if (typed.values() == 0) {
            throw new IllegalStateException("No values read (" + what + ")");
        }
    }
}
