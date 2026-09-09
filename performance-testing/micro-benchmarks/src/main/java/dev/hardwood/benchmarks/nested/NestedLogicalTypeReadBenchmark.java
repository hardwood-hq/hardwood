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
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.benchmarks.BenchmarkData;
import dev.hardwood.benchmarks.nested.NestedListFileGenerator.NullDensity;
import dev.hardwood.benchmarks.nested.NestedLogicalTypeFileGenerator.LogicalElem;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;

/// The row reader's typed accessors over a top-level `LIST<annotated>` — the decode a
/// caller pays per element once the bytes are in memory.
///
/// - `typedView` — the accessor that names the type it returns (`dates()`,
///   `timestamps()`, `decimals()`). It reads the stored primitive out of the column
///   array and decodes it, so nothing is boxed on the way to the value.
/// - `genericView` — `values()`, which returns whatever the column holds. Its result
///   is an `Object` by contract, so it starts from a boxed value and dispatches. It is
///   here as the contrast: the same bytes and the same decode, reached the other way.
///
/// The gap between the two is what naming the return type buys. Element type spans the
/// three representations a decode starts from — `INT32`, `INT64` and a fixed-width byte
/// array — because they do not cost the same: the first two carried a box that naming
/// the type removes, while the byte-array types instead carried a copy of the payload,
/// which [dev.hardwood.internal.reader.BinaryBatchValues] now reads in place.
///
/// Generate the corpus first (also done on demand from `@Setup`):
/// ```
/// java ... dev.hardwood.benchmarks.nested.NestedLogicalTypeFileGenerator <dataDir>
/// ```
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class NestedLogicalTypeReadBenchmark {

    private static final String LIST_FIELD = "vec";

    @Param({ "date", "timestamp", "decimal" })
    private String elem;

    @Param({ "none", "sparse" })
    private String nullDensity;

    private LogicalElem elemKind;
    private Path listPath;
    private HardwoodContext context;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        elemKind = LogicalElem.valueOf(elem.toUpperCase());
        NullDensity density = NullDensity.valueOf(nullDensity.toUpperCase());
        Path dir = Path.of(BenchmarkData.dir());
        NestedLogicalTypeFileGenerator.ensureList(dir, elemKind, density, BenchmarkData.totalValues());
        listPath = NestedLogicalTypeFileGenerator.listFile(dir, elemKind, density);
        context = HardwoodContext.create();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        context.close();
    }

    @Benchmark
    public long typedView() throws IOException {
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(listPath), context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(LIST_FIELD);
                if (vec == null) {
                    continue;
                }
                sum += switch (elemKind) {
                    case DATE -> foldDates(vec);
                    case TIMESTAMP -> foldTimestamps(vec);
                    case DECIMAL -> foldDecimals(vec);
                };
            }
        }
        return sum;
    }

    @Benchmark
    public long genericView() throws IOException {
        long sum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(listPath), context);
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList(LIST_FIELD);
                if (vec == null) {
                    continue;
                }
                List<Object> values = vec.values();
                for (int i = 0, n = values.size(); i < n; i++) {
                    Object value = values.get(i);
                    if (value != null) {
                        sum += value.hashCode();
                    }
                }
            }
        }
        return sum;
    }

    private static long foldDates(PqList vec) {
        long sum = 0;
        List<LocalDate> dates = vec.dates();
        for (int i = 0, n = dates.size(); i < n; i++) {
            LocalDate date = dates.get(i);
            if (date != null) {
                sum += date.toEpochDay();
            }
        }
        return sum;
    }

    private static long foldTimestamps(PqList vec) {
        long sum = 0;
        List<Instant> stamps = vec.timestamps();
        for (int i = 0, n = stamps.size(); i < n; i++) {
            Instant stamp = stamps.get(i);
            if (stamp != null) {
                sum += stamp.getEpochSecond();
            }
        }
        return sum;
    }

    private static long foldDecimals(PqList vec) {
        long sum = 0;
        List<BigDecimal> decimals = vec.decimals();
        for (int i = 0, n = decimals.size(); i < n; i++) {
            BigDecimal decimal = decimals.get(i);
            if (decimal != null) {
                sum += decimal.unscaledValue().longValue();
            }
        }
        return sum;
    }
}
