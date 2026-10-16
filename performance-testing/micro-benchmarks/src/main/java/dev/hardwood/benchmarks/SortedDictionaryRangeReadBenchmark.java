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
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

/// SPIKE (#1258): `url < literal` over a dictionary-encoded column whose dictionaries are sorted,
/// read end to end through the row reader. Run once with and once without
/// `-Dhardwood.spike.sortedDictionaries=true` to compare the per-entry and the id-interval paths.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 4, time = 2)
public class SortedDictionaryRangeReadBenchmark {

    public enum Shape {
        /// Projects `id`, filters on `url`: the filter column is never materialized.
        FILTER_ONLY,
        /// Projects and reads `url` as a `String` for every match.
        READ_URL,
        /// As `READ_URL`, also filtering on `id < 20000`, so the page index prunes the read to
        /// a few pages of the first row group.
        PRUNED,
        /// As `READ_URL`, limited to the first 10 matches.
        HEAD
    }

    @Param("")
    private String file;

    /// The 9-digit prefix the literal cuts at; prefixes are uniform over `[0, 10^9)`.
    @Param({"010000000", "500000000"})
    private String cut;

    @Param
    private Shape shape;

    /// `LT`: `url < literal`; `IN50`: `url IN` 50 URLs of the column's shape, an expensive
    /// per-entry decision.
    @Param({"LT"})
    private String predicate;

    private java.util.Set<String> members;

    private static final long PRUNED_ID_LIMIT = 20_000L;
    private static final long HEAD_ROWS = 10L;

    private FilterPredicate filter;
    private String literal;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        literal = "https://example.com/" + cut;
        String[] memberArray = new String[50];
        for (int i = 0; i < memberArray.length; i++) {
            // The generator's URL for key i, so every member occurs in the file.
            memberArray[i] = String.format("https://example.com/%09d/item-%07d", i * 2654435761L % 1_000_000_000L, i);
        }
        members = java.util.Set.of(memberArray);
        FilterPredicate urlFilter = predicate.equals("IN50")
                ? FilterPredicate.in("url", memberArray)
                : FilterPredicate.lt("url", literal);
        filter = shape == Shape.PRUNED
                ? FilterPredicate.and(FilterPredicate.lt("id", PRUNED_ID_LIMIT), urlFilter)
                : urlFilter;
        long expected = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Path.of(file)));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id", "url")).build()) {
            while (rows.hasNext()) {
                rows.next();
                String url = rows.getString(1);
                if ((predicate.equals("IN50") ? members.contains(url) : url.compareTo(literal) < 0)
                        && (shape != Shape.PRUNED || rows.getLong(0) < PRUNED_ID_LIMIT)) {
                    expected++;
                }
            }
        }
        if (shape == Shape.HEAD) {
            expected = Math.min(expected, HEAD_ROWS);
        }
        long actual = count();
        if (actual != expected) {
            throw new IllegalStateException("Filtered read found " + actual + " rows, a full scan " + expected);
        }
        System.out.printf("%nGate passed: %,d matching rows (interval %s, entry mode %s)%n", expected,
                Boolean.getBoolean("hardwood.spike.sortedDictionaries") ? "ON" : "OFF",
                System.getProperty("hardwood.spike.entryMode", "lazy"));
    }

    @Benchmark
    public long read() throws IOException {
        return run();
    }

    private long count() throws IOException {
        long count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Path.of(file)));
             RowReader rows = builder(reader, "id").build()) {
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
        }
        return count;
    }

    private ParquetFileReader.RowReaderBuilder builder(ParquetFileReader reader, String column) {
        ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader()
                .projection(ColumnProjection.columns(column)).filter(filter);
        return shape == Shape.HEAD ? builder.head(HEAD_ROWS) : builder;
    }

    private long run() throws IOException {
        long checksum = 0;
        String column = shape == Shape.FILTER_ONLY ? "id" : "url";
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(Path.of(file)));
             RowReader rows = builder(reader, column).build()) {
            while (rows.hasNext()) {
                rows.next();
                checksum += shape == Shape.FILTER_ONLY ? rows.getLong(0) : rows.getString(0).length();
            }
        }
        return checksum;
    }
}
