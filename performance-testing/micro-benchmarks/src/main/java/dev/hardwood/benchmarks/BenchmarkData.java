/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

/// Shared benchmark-data configuration for the nested-read benchmarks: the corpus
/// directory and the fixture sizes, overridable from the command line.
public final class BenchmarkData {

    /// Leaf-value count per list / repeated / depth fixture, matching the
    /// fixed-size-list corpus.
    public static final long DEFAULT_TOTAL_VALUES = 8_000_000L;
    /// Rows in the mixed / struct fixtures.
    public static final long DEFAULT_ROWS = 2_000_000L;

    private static final String DEFAULT_DIR =
            "performance-testing/test-data-setup/target/benchmark-data";

    private BenchmarkData() {
    }

    /// Corpus directory, `-Dperf.dataDir` or the shared benchmark-data directory.
    public static String dir() {
        String dir = System.getProperty("perf.dataDir");
        return dir == null || dir.isBlank() ? DEFAULT_DIR : dir;
    }

    /// Leaf-value count per list / repeated / depth fixture, `-Dperf.totalValues`.
    public static long totalValues() {
        return Long.getLong("perf.totalValues", DEFAULT_TOTAL_VALUES);
    }

    /// Row count for the mixed / struct fixtures, `-Dperf.rows`.
    public static long rows() {
        return Long.getLong("perf.rows", DEFAULT_ROWS);
    }
}
