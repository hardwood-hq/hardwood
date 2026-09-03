/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.format.BloomFilterAlgorithm;
import org.apache.parquet.format.BloomFilterCompression;
import org.apache.parquet.format.BloomFilterHash;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.SplitBlockAlgorithm;
import org.apache.parquet.format.Uncompressed;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.XxHash;
import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.XxHash64;
import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/// The I/O cost model of bloom filter reads during row-group pruning, pinned as request counts
/// rather than wall-clock (see `_designs/BLOOM_FILTER_IO_COALESCING.md`, issue #735).
///
/// Over `bloom_filter_multi_rg_test.parquet` (5 row groups of 200 rows; bloom filters on `id`,
/// `name`, `code`; `value` carries none; no dictionary, compression, or page index, so a filtered
/// read's request counts isolate bloom fetches):
///
/// - without prefetch, an `eq` whose value falls inside every row group's statistics range issues
///   one lazy bloom read per row group, sequentially;
/// - with prefetch, the same read merges the column's contiguous filters into a single ranged GET;
/// - queries no eligible leaf can serve — no filter, range-only predicates, metadata filtering
///   disabled — issue no bloom reads at all; and
/// - pruning decisions are identical with prefetch on and off.
class BloomFilterIoCoalescingTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/bloom_filter_multi_rg_test.parquet");

    // Column order in the fixture schema: id(0), value(1), name(2), code(3).
    private static final int CODE_COLUMN = 3;

    /// Bloom-attributed reads: merged region fetches plus lazy fallbacks. Region fetches carry the
    /// `bloom region ...` scope; lazy (unprefetched or fallback) reads carry `bloom filter ...`.
    private record Reads(int bloomReads, int regionReads, int lazyReads, int rows) {}

    /// Reads `code` with the given filter and prefetch mode. The
    /// `hardwood.internal.bloomPrefetch` property is JVM-global but read per planner invocation;
    /// it is set just before the read and restored in `finally` so no other test inherits it.
    private static Reads read(FilterPredicate filter, boolean prefetchEnabled) throws Exception {
        return read(filter, prefetchEnabled, InputFile.of(FIXTURE));
    }

    private static Reads read(FilterPredicate filter, boolean prefetchEnabled, InputFile input)
            throws Exception {
        CountingInputFile file = new CountingInputFile(input);
        file.open();
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            String previous = System.getProperty("hardwood.internal.bloomPrefetch");
            try {
                System.setProperty("hardwood.internal.bloomPrefetch", Boolean.toString(prefetchEnabled));
                ParquetFileReader.ColumnReaderBuilder builder = reader.buildColumnReader("code");
                if (filter != null) {
                    builder.filter(filter);
                }
                int rows = 0;
                try (ColumnReader values = builder.build()) {
                    while (values.nextBatch()) {
                        rows += values.getRecordCount();
                    }
                }
                return new Reads(file.readCount("bloom"), file.readCount("bloom region"),
                        file.readCount("bloom filter"), rows);
            } finally {
                if (previous == null) {
                    System.clearProperty("hardwood.internal.bloomPrefetch");
                } else {
                    System.setProperty("hardwood.internal.bloomPrefetch", previous);
                }
            }
        }
    }

    @Test
    void contiguousFiltersFetchAsOneRegion() throws Exception {
        // code = 1 is never written (values are multiples of 3) and sits inside every row group's
        // statistics range, so all 5 row groups' filters are candidates. They are written
        // contiguously, so prefetch fetches them as one merged GET; without prefetch the same read
        // pays one lazy GET per row group.
        Reads prefetched = read(FilterPredicate.eq("code", 1), true);
        Reads lazy = read(FilterPredicate.eq("code", 1), false);

        assertThat(prefetched.regionReads())
                .as("the five contiguous filters coalesce into one region fetch")
                .isEqualTo(1);
        assertThat(prefetched.lazyReads()).isZero();
        assertThat(lazy.regionReads()).isZero();
        assertThat(lazy.lazyReads())
                .as("the reference run reads one filter per statistics-surviving row group")
                .isEqualTo(5);
    }

    @Test
    void regionFetchesOverlapConcurrently() throws Exception {
        // The parallel half of the issue: request counts cannot distinguish concurrent regions
        // from sequential ones, so the first region read blocks until a second is in flight.
        // A serialized implementation waits the timeout out, records no overlap, and fails.
        CountingInputFile counted = new CountingInputFile(InputFile.of(FIXTURE));
        counted.open();
        OverlappingRegions input = new OverlappingRegions(counted);
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(gappedCandidates()), input);
        assertThat(input.overlapped.get())
                .as("the two region fetches must be in flight at the same time")
                .isTrue();
        assertThat(counted.readCount("bloom region")).isEqualTo(2);
        assertThat(prefetch).isNotNull();
    }

    @Test
    void candidatesBeyondTheCoalescingGapFetchAsSeparateRegions() throws Exception {
        // The gap tolerance bounds what one GET may bridge: candidates far enough apart stay
        // two regions, each still prefetched rather than read lazily.
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(gappedCandidates()), file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom region")).isEqualTo(2);
        assertThat(file.readCount("bloom filter")).isZero();
        assertThat(file.readReasons())
                .as("each region read names its exact span")
                .contains("bloom region 1000..1132", "bloom region 70000..70032");
        assertThat(prefetch.lookup(1000)).isNotNull();
        assertThat(prefetch.lookup(1100)).isNotNull();
        assertThat(prefetch.lookup(70000)).isNotNull();
    }

    @Test
    void nestedRegionsRetainTheOuterSpan() throws Exception {
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        List<BloomFilterReadPlanner.BloomCandidate> candidates = List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32),
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1010, 10));
        for (List<BloomFilterReadPlanner.BloomCandidate> order : List.of(
                candidates, List.of(candidates.get(1), candidates.get(0)))) {
            CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
            file.open();
            BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                    new BloomFilterReadPlanner.BloomFilterReadPlan(order), file);
            assertThat(prefetch).isNotNull();
            assertThat(file.readCount("bloom region")).isEqualTo(1);
            assertThat(prefetch.lookup(1000)).isNotNull();
            assertThat(prefetch.lookup(1010)).isNotNull();
        }
    }

    @Test
    void conflictingDuplicateOffsetsStayOnTheLazyPath() throws Exception {
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        List<BloomFilterReadPlanner.BloomCandidate> candidates = List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32),
                new BloomFilterReadPlanner.BloomCandidate(1, rowGroup, CODE_COLUMN, 1000, 64));
        for (List<BloomFilterReadPlanner.BloomCandidate> order : List.of(
                candidates, List.of(candidates.get(1), candidates.get(0)))) {
            CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
            file.open();
            BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                    new BloomFilterReadPlanner.BloomFilterReadPlan(order), file);
            assertThat(prefetch).isNull();
            assertThat(file.readCount("bloom")).isZero();
        }
    }

    @Test
    void singletonLargerThanRegionCapStaysLazy() throws Exception {
        int overCap = 16 * 1024 * 1024 + 1;
        int pairOffset = overCap + 10_000;
        byte[] fileBytes = new byte[pairOffset + 128];
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        List<BloomFilterReadPlanner.BloomCandidate> candidates = List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                        100, overCap - 1),
                new BloomFilterReadPlanner.BloomCandidate(1, rowGroup, CODE_COLUMN, 64, overCap),
                new BloomFilterReadPlanner.BloomCandidate(2, rowGroup, CODE_COLUMN, pairOffset, 32),
                new BloomFilterReadPlanner.BloomCandidate(3, rowGroup, CODE_COLUMN,
                        pairOffset + 64, 32));
        file.open();
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(candidates), file);
        assertThat(prefetch).isNotNull();
        assertThat(prefetch.lookup(64)).isNull();
        assertThat(prefetch.lookup(100)).isNotNull();
        assertThat(file.readCount("bloom region")).isEqualTo(2);
        assertThat(file.readReasons()).doesNotContain("bloom region 64..16777281");
    }


    @Test
    void largeLegacyExtentCanReachKnownNeighbour() throws Exception {
        long presentHash = XxHash64.hash(11);
        byte[] legacyFilter = serializeMinimalFilter(presentHash, 131072);
        int legacyOffset = 1000;
        int knownOffset = legacyOffset + legacyFilter.length;
        byte[] fileBytes = new byte[knownOffset + legacyFilter.length + 32];
        System.arraycopy(legacyFilter, 0, fileBytes, legacyOffset, legacyFilter.length);
        System.arraycopy(legacyFilter, 0, fileBytes, knownOffset, legacyFilter.length);
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        List<BloomFilterReadPlanner.BloomCandidate> candidates = List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                        legacyOffset, null),
                new BloomFilterReadPlanner.BloomCandidate(1, rowGroup, CODE_COLUMN,
                        knownOffset, legacyFilter.length));
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        file.open();
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(candidates), file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(1);
        assertThat(file.readCount("bloom region")).isEqualTo(1);
        assertThat(prefetch.lookup(legacyOffset)).isNotNull();
        assertThat(prefetch.lookup(knownOffset)).isNotNull();
    }

    @Test
    void retainedLegacyProbeRequiresPositiveFilterLength() {
        BloomFilterPrefetch.PrefetchedProbe probe =
                new BloomFilterPrefetch.PrefetchedProbe(5);
        assertThat(probe.filterLength()).isEqualTo(5);

        assertThatThrownBy(() -> new BloomFilterPrefetch.PrefetchedProbe(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retainedLegacyProbeAvoidsRepeatingTheHeaderRead() throws Exception {
        long presentHash = XxHash64.hash(17);
        byte[] filter = serializeMinimalFilter(presentHash, 1024);
        int offset = 64;
        byte[] fileBytes = new byte[offset + filter.length + 32];
        System.arraycopy(filter, 0, fileBytes, offset, filter.length);
        RowGroup rowGroup = syntheticSingleColumnRowGroups(offset, offset + filter.length + 1)
                .getFirst();
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        file.open();
        BloomFilterPrefetch retainedProbe = new BloomFilterPrefetch() {
            @Override
            public PrefetchedBloom lookup(long ignored) {
                return null;
            }

            @Override
            public PrefetchedProbe lookupProbe(long candidateOffset) {
                return candidateOffset == offset
                        ? new PrefetchedProbe(filter.length)
                        : null;
            }
        };

        BloomFilter served = new RowGroupBloomFilterSource(file, rowGroup, retainedProbe)
                .forColumn(0);

        assertThat(served).isNotNull();
        assertThat(served.mightContain(presentHash)).isTrue();
        assertThat(file.readCount("bloom probe")).isZero();
        assertThat(file.readCount("bloom filter")).isEqualTo(1);
    }

    @Test
    void overflowingCandidateSpanStaysOnTheLazyPath() throws Exception {
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        List<BloomFilterReadPlanner.BloomCandidate> candidates = List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                        Long.MAX_VALUE, 32),
                new BloomFilterReadPlanner.BloomCandidate(1, rowGroup, CODE_COLUMN, 1000, 32));
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(candidates), file);
        assertThat(prefetch).isNull();
        assertThat(file.readCount("bloom")).isZero();
    }

    @Test
    void candidatesExactlyAtTheCoalescingGapStillMerge() throws Exception {
        // The gap tolerance is inclusive: candidates separated by exactly 64 KB still merge —
        // any real gap is padding, and the boundary is precisely where a fencepost would
        // silently split regions that should be one GET.
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        long near = 1000;
        long far = near + 32 + 64 * 1024; // exactly one gap beyond the near candidate's end
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, near, 32),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, far, 32))),
                file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom region"))
                .as("a gap of exactly BLOOM_COALESCE_GAP_BYTES is bridged")
                .isEqualTo(1);
        assertThat(prefetch.lookup(near)).isNotNull();
        assertThat(prefetch.lookup(far)).isNotNull();
    }

    @Test
    void regionsSpanningExactlyTheSizeCapStillMerge() throws Exception {
        // The 16 MB cap is inclusive too: a span of exactly BLOOM_MAX_REGION_BYTES stays one
        // region. Only the in-memory file's geometry matters, so it stands in for a much
        // larger on-disk one.
        int cap = 16 * 1024 * 1024;
        long near = 1000;
        int nearLength = cap - 32;
        long far = near + nearLength; // adjacent: zero gap, combined span exactly the cap
        byte[] fileBytes = new byte[Math.toIntExact(far + 32)];
        InputFile input = InputFile.of(ByteBuffer.wrap(fileBytes));
        input.open();
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        CountingInputFile file = new CountingInputFile(input);
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, near, nearLength),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, far, 32))),
                file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom region"))
                .as("a span of exactly BLOOM_MAX_REGION_BYTES is not split")
                .isEqualTo(1);
        assertThat(prefetch.lookup(near)).isNotNull();
        assertThat(prefetch.lookup(far)).isNotNull();
    }

    @Test
    void unmergeableKnownCandidatesAreNotPrefetched() throws Exception {
        // The skip rule for footer-declared candidates: when no merged region can hold two
        // candidates there is nothing to gain, so no prefetch runs at all and every filter is
        // read lazily — exactly today's path.
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        BloomFilterReadPlanner.BloomFilterReadPlan plan = new BloomFilterReadPlanner.BloomFilterReadPlan(
                List.of(new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 70000, 32)));
        assertThat(BloomFilterPrefetcher.fetch(plan, file)).isNull();
        assertThat(file.readCount("bloom region")).isZero();
        assertThat(file.readCount("bloom probe")).isZero();
    }

    @Test
    void regionsSplitAtTheSizeCap() throws Exception {
        // Three 6 MB candidates whose combined span exceeds the 16 MB cap: the greedy walk
        // must split rather than issue one unbounded GET. Only region geometry matters here,
        // so an in-memory file stands in for a much larger on-disk one.
        int filterBytes = 6 * 1024 * 1024;
        long first = 1000;
        long second = first + filterBytes + 1024;
        long third = second + filterBytes + 1024;
        byte[] fileBytes = new byte[Math.toIntExact(third + filterBytes + 1024)];
        InputFile input = InputFile.of(ByteBuffer.wrap(fileBytes));
        input.open();
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        CountingInputFile file = new CountingInputFile(input);
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                                first, filterBytes),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                                second, filterBytes),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                                third, filterBytes))),
                file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom region"))
                .as("the first two candidates merge; the cap splits the third into its own region")
                .isEqualTo(2);
        assertThat(prefetch.lookup(first)).isNotNull();
        assertThat(prefetch.lookup(second)).isNotNull();
        assertThat(prefetch.lookup(third)).isNotNull();
    }

    @Test
    void legacyCandidateMergesWithLengthKnownNeighbour() throws Exception {
        // `code`'s filters for row groups 0 and 1 sit back-to-back. Declaring the first legacy
        // (no length) and the second footer-declared must still coalesce into one region once
        // the probe derives the first's length — the merged phase serves both kinds together.
        List<RowGroup> rowGroups = fixtureRowGroups();
        ColumnMetaData first = rowGroups.get(0).columns().get(CODE_COLUMN).metaData();
        ColumnMetaData second = rowGroups.get(1).columns().get(CODE_COLUMN).metaData();
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroups.get(0), CODE_COLUMN,
                                first.bloomFilterOffset(), null),
                        new BloomFilterReadPlanner.BloomCandidate(1, rowGroups.get(1), CODE_COLUMN,
                                second.bloomFilterOffset(), second.bloomFilterLength()))),
                file);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(1);
        assertThat(file.readCount("bloom region")).isEqualTo(1);
        assertThat(file.readCount("bloom filter")).isZero();
        assertThat(prefetch.lookup(first.bloomFilterOffset())).isNotNull();
        assertThat(prefetch.lookup(second.bloomFilterOffset())).isNotNull();
    }

    @Test
    void prefetchSwitchSelectsMergedRegionsOverLazyReads() throws Exception {
        // The property is the parity test's reference switch; these assertions keep an accidental
        // prefetch-on versus prefetch-on comparison from passing.
        Reads prefetched = read(FilterPredicate.eq("code", 1), true);
        assertThat(prefetched.bloomReads()).isGreaterThan(0);

        Reads reference = read(FilterPredicate.eq("code", 1), false);
        assertThat(reference.regionReads()).isZero();
        assertThat(reference.lazyReads()).isEqualTo(5);
    }

    @Test
    void pruningDecisionsMatchWithAndWithoutPrefetch() throws Exception {
        List<FilterPredicate> predicates = List.of(
                FilterPredicate.eq("code", 1),          // absent, in range: bloom drops every group
                FilterPredicate.eq("code", 3),          // present: one row per row group
                FilterPredicate.in("code", 1, 2),       // all absent: dropped
                FilterPredicate.in("code", 1, 3),       // one present: kept
                FilterPredicate.eq("id", 50L),          // statistics keep one row group only
                FilterPredicate.eq("name", "zzz"),      // outside statistics range: no bloom consult
                FilterPredicate.and(FilterPredicate.eq("code", 1), FilterPredicate.eq("value", 15L)),
                FilterPredicate.or(FilterPredicate.eq("code", 1), FilterPredicate.eq("id", 50L)));
        for (FilterPredicate predicate : predicates) {
            Reads prefetched = read(predicate, true);
            Reads reference = read(predicate, false);
            assertThat(prefetched.rows())
                    .as("rows with prefetch on vs off for %s", predicate)
                    .isEqualTo(reference.rows());
        }
        // Spot-check the rows themselves: `code` cycles 0, 3, 6, 9, so code = 3 is one quarter of
        // the rows — 50 in each row group — and `id` = 50 matches exactly one row.
        assertThat(read(FilterPredicate.eq("code", 3), true).rows()).isEqualTo(250);
        assertThat(read(FilterPredicate.eq("id", 50L), true).rows()).isEqualTo(1);
    }

    @Test
    void noBloomIoWithoutABloomEligiblePredicate() throws Exception {
        Reads unfiltered = read(null, true);
        assertThat(unfiltered.bloomReads())
                .as("a read with no filter never touches bloom filters")
                .isZero();

        // `code` < 5 keeps every row group (statistics cannot drop it) but the operator is not
        // EQ / IN, so no bloom filter is consulted. Rows: code values 0 and 3 — half of them.
        Reads rangeOnly = read(FilterPredicate.lt("code", 5), true);
        assertThat(rangeOnly.bloomReads()).isZero();
        assertThat(rangeOnly.rows()).isEqualTo(500);
    }

    @Test
    void metadataFilteringDisabledIssuesNoBloomReads() throws Exception {
        // The #797 opt-out disables every metadata-driven prune; the predicate is evaluated per
        // row, and no bloom filter is fetched — even with prefetch enabled.
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.openAll(List.of(file), context,
                     ReaderConfig.builder().option("hardwood.metadata-filtering", "false").build())) {
            String previous = System.getProperty("hardwood.internal.bloomPrefetch");
            try {
                System.setProperty("hardwood.internal.bloomPrefetch", "true");
                int rows = 0;
                try (ColumnReader values = reader.buildColumnReader("code")
                        .filter(FilterPredicate.eq("code", 1)).build()) {
                    while (values.nextBatch()) {
                        rows += values.getRecordCount();
                    }
                }
                assertThat(rows).isZero(); // per-row evaluation: no row holds code = 1
                assertThat(file.readCount("bloom")).isZero();
            } finally {
                if (previous == null) {
                    System.clearProperty("hardwood.internal.bloomPrefetch");
                } else {
                    System.setProperty("hardwood.internal.bloomPrefetch", previous);
                }
            }
        }
    }

    @Test
    void singleCandidateCostsExactlyOneLazyRead() throws Exception {
        // id's statistics keep only row group 0 for id = 50 (the column is strictly increasing, so
        // the row groups' ranges are disjoint), so the plan has a single candidate: prefetch is
        // skipped (a lone candidate has no neighbour to merge with) and the lazy path issues
        // exactly one read.
        Reads reads = read(FilterPredicate.eq("id", 50L), true);
        assertThat(reads.regionReads()).isZero();
        assertThat(reads.lazyReads()).isEqualTo(1);
        assertThat(reads.rows()).isEqualTo(1);
    }

    @Test
    void failedRegionFetchFallsBackToLazyReads() throws Exception {
        // A prefetch failure must not change the outcome: the region fetch (the one large bloom
        // read) fails, is swallowed, and every filter is read lazily — small per-filter ranges the
        // failing wrapper still serves.
        InputFile input = new FailingBloomReads(InputFile.of(FIXTURE), 3000);
        Reads reads = read(FilterPredicate.eq("code", 1), true, input);
        assertThat(reads.regionReads()).isEqualTo(1);
        assertThat(reads.lazyReads()).isEqualTo(5);
        assertThat(reads.rows()).isZero();
    }

    @Test
    void permanentlyFailingBloomReadsSurfaceTheLazyPathException() throws Exception {
        // When the lazy path fails too, the error matches the prefetch-disabled run exactly —
        // prefetching must never turn an I/O failure into a different outcome or message.
        InputFile input = new FailingBloomReads(InputFile.of(FIXTURE), 0);
        assertThatThrownBy(() -> read(FilterPredicate.eq("code", 1), true, input))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read bloom filter");
        assertThatThrownBy(() -> read(FilterPredicate.eq("code", 1), false, input))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read bloom filter");
    }

    @Test
    void unconsultedPastEofCandidateDoesNotFailTheRead() throws Exception {
        // A filter the real evaluation never consults — the AND's first leaf (code = 1, absent
        // from every bloom) proves CANNOT_MATCH, so id is never read — but the planner plans it
        // (the allowed over-approximation), and id's chunk declares a region past EOF. The
        // prefetch read fails with an unchecked bounds error; it must be contained so the read
        // succeeds with today's decisions instead of dying on a CompletionException.
        long pastEof;
        try (InputFile input = InputFile.of(FIXTURE)) {
            input.open();
            pastEof = input.length() + 1000;
        }
        List<RowGroup> rowGroups = new ArrayList<>();
        for (RowGroup rowGroup : fixtureRowGroups()) {
            ColumnChunk id = rowGroup.columns().getFirst();
            ColumnChunk patched = new ColumnChunk(withBloom(id.metaData(), pastEof, 32),
                    id.offsetIndexOffset(), id.offsetIndexLength(),
                    id.columnIndexOffset(), id.columnIndexLength(), "");
            List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
            columns.set(0, patched);
            rowGroups.add(new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()));
        }
        ResolvedPredicate predicate = FilterPredicateResolver.resolve(
                FilterPredicate.and(FilterPredicate.eq("code", 1), FilterPredicate.eq("id", 50L)),
                fixtureSchema());
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, predicate, rowGroups);

        List<FilterDecision> withPrefetch = new ArrayList<>();
        List<FilterDecision> withoutPrefetch = new ArrayList<>();
        for (RowGroup rowGroup : rowGroups) {
            withPrefetch.add(RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup,
                    new RowGroupBloomFilterSource(file, rowGroup, prefetch), null));
            withoutPrefetch.add(RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup,
                    new RowGroupBloomFilterSource(file, rowGroup), null));
        }
        assertThat(withPrefetch)
                .as("a failed prefetch read must not change any decision")
                .isEqualTo(withoutPrefetch);
    }

    @Test
    void chunkNamingAnotherFileFailsIdenticallyWithAndWithoutPrefetch() throws Exception {
        // The split-file check lives in the lazy source and must fire whether or not a
        // prefetch was supplied: the chunk's offset addresses another file, so pruning on
        // whatever sits there would drop row groups that match.
        List<RowGroup> rowGroups = fixtureRowGroups();
        RowGroup rowGroup = rowGroups.getFirst();
        ColumnChunk code = rowGroup.columns().get(CODE_COLUMN);
        ColumnChunk patched = new ColumnChunk(
                withBloom(code.metaData(), 1000L, 32),
                code.offsetIndexOffset(), code.offsetIndexLength(),
                code.columnIndexOffset(), code.columnIndexLength(), "other.parquet");
        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
        columns.set(CODE_COLUMN, patched);
        RowGroup split = new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());

        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        assertThatThrownBy(() -> new RowGroupBloomFilterSource(file, split).forColumn(CODE_COLUMN))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Cannot read column " + CODE_COLUMN);
        BloomFilterPrefetch prefetch = offset ->
                new BloomFilterPrefetch.PrefetchedBloom(ByteBuffer.allocate(32), 32);
        assertThatThrownBy(() -> new RowGroupBloomFilterSource(file, split, prefetch)
                .forColumn(CODE_COLUMN))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Cannot read column " + CODE_COLUMN);
    }

    @Test
    void consultedPastEofCandidateSurfacesTheLazyPathException() throws Exception {
        // The consult-reachable counterpart: row group 0's code chunk declares a region past
        // EOF while its siblings are honest, so the merged region read fails. The candidate
        // must stay unprefetched and its lazy read throw exactly what the prefetch-disabled
        // run throws — not a CompletionException out of the prefetch.
        long pastEof;
        try (InputFile input = InputFile.of(FIXTURE)) {
            input.open();
            pastEof = input.length() + 1000;
        }
        List<RowGroup> rowGroups = codeBloomAt(pastEof, 32);
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedCodePredicate(), rowGroups);

        RowGroup first = rowGroups.getFirst();
        Throwable lazy = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first).forColumn(CODE_COLUMN));
        Throwable prefetched = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first, prefetch).forColumn(CODE_COLUMN));
        assertThat(prefetched)
                .as("the corrupt candidate fails through the lazy path, identically with prefetch disabled")
                .isInstanceOf(lazy.getClass())
                .hasMessage(lazy.getMessage());
    }

    @Test
    void zeroLengthCandidateFailsThroughTheLazyPath() throws Exception {
        // A zero bloom_filter_length is footer-representable and cannot hold a filter. The
        // planner leaves it unprefetched so the lazy path fails on it with its usual
        // UncheckedIOException — the same exception as the prefetch-disabled run.
        long codeOffset = fixtureRowGroups().getFirst().columns().get(CODE_COLUMN)
                .metaData().bloomFilterOffset();
        List<RowGroup> rowGroups = codeBloomAt(codeOffset, 0);
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedCodePredicate(), rowGroups);

        RowGroup first = rowGroups.getFirst();
        assertThatThrownBy(() -> new RowGroupBloomFilterSource(file, first, prefetch)
                .forColumn(CODE_COLUMN))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read bloom filter");
        assertThatThrownBy(() -> new RowGroupBloomFilterSource(file, first).forColumn(CODE_COLUMN))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read bloom filter");
    }

    @Test
    void shortRegionBufferNamesTheRegion() throws Exception {
        // An InputFile returning fewer bytes than requested must not surface as a context-free
        // IndexOutOfBoundsException from ByteBuffer.slice: publication validates coverage and
        // names the region and candidate, mirroring SharedRegion.slice.
        CountingInputFile file = new CountingInputFile(new ShortBloomRegions(InputFile.of(FIXTURE)));
        file.open();
        assertThatThrownBy(() -> prefetch(file, resolvedCodePredicate(), fixtureRowGroups()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bloom region")
                .hasMessageContaining("does not cover the filter at offset");
    }

    @Test
    void shortRegionMessageNamesTheExactSpan() throws Exception {
        // The diagnostic carries the region's actual span — the short read's byte count, not
        // the requested one — so the numbers in the message are pinned too.
        CountingInputFile file = new CountingInputFile(new ShortBloomRegions(InputFile.of(FIXTURE)));
        file.open();
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        assertThatThrownBy(() -> BloomFilterPrefetcher.fetch(
                new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32),
                        new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1100, 32))),
                file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[1000, 1131)"); // region [1000, 1132), served one byte short
    }

    /// Delegates every read to `delegate` except bloom-attributed ones of at least `minBytes`,
    /// which fail. With `minBytes` = 0 every bloom read fails; with the per-filter sizes well
    /// below it and the merged region above it, only the region fetch fails.
    private record FailingBloomReads(InputFile delegate, int minBytes) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (length >= minBytes && FetchReason.current().startsWith("bloom")) {
                throw new IOException("Forced bloom read failure at offset " + offset);
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Delegates every read but serves `bloom region` reads one byte short — an InputFile
    /// violating its read contract, exercising the publication step's coverage validation.
    private record ShortBloomRegions(InputFile delegate) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (FetchReason.current().startsWith("bloom region")) {
                return delegate.readRange(offset, Math.max(0, length - 1));
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Delegates every read except the prefetch probe of the filter at `offset`, which fails —
    /// only the `bloom probe` scope, so the lazy path's own probe of the same bytes succeeds.
    private record FailingProbeAt(InputFile delegate, long offset) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (offset == this.offset && FetchReason.current().startsWith("bloom probe")) {
                throw new IOException("Forced probe failure at offset " + offset);
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Delegates every read; `bloom region` reads announce themselves, and the first one blocks
    /// until a second is in flight, proving the region fetches overlap. A serialized
    /// prefetcher waits the timeout out and records no overlap.
    private static final class OverlappingRegions implements InputFile {

        private final InputFile delegate;
        private final AtomicInteger regionReadsInFlight = new AtomicInteger();
        private final CountDownLatch secondInFlight = new CountDownLatch(1);
        private final AtomicBoolean overlapped = new AtomicBoolean();

        private OverlappingRegions(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            boolean regionRead = FetchReason.current().startsWith("bloom region");
            if (regionRead) {
                if (regionReadsInFlight.incrementAndGet() > 1) {
                    secondInFlight.countDown();
                } else {
                    try {
                        overlapped.set(secondInFlight.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted awaiting a concurrent region read", e);
                    }
                }
            }
            try {
                return delegate.readRange(offset, length);
            } finally {
                if (regionRead) {
                    regionReadsInFlight.decrementAndGet();
                }
            }
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Counts `length()` calls while delegating everything else — the legacy probe phase must
    /// resolve the file length once per file, not once per candidate.
    private static final class LengthCountingFile implements InputFile {

        private final InputFile delegate;
        private final AtomicInteger lengthCalls = new AtomicInteger();

        private LengthCountingFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            lengthCalls.incrementAndGet();
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    // ==================== Legacy (length-absent) candidates ====================

    /// The fixture's row groups, unmodified.
    private static List<RowGroup> fixtureRowGroups() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return new ArrayList<>(reader.getFileMetaData().rowGroups());
        }
    }

    /// A copy of the fixture's row groups with row group 0's `code` bloom geometry replaced.
    private static List<RowGroup> codeBloomAt(Long offset, Integer length) throws Exception {
        List<RowGroup> rowGroups = fixtureRowGroups();
        RowGroup rowGroup = rowGroups.getFirst();
        ColumnChunk code = rowGroup.columns().get(CODE_COLUMN);
        ColumnChunk patched = new ColumnChunk(withBloom(code.metaData(), offset, length),
                code.offsetIndexOffset(), code.offsetIndexLength(),
                code.columnIndexOffset(), code.columnIndexLength(), "");
        List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
        columns.set(CODE_COLUMN, patched);
        rowGroups.set(0, new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()));
        return rowGroups;
    }

    /// The fixture's row groups with the `code` chunk's `bloom_filter_length` blanked, as legacy
    /// writers omit it, keeping the (contiguous) offsets. pyarrow always writes the length, so
    /// these tests run at the planner/source level over patched metadata.
    private static List<RowGroup> legacyLengthRowGroups() throws Exception {
        List<RowGroup> legacy = new ArrayList<>();
        try (InputFile input = InputFile.of(FIXTURE);
             ParquetFileReader reader = ParquetFileReader.open(input)) {
            for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
                ColumnChunk original = rowGroup.columns().get(CODE_COLUMN);
                ColumnChunk patched = new ColumnChunk(withoutLength(original.metaData()),
                        original.offsetIndexOffset(), original.offsetIndexLength(),
                        original.columnIndexOffset(), original.columnIndexLength(), "");
                List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
                columns.set(CODE_COLUMN, patched);
                legacy.add(new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()));
            }
        }
        return legacy;
    }

    private static ColumnMetaData withoutLength(ColumnMetaData md) {
        return new ColumnMetaData(
                md.type(), md.encodings(), md.pathInSchema(), md.codec(),
                md.numValues(), md.totalUncompressedSize(), md.totalCompressedSize(),
                md.keyValueMetadata(), md.dataPageOffset(), md.dictionaryPageOffset(),
                md.statistics(), md.geospatialStatistics(), md.bloomFilterOffset(), null,
                md.encodingStats(), md.sizeStatistics());
    }

    private static ColumnMetaData withBloom(ColumnMetaData md, Long offset, Integer length) {
        return new ColumnMetaData(
                md.type(), md.encodings(), md.pathInSchema(), md.codec(),
                md.numValues(), md.totalUncompressedSize(), md.totalCompressedSize(),
                md.keyValueMetadata(), md.dataPageOffset(), md.dictionaryPageOffset(),
                md.statistics(), md.geospatialStatistics(), offset, length,
                md.encodingStats(), md.sizeStatistics());
    }

    /// Plans and prefetches over the given row groups with the given counting file, returning
    /// the lookup (null when prefetch declined).
    private static BloomFilterPrefetch prefetch(CountingInputFile file, ResolvedPredicate predicate,
                                                List<RowGroup> rowGroups) {
        BloomFilterReadPlanner.BloomFilterReadPlan plan =
                BloomFilterReadPlanner.plan(predicate, rowGroups);
        return BloomFilterPrefetcher.fetch(plan, file);
    }

    @Test
    void legacyCandidatesProbeInParallelThenFetchMerged() throws Exception {
        // Five legacy filters, contiguous like the footer-declared ones: one concurrent probe
        // phase (five tiny header reads), then one merged region fetch — at most two sequential
        // phases regardless of row-group count, where the lazy path pays two reads per filter.
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        ResolvedPredicate predicate = resolvedCodePredicate();
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, predicate, rowGroups);

        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(5);
        assertThat(file.readCount("bloom region")).isEqualTo(1);
        assertThat(file.readCount("bloom filter")).isZero();

        // The prefetched legacy filter decides identically to the same filter read through the
        // footer-declared (known-length) path.
        RowGroup first = rowGroups.getFirst();
        BloomFilter legacyFilter = new RowGroupBloomFilterSource(file, first, prefetch).forColumn(CODE_COLUMN);
        BloomFilter knownFilter = new RowGroupBloomFilterSource(file, first).forColumn(CODE_COLUMN);
        assertThat(legacyFilter).isNotNull();
        assertThat(knownFilter).isNotNull();
        assertThat(legacyFilter.mightContain(XxHash64.hash(3))).isEqualTo(
                knownFilter.mightContain(XxHash64.hash(3))).isTrue();
        assertThat(legacyFilter.mightContain(XxHash64.hash(1))).isEqualTo(
                knownFilter.mightContain(XxHash64.hash(1))).isFalse();
    }

    @Test
    void windowFittingLegacyCandidatesCostOneProbeEach() throws Exception {
        // Filters small enough to fit the 64-byte probe window need no second phase: two
        // candidates cost two probes, zero region fetches, and one length() resolution.
        long presentHash = XxHash64.hash(42);
        byte[] filter = serializeMinimalFilter(presentHash);
        assertThat(filter.length).isLessThan(64);

        int offsetA = 4;
        int offsetB = offsetA + filter.length + 16;
        byte[] fileBytes = new byte[offsetB + filter.length + 32];
        System.arraycopy(filter, 0, fileBytes, offsetA, filter.length);
        System.arraycopy(filter, 0, fileBytes, offsetB, filter.length);

        LengthCountingFile input = new LengthCountingFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        input.open();
        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(offsetA, offsetB);
        CountingInputFile file = new CountingInputFile(input);
        BloomFilterPrefetch prefetch = prefetch(file, resolvedIdPredicate(), rowGroups);

        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(2);
        assertThat(file.readCount("bloom region")).isZero();
        assertThat(input.lengthCalls.get())
                .as("the file length is resolved once for all legacy probes")
                .isEqualTo(1);

        // Both candidates were prefetched complete; the stored value must report present.
        for (RowGroup rowGroup : rowGroups) {
            BloomFilter filter0 = new RowGroupBloomFilterSource(file, rowGroup, prefetch).forColumn(0);
            assertThat(filter0).isNotNull();
            assertThat(filter0.mightContain(presentHash)).isTrue();
        }
    }

    @Test
    void isolatedLegacyCandidatesUseTwoPhasesWithoutExtraReads() throws Exception {
        // Unknown-length filters are probed so their true extents can be coalesced. When they
        // remain isolated, the probe and exact reads still total the lazy path's two reads per
        // filter, but both phases run concurrently.
        long presentHash = XxHash64.hash(7);
        byte[] filter = serializeMinimalFilter(presentHash, 1024);
        assertThat(filter.length).isGreaterThan(BloomFilterProbe.HEADER_PROBE_BYTES);

        int offsetA = 64;
        int offsetB = offsetA + 64 * 1024 + 8 * 1024;
        byte[] fileBytes = new byte[offsetB + filter.length + 32];
        System.arraycopy(filter, 0, fileBytes, offsetA, filter.length);
        System.arraycopy(filter, 0, fileBytes, offsetB, filter.length);

        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(offsetA, offsetB);
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedIdPredicate(), rowGroups);

        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(2);
        assertThat(file.readCount("bloom region")).isEqualTo(2);

        for (RowGroup rowGroup : rowGroups) {
            BloomFilter lazyFilter = new RowGroupBloomFilterSource(file, rowGroup, prefetch)
                    .forColumn(0);
            assertThat(lazyFilter).isNotNull();
            assertThat(lazyFilter.mightContain(presentHash)).isTrue();
        }
        assertThat(file.readCount("bloom filter")).isZero();
    }

    @Test
    void failingLengthLeavesLegacyCandidatesUnprefetched() throws Exception {
        // An unreadable file length means no probe geometry; every legacy candidate stays on the
        // lazy path, which fails with the same exception as the prefetch-disabled run.
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        ResolvedPredicate predicate = resolvedCodePredicate();
        InputFile failingLength = new FailingLengthFile(InputFile.of(FIXTURE));
        assertThat(prefetch(new CountingInputFile(failingLength), predicate, rowGroups)).isNull();

        assertThatThrownBy(() -> new RowGroupBloomFilterSource(failingLength, rowGroups.getFirst())
                .forColumn(CODE_COLUMN))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read bloom filter");
    }

    @Test
    void uncheckedLengthFailureLeavesLegacyCandidatesUnprefetched() throws Exception {
        // `length()` failing with anything — not just an IOException — must leave every legacy
        // candidate on the lazy path rather than escaping the prefetch, and resurface from the
        // lazy read with the same type and message.
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        InputFile failingLength = new UncheckedFailingLengthFile(InputFile.of(FIXTURE));
        assertThat(prefetch(new CountingInputFile(failingLength), resolvedCodePredicate(),
                rowGroups)).isNull();

        assertThatThrownBy(() -> new RowGroupBloomFilterSource(failingLength, rowGroups.getFirst())
                .forColumn(CODE_COLUMN))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Forced unchecked length failure");
    }

    @Test
    void offsetPastEofStaysUnprefetched() throws Exception {
        // Candidates at or past EOF are excluded from prefetch; the lazy path keeps its
        // historical behavior for the corrupt offset — the same exception as prefetch disabled.
        List<RowGroup> rowGroups = new ArrayList<>();
        try (InputFile input = InputFile.of(FIXTURE);
             ParquetFileReader reader = ParquetFileReader.open(input)) {
            long pastEof = input.length() + 1000;
            for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
                ColumnChunk original = rowGroup.columns().get(CODE_COLUMN);
                ColumnChunk patched = new ColumnChunk(
                        withBloom(original.metaData(), pastEof, null),
                        original.offsetIndexOffset(), original.offsetIndexLength(),
                        original.columnIndexOffset(), original.columnIndexLength(), "");
                List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
                columns.set(CODE_COLUMN, patched);
                rowGroups.add(new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()));
            }
        }
        ResolvedPredicate predicate = resolvedCodePredicate();
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        assertThat(prefetch(file, predicate, rowGroups)).isNull();
    }

    @Test
    void candidateExactlyAtEofIsGuardedBeforeAnyRead() throws Exception {
        // A legacy candidate at exactly the file length is unprefetched by the EOF guard
        // before any read is issued; an off-by-one there would fetch a zero-byte window.
        long fileLength;
        try (InputFile input = InputFile.of(FIXTURE)) {
            input.open();
            fileLength = input.length();
        }
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        RowGroup first = rowGroups.getFirst();
        ColumnChunk original = first.columns().get(CODE_COLUMN);
        ColumnChunk patched = new ColumnChunk(
                withBloom(original.metaData(), fileLength, null),
                original.offsetIndexOffset(), original.offsetIndexLength(),
                original.columnIndexOffset(), original.columnIndexLength(), "");
        List<ColumnChunk> columns = new ArrayList<>(first.columns());
        columns.set(CODE_COLUMN, patched);
        rowGroups.set(0, new RowGroup(columns, first.totalByteSize(), first.numRows()));

        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedCodePredicate(), rowGroups);
        assertThat(file.readCount("bloom probe")).isEqualTo(4);
        // The four honest probes parsed the fixture's real filters and were fetched as one
        // merged region (they are contiguous); the EOF-guarded candidate contributed no
        // probe, no region, and no cache entry.
        assertThat(file.readCount("bloom region")).isEqualTo(1);
        assertThat(prefetch).isNotNull();
        assertThat(prefetch.lookup(fileLength)).isNull();
    }

    @Test
    void legacyCandidatesProbeBeforeTheirExtentsAreKnown() throws Exception {
        // Both legacy candidates are probed before their true extents are known. This keeps
        // extent-bridging layouts eligible for coalescing instead of treating legacy filters as
        // zero-length points.
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        BloomFilterReadPlanner.BloomCandidate known =
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32);

        CountingInputFile atBoundary = new CountingInputFile(InputFile.of(FIXTURE));
        atBoundary.open();
        BloomFilterPrefetcher.fetch(new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(known,
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                        1000 + 32 + 64 * 1024, null))), atBoundary);
        assertThat(atBoundary.readCount("bloom probe")).isEqualTo(1);

        CountingInputFile pastBoundary = new CountingInputFile(InputFile.of(FIXTURE));
        pastBoundary.open();
        BloomFilterPrefetcher.fetch(new BloomFilterReadPlanner.BloomFilterReadPlan(List.of(known,
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN,
                        1000 + 32 + 64 * 1024 + 1, null))), pastBoundary);
        assertThat(pastBoundary.readCount("bloom probe"))
                .as("legacy candidates are probed before their extents are known")
                .isEqualTo(1);
    }

    @Test
    void windowEndingExactlyAtEofServesTheWholeFilter() throws Exception {
        // Two boundary rules in one shape: the probe window clamps to the bytes actually left
        // in the file, and a filter exactly as long as the window is complete after the probe
        // — no second read, no region fetch. A fencepost in either rule would re-fetch (or
        // read past EOF and fall back).
        long presentHash = XxHash64.hash(11);
        byte[] filter = serializeMinimalFilter(presentHash);
        int offsetA = 64;
        int offsetB = 4096;
        int fileLength = offsetB + filter.length; // the second filter ends exactly at EOF
        byte[] fileBytes = new byte[fileLength];
        System.arraycopy(filter, 0, fileBytes, offsetA, filter.length);
        System.arraycopy(filter, 0, fileBytes, offsetB, filter.length);

        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        file.open();
        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(offsetA, offsetB);
        BloomFilterPrefetch prefetch = prefetch(file, resolvedIdPredicate(), rowGroups);

        RowGroup second = rowGroups.get(1); // the one whose window was clamped to EOF
        BloomFilter served = new RowGroupBloomFilterSource(file, second, prefetch).forColumn(0);
        assertThat(served).isNotNull();
        assertThat(served.mightContain(presentHash)).isTrue();

        // Asserted after serving: a fencepost in either boundary rule would re-fetch the
        // second filter lazily, and those reads would only be visible now.
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(2);
        assertThat(file.readCount("bloom region")).isZero();
        assertThat(file.readCount("bloom filter"))
                .as("the clamped window served both filters; nothing fell back to a lazy read")
                .isZero();
    }

    @Test
    void oversizedLegacyProbeIsRetainedAndServesTheExactLazyRead() throws Exception {
        // A legacy filter whose probe-derived length exceeds the region cap cannot be
        // prefetched as bytes; its probe is retained so the source's lazy path can skip its
        // own probe and read the exact region in one call. This drives that retained-probe
        // wiring end to end: same filter as the probe-plus-refetch lazy path, at one read
        // instead of two. 32 MB is above the 16 MB cap yet a valid power-of-two bitset, so
        // the exact read parses a real (all-zero) filter.
        int oversizedBitset = 32 * 1024 * 1024;
        ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
        Util.writeBloomFilterHeader(new BloomFilterHeader(oversizedBitset,
                BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm()),
                BloomFilterHash.XXHASH(new XxHash()),
                BloomFilterCompression.UNCOMPRESSED(new Uncompressed())), headerBytes);
        byte[] header = headerBytes.toByteArray();
        int totalLength = header.length + oversizedBitset;

        long presentHash = XxHash64.hash(7);
        byte[] windowFilter = serializeMinimalFilter(presentHash);
        int offsetA = 64;
        int offsetB = offsetA + totalLength + 64;
        byte[] fileBytes = new byte[offsetB + windowFilter.length + 32];
        System.arraycopy(header, 0, fileBytes, offsetA, header.length);
        System.arraycopy(windowFilter, 0, fileBytes, offsetB, windowFilter.length);

        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(offsetA, offsetB);
        CountingInputFile file = new CountingInputFile(InputFile.of(ByteBuffer.wrap(fileBytes)));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedIdPredicate(), rowGroups);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom probe")).isEqualTo(2);
        assertThat(file.readCount("bloom region")).isZero();

        // The oversized candidate is retained as a derived length, not prefetched bytes; the
        // window-fitting sibling is prefetched complete.
        assertThat(prefetch.lookup(offsetA)).isNull();
        assertThat(prefetch.lookupProbe(offsetA))
                .as("the oversized legacy probe is retained with its derived exact length")
                .extracting(BloomFilterPrefetch.PrefetchedProbe::filterLength)
                .isEqualTo(totalLength);
        assertThat(prefetch.lookup(offsetB)).isNotNull();

        // The retained probe serves the source's lazy path: one exact read, no re-probe, and
        // the filter parses (a zero bitset contains nothing).
        BloomFilter served = new RowGroupBloomFilterSource(file, rowGroups.getFirst(), prefetch)
                .forColumn(0);
        assertThat(served).isNotNull();
        assertThat(served.mightContain(presentHash)).isFalse();
        assertThat(file.readCount("bloom filter"))
                .as("the retained probe length drove one exact lazy read")
                .isEqualTo(1);

        // The lazy path without prefetch reads the same filter with probe plus re-fetch.
        BloomFilter lazy = new RowGroupBloomFilterSource(file, rowGroups.getFirst()).forColumn(0);
        assertThat(lazy).isNotNull();
        assertThat(lazy.mightContain(presentHash)).isFalse();
        assertThat(file.readCount("bloom filter"))
                .as("the reference lazy path pays probe plus exact re-fetch")
                .isEqualTo(3);
    }

    @Test
    void pastEofLegacyCandidateKeepsTheLazyPathException() throws Exception {
        // The lazy half of the past-EOF contract: row group 0's legacy chunk points past EOF
        // while its siblings probe and merge normally, so the prefetch is live — and the
        // corrupt candidate's lazy probe fails exactly as with prefetch disabled.
        long pastEof;
        try (InputFile input = InputFile.of(FIXTURE)) {
            input.open();
            pastEof = input.length() + 1000;
        }
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        RowGroup firstOriginal = rowGroups.getFirst();
        ColumnChunk original = firstOriginal.columns().get(CODE_COLUMN);
        ColumnChunk patched = new ColumnChunk(withBloom(original.metaData(), pastEof, null),
                original.offsetIndexOffset(), original.offsetIndexLength(),
                original.columnIndexOffset(), original.columnIndexLength(), "");
        List<ColumnChunk> columns = new ArrayList<>(firstOriginal.columns());
        columns.set(CODE_COLUMN, patched);
        rowGroups.set(0, new RowGroup(columns, firstOriginal.totalByteSize(),
                firstOriginal.numRows()));

        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        ResolvedPredicate predicate = resolvedCodePredicate();
        BloomFilterPrefetch prefetch = prefetch(file, predicate, rowGroups);
        assertThat(prefetch).isNotNull(); // the four honest siblings probed and merged

        RowGroup first = rowGroups.getFirst();
        Throwable lazy = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first).forColumn(CODE_COLUMN));
        Throwable prefetched = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first, prefetch).forColumn(CODE_COLUMN));
        assertThat(prefetched)
                .as("the corrupt candidate fails through the lazy path, identically with prefetch disabled")
                .isInstanceOf(lazy.getClass())
                .hasMessage(lazy.getMessage());
    }

    @Test
    void singleFailingProbeFallsBackToLazyForThatCandidate() throws Exception {
        // One probe failing must not sink the phase: the candidate falls back to the lazy path
        // — its probe and exact fetch run under the `bloom filter` scope, which the wrapper
        // still serves — while its siblings are prefetched and merged.
        List<RowGroup> rowGroups = legacyLengthRowGroups();
        long failingOffset = rowGroups.getFirst().columns().get(CODE_COLUMN)
                .metaData().bloomFilterOffset();
        CountingInputFile file = new CountingInputFile(
                new FailingProbeAt(InputFile.of(FIXTURE), failingOffset));
        file.open();
        BloomFilterPrefetch prefetch = prefetch(file, resolvedCodePredicate(), rowGroups);

        BloomFilter filter = new RowGroupBloomFilterSource(file, rowGroups.getFirst(), prefetch)
                .forColumn(CODE_COLUMN);
        assertThat(filter).isNotNull();
        assertThat(file.readCount("bloom probe"))
                .as("five probes attempted; the forced failure still counted as a read")
                .isEqualTo(5);
        assertThat(file.readCount("bloom region")).isEqualTo(1);
        assertThat(file.readCount("bloom filter"))
                .as("the failed candidate's lazy probe and exact fetch")
                .isEqualTo(2);
        assertThat(filter.mightContain(XxHash64.hash(3))).isTrue();
        assertThat(filter.mightContain(XxHash64.hash(1))).isFalse();
    }

    @Test
    void garbageLegacyProbeFallsBackToTheLazyPath() throws Exception {
        // The probe read succeeds but the bytes are not a filter: the unchecked parse error
        // must be contained — the candidate stays unprefetched — and the lazy path must throw
        // its own historical error, where a CompletionException out of the prefetch would
        // instead fail the read before any decision. Zero bytes make the header struct stop
        // before its required numBytes field.
        byte[] fileBytes = new byte[512];
        InputFile input = InputFile.of(ByteBuffer.wrap(fileBytes));
        input.open();
        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(64, 96);
        CountingInputFile file = new CountingInputFile(input);
        assertThat(prefetch(file, resolvedIdPredicate(), rowGroups)).isNull();
        assertThat(file.readCount("bloom probe"))
                .as("both probes were issued and failed at the parse, not skipped by the gate")
                .isEqualTo(2);

        assertThatThrownBy(() -> new RowGroupBloomFilterSource(file, rowGroups.getFirst())
                .forColumn(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid BloomFilterHeader");
    }

    @Test
    void corruptPrefetchedRegionFailsLikeTheLazyPath() throws Exception {
        // A region fetch that succeeds but holds garbage: the lookup hits, the parse fails,
        // and the unchecked parse error surfaces exactly as it does through the lazy read of
        // the same bytes — nothing about it is prefetch-shaped. Footer-declared lengths, so
        // no probe is involved.
        byte[] fileBytes = new byte[512];
        InputFile input = InputFile.of(ByteBuffer.wrap(fileBytes));
        input.open();
        List<RowGroup> rowGroups = syntheticSingleColumnRowGroups(64, 32, 128, 32);
        CountingInputFile file = new CountingInputFile(input);
        BloomFilterPrefetch prefetch = prefetch(file, resolvedIdPredicate(), rowGroups);
        assertThat(prefetch).isNotNull();
        assertThat(file.readCount("bloom region")).isEqualTo(1);

        RowGroup first = rowGroups.getFirst();
        Throwable lazy = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first).forColumn(0));
        Throwable prefetched = catchThrowable(
                () -> new RowGroupBloomFilterSource(file, first, prefetch).forColumn(0));
        assertThat(prefetched)
                .as("a corrupt prefetched filter fails exactly like the lazy read")
                .isInstanceOf(IllegalStateException.class)
                .isInstanceOf(lazy.getClass())
                .hasMessage(lazy.getMessage());
    }

    // ==================== Legacy test scaffolding ====================

    /// A `BloomFilterHeader` thrift struct followed by a minimal one-block (32-byte) bitset
    /// holding `presentHash`, serialized as a legacy writer lays it out at `bloom_filter_offset`
    /// with no declared length.
    private static byte[] serializeMinimalFilter(long presentHash) throws Exception {
        return serializeMinimalFilter(presentHash, 32);
    }

    /// The same filter with a bitset of `bitsetBytes` bytes: 32 fits the 64-byte probe window,
    /// larger values overflow it and need the exact-region second phase.
    private static byte[] serializeMinimalFilter(long presentHash, int bitsetBytes) throws Exception {
        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(bitsetBytes);
        reference.insertHash(presentHash);
        ByteArrayOutputStream bitset = new ByteArrayOutputStream();
        reference.writeTo(bitset); // bitset only — the header is written separately below

        BloomFilterHeader header = new BloomFilterHeader(bitset.size(),
                BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm()),
                BloomFilterHash.XXHASH(new XxHash()),
                BloomFilterCompression.UNCOMPRESSED(new Uncompressed()));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writeBloomFilterHeader(header, out);
        bitset.writeTo(out);
        return out.toByteArray();
    }

    /// Two single-column row groups (the fixture's `id` leaf) whose chunks point at legacy
    /// filters at the given offsets in the given file.
    private static List<RowGroup> syntheticSingleColumnRowGroups(long offsetA, long offsetB)
            throws Exception {
        return syntheticSingleColumnRowGroups(offsetA, null, offsetB, null);
    }

    /// Two single-column row groups (the fixture's `id` leaf) whose chunks point at filters
    /// with explicit footer-declared geometry at the given offsets.
    private static List<RowGroup> syntheticSingleColumnRowGroups(long offsetA, Integer lengthA,
                                                                 long offsetB, Integer lengthB)
            throws Exception {
        RowGroup templateRowGroup;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            templateRowGroup = reader.getFileMetaData().rowGroups().getFirst();
        }
        ColumnChunk template = templateRowGroup.columns().getFirst();
        List<RowGroup> rowGroups = new ArrayList<>();
        long[] offsets = {offsetA, offsetB};
        Integer[] lengths = {lengthA, lengthB};
        for (int i = 0; i < offsets.length; i++) {
            ColumnChunk chunk = new ColumnChunk(withBloom(template.metaData(), offsets[i], lengths[i]),
                    template.offsetIndexOffset(), template.offsetIndexLength(),
                    template.columnIndexOffset(), template.columnIndexLength(), "");
            rowGroups.add(new RowGroup(List.of(chunk), templateRowGroup.totalByteSize(),
                    templateRowGroup.numRows()));
        }
        return rowGroups;
    }

    /// Three synthetic candidates over the fixture's bytes: a mergeable pair at the file's
    /// start and a lone one beyond the 64 KB gap — two regions when the prefetch runs.
    private static List<BloomFilterReadPlanner.BloomCandidate> gappedCandidates() throws Exception {
        RowGroup rowGroup = fixtureRowGroups().getFirst();
        return List.of(
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1000, 32),
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 1100, 32),
                new BloomFilterReadPlanner.BloomCandidate(0, rowGroup, CODE_COLUMN, 70000, 32));
    }

    private static FileSchema fixtureSchema() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return FileSchema.fromSchemaElements(reader.getFileMetaData().schema());
        }
    }

    /// The fixture schema's `code` leaf, resolved by name, for planner-level tests.
    private static ResolvedPredicate resolvedCodePredicate() throws Exception {
        return FilterPredicateResolver.resolve(FilterPredicate.eq("code", 1), fixtureSchema());
    }

    /// The fixture schema's `id` leaf, resolved by name, for planner-level tests.
    private static ResolvedPredicate resolvedIdPredicate() throws Exception {
        return FilterPredicateResolver.resolve(FilterPredicate.eq("id", 5L), fixtureSchema());
    }

    /// Delegates everything but `length()`, which always fails — no probe geometry can be
    /// derived for legacy candidates.
    private record FailingLengthFile(InputFile delegate) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            throw new IOException("Forced length failure");
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Delegates everything but `length()`, which fails with an unchecked exception — the
    /// prefetch must swallow it and leave the legacy candidates to the lazy path.
    private record UncheckedFailingLengthFile(InputFile delegate) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() {
            throw new IllegalStateException("Forced unchecked length failure");
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
