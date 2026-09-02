/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.predicate.BloomFilterReadPlanner.BloomCandidate;
import dev.hardwood.internal.predicate.BloomFilterReadPlanner.BloomFilterReadPlan;

/// Fetches a [BloomFilterReadPlan]'s candidates ahead of the pruning pass: candidate regions
/// are sorted by offset and greedily merged, then fetched concurrently, so a column's filters
/// across row groups — typically written back-to-back — cost one or a few ranged GETs issued
/// in parallel instead of one small sequential GET per row group
/// (see `_designs/BLOOM_FILTER_IO_COALESCING.md`).
///
/// Legacy candidates (no footer-declared `bloom_filter_length`) are probed first: one small
/// header window per candidate, issued concurrently, from which the region's total length is
/// derived. A probe window that already contains the whole filter needs no further read; the
/// rest contribute exact regions to the merged fetch. At most two sequential phases serve any
/// number of legacy candidates, where the lazy path pays up to two reads per filter.
///
/// Fetching is best-effort: any probe, region read, or `length()` call that fails — an I/O
/// error or an unchecked read/parse error such as out-of-range region geometry, a malformed
/// bitset, or a corrupt header — is logged and the affected filters are left to the lazy read
/// path, which reproduces today's behavior and errors. The planner itself stays I/O-free;
/// this class is its fetch phase.
public final class BloomFilterPrefetcher {

    private static final System.Logger LOG = System.getLogger(BloomFilterPrefetcher.class.getName());

    /// Maximum byte gap between adjacent candidates bridged by one merged region. Filters are
    /// normally back-to-back; any real gap is padding, and 64 KB matches
    /// `RowGroupIterator.MAX_CROSS_COL_GAP_BYTES`.
    private static final int BLOOM_COALESCE_GAP_BYTES = 64 * 1024;

    /// Maximum size of a single merged region. Bloom filters are small; the cap bounds a
    /// pathological footer rather than shaping normal traffic.
    private static final int BLOOM_MAX_REGION_BYTES = 16 * 1024 * 1024;

    private BloomFilterPrefetcher() {
    }

    /// Fetches the plan's candidates merged and in parallel, returning a lookup over the
    /// prefetched bytes. Returns `null` when prefetching cannot pay: fewer than two candidates
    /// (a lone candidate has no neighbour to merge with and keeps today's single read), or
    /// nothing merged and no probe completed — the `anyMerged` rule of cross-column coalescing,
    /// where N contiguous candidates coalescing into one GET is precisely the case this exists
    /// for.
    public static BloomFilterPrefetch fetch(BloomFilterReadPlan plan, InputFile inputFile) {
        List<BloomCandidate> candidates = plan.candidates();
        if (candidates.size() < 2) {
            return null;
        }
        List<BloomCandidate> lengthKnown = new ArrayList<>();
        List<BloomCandidate> legacy = new ArrayList<>();
        for (BloomCandidate candidate : candidates) {
            (candidate.length() != null ? lengthKnown : legacy).add(candidate);
        }

        Map<Long, BloomFilterPrefetch.PrefetchedBloom> filters = new HashMap<>();
        List<BloomCandidate> fetchable = new ArrayList<>(lengthKnown);
        if (!legacy.isEmpty()) {
            List<BloomCandidate> probeable = probeableLegacy(legacy, lengthKnown);
            if (!probeable.isEmpty()) {
                fetchable.addAll(probeLegacy(probeable, inputFile, filters));
            }
        }

        List<Region> regions = mergeRegions(fetchable);
        boolean anyMerged = regions.stream().anyMatch(region -> region.candidates().size() > 1);
        if (anyMerged) {
            fetchRegions(regions, inputFile, filters);
        }
        return filters.isEmpty() ? null : new MergedRegionPrefetch(filters);
    }

    /// One merged byte region and the candidates it covers.
    private record Region(long offset, int length, List<BloomCandidate> candidates) {
    }

    /// One fetched merged region.
    private record PrefetchedRegion(long offset, ByteBuffer data) {
    }

    /// One legacy probe: the candidate, its probe window, and the parsed outcome.
    private record ProbeOutcome(BloomCandidate candidate, ByteBuffer window,
                                BloomFilterProbe.Result parsed) {
    }

    /// The legacy candidates worth probing: those with at least one other candidate — known or
    /// legacy — within the coalescing gap. Probing only pays when the derived region can merge
    /// with a neighbour, and a derived length can only grow a candidate's extent toward its
    /// neighbour, shrinking the gap, so a candidate within reach at its offset merges once
    /// probed — bar the region size cap. A legacy candidate with no neighbour in reach can
    /// never merge: probing it would only add a read ahead of the lazy path's identical
    /// probe-plus-refetch, so it stays unprobed.
    private static List<BloomCandidate> probeableLegacy(List<BloomCandidate> legacy,
                                                        List<BloomCandidate> lengthKnown) {
        List<BloomCandidate> all = new ArrayList<>(legacy);
        all.addAll(lengthKnown);
        List<BloomCandidate> probeable = new ArrayList<>();
        for (BloomCandidate candidate : legacy) {
            if (all.stream().anyMatch(other -> other != candidate
                    && withinCoalescingGap(candidate, other))) {
                probeable.add(candidate);
            }
        }
        return probeable;
    }

    /// Whether two candidates' regions are adjacent enough to merge: overlapping, or separated
    /// by at most the coalescing gap. A legacy candidate's length is unknown, so its region is
    /// its offset alone.
    private static boolean withinCoalescingGap(BloomCandidate a, BloomCandidate b) {
        long aEnd = a.offset() + (a.length() == null ? 0 : a.length());
        long bEnd = b.offset() + (b.length() == null ? 0 : b.length());
        long gap = Math.max(a.offset(), b.offset()) - Math.min(aEnd, bEnd);
        return gap <= BLOOM_COALESCE_GAP_BYTES;
    }

    /// Probes every legacy candidate's header concurrently, filling `filters` with the
    /// candidates whose filter fits the probe window. Returns the rest as candidates with
    /// derived lengths, ready for the merged fetch. Candidates whose probe fails — unreadable
    /// file length, offset at or past EOF, failed read — stay out of both, on the lazy path.
    private static List<BloomCandidate> probeLegacy(List<BloomCandidate> legacy, InputFile inputFile,
                                                    Map<Long, BloomFilterPrefetch.PrefetchedBloom> filters) {
        Long fileLength = fileLength(inputFile);
        if (fileLength == null) {
            return List.of();
        }
        // One pre-assigned slot per probe: tasks publish through the slot index, the
        // coordinating thread reads results after the join, and nothing is written from the
        // pool into shared collections.
        ProbeOutcome[] slots = new ProbeOutcome[legacy.size()];
        List<CompletableFuture<Void>> probes = new ArrayList<>(legacy.size());
        for (int i = 0; i < legacy.size(); i++) {
            BloomCandidate candidate = legacy.get(i);
            int slot = i;
            probes.add(CompletableFuture.runAsync(FetchReason.bind(
                    () -> slots[slot] = probe(candidate, fileLength, inputFile))));
        }
        CompletableFuture.allOf(probes.toArray(CompletableFuture[]::new)).join();

        List<BloomCandidate> oversized = new ArrayList<>();
        for (ProbeOutcome outcome : slots) {
            if (outcome == null) {
                continue;
            }
            switch (outcome.parsed()) {
                case BloomFilterProbe.Result.Complete complete -> {
                    ByteBuffer data = outcome.window().slice(0, complete.totalLength());
                    filters.put(outcome.candidate().offset(),
                            new BloomFilterPrefetch.PrefetchedBloom(data, complete.totalLength()));
                }
                // The bitset extends past the probe window: its exact region joins the merged fetch.
                case BloomFilterProbe.Result.Oversized oversizedResult ->
                    oversized.add(new BloomCandidate(outcome.candidate().rowGroupIndex(),
                            outcome.candidate().rowGroup(), outcome.candidate().columnIndex(),
                            outcome.candidate().offset(), oversizedResult.totalLength()));
            }
        }
        return oversized;
    }

    /// Reads one legacy probe window and parses it. Any failure answers `null`, leaving the
    /// candidate to the lazy path.
    private static ProbeOutcome probe(BloomCandidate candidate, long fileLength, InputFile inputFile) {
        try (FetchReason.Scope ignored = FetchReason.set("bloom probe " + candidate.offset())) {
            if (candidate.offset() >= fileLength) {
                return null;
            }
            int probe = BloomFilterProbe.probeLength(fileLength, candidate.offset());
            ByteBuffer window = inputFile.readRange(candidate.offset(), probe);
            return new ProbeOutcome(candidate, window, BloomFilterProbe.parseWindow(window));
        }
        catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG,
                    "Prefetch probe failed for bloom filter at offset {0} in {1}",
                    candidate.offset(), inputFile.name(), e);
            return null;
        }
    }

    /// The file's length, fetched once for all legacy probes. A failure is logged and answered
    /// with `null`, leaving every legacy candidate on the lazy path.
    private static Long fileLength(InputFile inputFile) {
        try {
            return inputFile.length();
        }
        catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG,
                    "Prefetch could not resolve the length of {0}", inputFile.name(), e);
            return null;
        }
    }

    /// Fetches the merged regions concurrently, filling `filters` with one entry per
    /// successfully fetched candidate. Failed regions are logged and skipped.
    private static void fetchRegions(List<Region> regions, InputFile inputFile,
                                     Map<Long, BloomFilterPrefetch.PrefetchedBloom> filters) {
        // One pre-assigned slot per region: tasks publish through the slot index, the
        // coordinating thread assembles the lookup after the join, and no map is written from
        // the common pool.
        PrefetchedRegion[] slots = new PrefetchedRegion[regions.size()];
        List<CompletableFuture<Void>> fetches = new ArrayList<>(regions.size());
        for (int i = 0; i < regions.size(); i++) {
            Region region = regions.get(i);
            int slot = i;
            fetches.add(CompletableFuture.runAsync(FetchReason.bind(
                    () -> slots[slot] = fetchRegion(region, inputFile))));
        }
        CompletableFuture.allOf(fetches.toArray(CompletableFuture[]::new)).join();

        for (int i = 0; i < regions.size(); i++) {
            PrefetchedRegion fetched = slots[i];
            if (fetched == null) {
                continue; // failed fetch; the affected filters fall back to the lazy path
            }
            for (BloomCandidate candidate : regions.get(i).candidates()) {
                int rel = Math.toIntExact(candidate.offset() - fetched.offset());
                // A region is the union of its candidates' spans, so coverage holds by
                // construction — unless the InputFile violated its read contract by returning
                // fewer bytes than requested. Fail with the region and candidate named rather
                // than a context-free IndexOutOfBoundsException from ByteBuffer.slice,
                // mirroring SharedRegion.slice.
                if (rel < 0 || rel + (long) candidate.length() > fetched.data().remaining()) {
                    throw new IllegalArgumentException("Prefetched bloom region [" + fetched.offset()
                            + ", " + (fetched.offset() + fetched.data().remaining())
                            + ") does not cover the filter at offset " + candidate.offset()
                            + " (length " + candidate.length() + ")");
                }
                ByteBuffer data = fetched.data().slice(rel, candidate.length());
                filters.put(candidate.offset(),
                        new BloomFilterPrefetch.PrefetchedBloom(data, candidate.length()));
            }
        }
    }

    /// Reads one merged region. A failure is logged and answered with `null`, answering "not
    /// prefetched" for every filter in the region; the lazy path re-issues the read and
    /// surfaces the error exactly as it does without prefetching.
    private static PrefetchedRegion fetchRegion(Region region, InputFile inputFile) {
        try (FetchReason.Scope ignored = FetchReason.set("bloom region " + region.offset() + ".."
                + (region.offset() + region.length()))) {
            return new PrefetchedRegion(region.offset(), inputFile.readRange(region.offset(), region.length()));
        }
        catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG,
                    "Prefetch failed for bloom filter region at offset {0} (length {1}) in {2}",
                    region.offset(), region.length(), inputFile.name(), e);
            return null;
        }
    }

    /// Sorts the candidates by offset and greedily merges those whose gap and combined span
    /// stay within bounds.
    private static List<Region> mergeRegions(List<BloomCandidate> candidates) {
        List<BloomCandidate> sorted = candidates.stream()
                .sorted(Comparator.comparingLong(BloomCandidate::offset))
                .toList();
        List<Region> regions = new ArrayList<>();
        long start = -1;
        long end = -1;
        List<BloomCandidate> current = new ArrayList<>();
        for (BloomCandidate candidate : sorted) {
            long candidateEnd = candidate.offset() + candidate.length();
            if (!current.isEmpty()
                    && (candidate.offset() - end > BLOOM_COALESCE_GAP_BYTES
                    || candidateEnd - start > BLOOM_MAX_REGION_BYTES)) {
                regions.add(new Region(start, Math.toIntExact(end - start), current));
                current = new ArrayList<>();
            }
            if (current.isEmpty()) {
                start = candidate.offset();
            }
            current.add(candidate);
            end = candidateEnd;
        }
        if (!current.isEmpty()) {
            regions.add(new Region(start, Math.toIntExact(end - start), current));
        }
        return regions;
    }

    /// Lookup over the merged regions' slices and probe-complete filters, built by the
    /// coordinating thread after the joins.
    private record MergedRegionPrefetch(Map<Long, BloomFilterPrefetch.PrefetchedBloom> filters)
            implements BloomFilterPrefetch {

        private MergedRegionPrefetch {
            filters = Map.copyOf(filters);
        }

        @Override
        public BloomFilterPrefetch.PrefetchedBloom lookup(long offset) {
            return filters.get(offset);
        }
    }
}
