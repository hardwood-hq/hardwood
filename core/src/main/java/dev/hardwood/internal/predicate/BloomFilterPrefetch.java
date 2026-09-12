/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.ByteBuffer;

/// Bloom filter bytes fetched ahead of the pruning pass, looked up by `bloom_filter_offset`.
///
/// [RowGroupBloomFilterSource] consults this before issuing its own `readRange`; a hit parses
/// the filter from the prefetched bytes with no I/O, a miss falls through to the lazy path.
/// The lookup never changes an outcome — it only removes round trips when it can.
public interface BloomFilterPrefetch {

    /// The filter at `offset`, or `null` when nothing was prefetched for it.
    ///
    /// The returned buffer is positioned at the filter's start and holds exactly
    /// `filterLength` bytes — the filter's total region (header plus bitset), which the source
    /// parses through its known-length path without re-deriving the length.
    PrefetchedBloom lookup(long offset);

    /// A legacy header probe retained for a filter that is too large to prefetch as one region.
    /// The source uses the derived length to perform the exact lazy read without probing again.
    default PrefetchedProbe lookupProbe(long offset) {
        return null;
    }

    /// The exact filter length derived from a retained legacy probe header.
    record PrefetchedProbe(int filterLength) {
        public PrefetchedProbe {
            if (filterLength <= 0) {
                throw new IllegalArgumentException(
                        "legacy probe filter length must be positive but was " + filterLength);
            }
        }
    }

    /// One prefetched filter: bytes positioned at the filter's start and its exact total
    /// length in bytes.
    record PrefetchedBloom(ByteBuffer data, int filterLength) {

        public PrefetchedBloom {
            if (filterLength <= 0) {
                throw new IllegalArgumentException(
                        "bloom filter length must be positive but was " + filterLength);
            }
            if (data.remaining() != filterLength) {
                throw new IllegalArgumentException(
                        "prefetched bloom filter holds " + data.remaining()
                                + " bytes but its length is " + filterLength);
            }
        }
    }
}
