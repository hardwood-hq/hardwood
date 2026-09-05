/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins the unit ladder of the CLI's single byte formatter. The golden
/// command-output tests only ever reach the `B` and `KiB` branches, so
/// the `MiB` / `GiB` ones — and every branch boundary — are covered here.
class SizesTest {

    private static final long KIB = 1_024L;
    private static final long MIB = KIB * 1_024;
    private static final long GIB = MIB * 1_024;

    @Test
    void rendersRawByteCountBelowOneKibibyte() {
        assertThat(Sizes.format(0)).isEqualTo("0 B");
        assertThat(Sizes.format(422)).isEqualTo("422 B");
        assertThat(Sizes.format(1_023)).isEqualTo("1023 B");
    }

    @Test
    void scalesToKibibytes() {
        assertThat(Sizes.format(KIB)).isEqualTo("1.0 KiB");
        assertThat(Sizes.format(1_536)).isEqualTo("1.5 KiB");
        assertThat(Sizes.format(80_255)).isEqualTo("78.4 KiB");
    }

    @Test
    void scalesToMebibytes() {
        assertThat(Sizes.format(MIB)).isEqualTo("1.0 MiB");
        assertThat(Sizes.format(MIB + MIB / 2)).isEqualTo("1.5 MiB");
        assertThat(Sizes.format(128 * MIB)).isEqualTo("128.0 MiB");
    }

    @Test
    void scalesToGibibytes() {
        assertThat(Sizes.format(GIB)).isEqualTo("1.0 GiB");
        assertThat(Sizes.format(GIB + GIB / 2)).isEqualTo("1.5 GiB");
        // No unit above GiB: a multi-tebibyte input keeps scaling in GiB.
        assertThat(Sizes.format(2_048 * GIB)).isEqualTo("2048.0 GiB");
    }

    /// The divisor is 1024, which is what the `KiB` / `MiB` / `GiB`
    /// labels claim — a mebibyte is 1,048,576 bytes, not 1,000,000.
    @Test
    void labelsMatchTheBinaryDivisor() {
        assertThat(Sizes.format(1_000_000)).isEqualTo("976.6 KiB");
        assertThat(Sizes.format(1_000_000_000)).isEqualTo("953.7 MiB");
    }

    /// A value that would render as `1024.0` is promoted to the next
    /// unit instead, so no output ever names a full multiple of its
    /// own divisor.
    @Test
    void promotesValuesThatWouldRoundUpToAFullUnit() {
        assertThat(Sizes.format(MIB - 1)).isEqualTo("1.0 MiB");
        assertThat(Sizes.format(GIB - 1)).isEqualTo("1.0 GiB");
        // Just below the rounding boundary the smaller unit is kept.
        assertThat(Sizes.format(MIB - KIB)).isEqualTo("1023.0 KiB");
        assertThat(Sizes.format(GIB - MIB)).isEqualTo("1023.0 MiB");
    }

    @Test
    void dualFormatAppendsTheRawCount() {
        assertThat(Sizes.dualFormat(1_536)).isEqualTo("1.5 KiB  (1,536 B)");
        assertThat(Sizes.dualFormat(55_415)).isEqualTo("54.1 KiB  (55,415 B)");
    }

    /// Below a kibibyte the human form already *is* the raw count, so
    /// the parenthesised repetition is dropped.
    @Test
    void dualFormatOmitsTheRawCountBelowOneKibibyte() {
        assertThat(Sizes.dualFormat(422)).isEqualTo("422 B");
        assertThat(Sizes.dualFormat(1_023)).isEqualTo("1023 B");
    }

    /// Compression is the share of the uncompressed size that survived, not
    /// the factor it divided by — the one form every surface renders.
    @Test
    void compressionIsThePercentageOfTheUncompressedSize() {
        assertThat(Sizes.compression(0, 10)).isEqualTo("0.0%");
        assertThat(Sizes.compression(1_000, 10_000)).isEqualTo("10.0%");
        assertThat(Sizes.compression(500, 500)).isEqualTo("100.0%");
    }

    /// An uncompressed size at or below zero is nothing to divide by. Every
    /// surface uses the same absent-value marker.
    @Test
    void compressionRendersTheSharedPlaceholderWithNothingToDivideBy() {
        assertThat(Sizes.compression(0, 0)).isEqualTo("—");
        assertThat(Sizes.compression(10, -1)).isEqualTo("—");
    }
}
