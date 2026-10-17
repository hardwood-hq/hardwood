/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import dev.hardwood.Validity;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The loader's arithmetic, exercised on synthetic views so that the size limit is checked
/// without a batch anywhere near it.
class BatchLoaderTest {

    private static final ColumnSchema COLUMN = new ColumnSchema(FieldPath.of("words"), PhysicalType.BYTE_ARRAY,
            RepetitionType.OPTIONAL, null, 0, 1, 0, null);

    @Test
    void viewsOfAppendedValuesAreContiguous() {
        int[] starts = { 0, 3, 3, 7 };
        int[] ends = { 3, 3, 7, 9 };
        BatchLoader.Views views = BatchLoader.views(starts, ends, Validity.NO_NULLS, 4);
        assertThat(views.totalLength()).isEqualTo(9);
        assertThat(views.contiguous()).isTrue();
        assertThat(views.firstStart()).isEqualTo(0);
    }

    @Test
    void nullsDoNotBreakContiguity() {
        // The null at index 1 has an empty range wherever the reader left it.
        int[] starts = { 5, 0, 8, 10 };
        int[] ends = { 8, 0, 10, 10 };
        Validity validity = Validity.ofNulls(new boolean[]{ false, true, false, false });
        BatchLoader.Views views = BatchLoader.views(starts, ends, validity, 4);
        assertThat(views.totalLength()).isEqualTo(5);
        assertThat(views.contiguous()).isTrue();
        assertThat(views.firstStart()).isEqualTo(5);
    }

    @Test
    void dictionaryViewsAreNotContiguous() {
        // Two values naming the same dictionary entry cover the same bytes.
        int[] starts = { 0, 0, 4 };
        int[] ends = { 4, 4, 6 };
        BatchLoader.Views views = BatchLoader.views(starts, ends, Validity.NO_NULLS, 3);
        assertThat(views.totalLength()).isEqualTo(10);
        assertThat(views.contiguous()).isFalse();
    }

    @Test
    void expandedLengthPastIntRangeIsRefused() {
        // Three views over the same 2^30-byte entry expand to more than 32-bit offsets address,
        // however small the buffer the views point into.
        int entry = 1 << 30;
        int[] starts = { 0, 0, 0 };
        int[] ends = { entry, entry, entry };
        BatchLoader.Views views = BatchLoader.views(starts, ends, Validity.NO_NULLS, 3);
        assertThat(views.totalLength()).isEqualTo(3L * entry);
        assertThatThrownBy(() -> BatchLoader.requireAddressable(views.totalLength(), COLUMN))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Column 'words' holds 3221225472 bytes of binary values in one batch, more than the "
                        + "2147483647 bytes an Arrow buffer with 32-bit offsets addresses: read with a smaller batch size");
    }

    @Test
    void expandedLengthAtIntRangeIsAccepted() {
        assertThat(BatchLoader.requireAddressable(Integer.MAX_VALUE, COLUMN)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void valueOutsideItsAnnotationIsRefused() {
        ColumnSchema tiny = new ColumnSchema(FieldPath.of("tiny"), PhysicalType.INT32, RepetitionType.OPTIONAL, null, 0,
                1, 0, LogicalType.intType(8, true));
        BatchLoader.requireInRange(tiny, 0, 127, Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertThatThrownBy(() -> BatchLoader.requireInRange(tiny, 1, 300, Byte.MIN_VALUE, Byte.MAX_VALUE))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Column 'tiny' is annotated INT_8 but holds 300 at row 1, outside [-128, 127]");
    }

    @Test
    void int96EpochIsZeroAtTheUnixEpoch() {
        assertThat(BatchLoader.epochNanos(COLUMN, 0, 2440588, 0)).isZero();
        assertThat(BatchLoader.epochNanos(COLUMN, 0, 2440587, 86_399_999_999_999L)).isEqualTo(-1L);
    }

    /// The 64-bit range's two ends fall inside a day: the first representable instant is
    /// 1677-09-21T00:12:43.145224192Z on Julian day 2333836, the last 2262-04-11T23:47:16.854775807Z.
    @Test
    void int96RangeEndsAreExact() {
        assertThat(BatchLoader.epochNanos(COLUMN, 0, 2333836, 763_145_224_192L)).isEqualTo(Long.MIN_VALUE);
        assertThat(BatchLoader.epochNanos(COLUMN, 0, 2333836, 763_145_224_193L)).isEqualTo(Long.MIN_VALUE + 1);
        LocalDateTime last = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000_000L,
                Long.MAX_VALUE % 1_000_000_000L), ZoneOffset.UTC);
        int lastDay = Math.toIntExact(last.toLocalDate().toEpochDay() + 2440588L);
        assertThat(BatchLoader.epochNanos(COLUMN, 0, lastDay, last.toLocalTime().toNanoOfDay()))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(BatchLoader.epochNanos(COLUMN, 0, lastDay, last.toLocalTime().toNanoOfDay() - 1))
                .isEqualTo(Long.MAX_VALUE - 1);
    }

    @Test
    void int96OutsideNanosecondRangeIsRefused() {
        // One nanosecond below the first representable instant, on the same day.
        assertThatThrownBy(() -> BatchLoader.epochNanos(COLUMN, 3, 2333836, 763_145_224_191L))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'words' holds an INT96 timestamp at row 3 (Julian day 2333836, 763145224191 "
                        + "nanoseconds into it) outside the range of an Arrow timestamp in nanoseconds");
        // Julian day 2451545 is 2000-01-01; day 2_000_000_000 is far past the year 2262.
        assertThatThrownBy(() -> BatchLoader.epochNanos(COLUMN, 3, 2_000_000_000, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'words' holds an INT96 timestamp at row 3 (Julian day 2000000000, 0 nanoseconds "
                        + "into it) outside the range of an Arrow timestamp in nanoseconds");
    }

    @Test
    void presentValueOnNullColumnIsRefused() {
        ColumnSchema nothing = new ColumnSchema(FieldPath.of("nothing"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.nullType());
        BatchLoader.requireAllNull(nothing, Validity.ofNulls(new boolean[]{ true, true }), 2);
        assertThatThrownBy(() -> BatchLoader.requireAllNull(nothing, Validity.ofNulls(new boolean[]{ true, false }), 2))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Column 'nothing' is annotated NULL but holds a value at row 1");
    }
}
