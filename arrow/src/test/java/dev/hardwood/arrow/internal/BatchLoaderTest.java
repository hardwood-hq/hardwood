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

/// Unit tests for [BatchLoader]'s arithmetic and bounds checks on synthetic inputs.
class BatchLoaderTest {

    private static final ColumnSchema COLUMN = new ColumnSchema(FieldPath.of("words"), PhysicalType.BYTE_ARRAY,
            RepetitionType.OPTIONAL, null, 0, 1, 0, null, null);

    private static final byte[] SHARED = new byte[0];

    private static BatchLoader.Views views(int[] starts, int[] ends, Validity validity, int count) {
        return BatchLoader.views(i -> SHARED, i -> starts[i], i -> ends[i], validity, count);
    }

    @Test
    void viewsOfAppendedValuesAreContiguous() {
        int[] starts = { 0, 3, 3, 7 };
        int[] ends = { 3, 3, 7, 9 };
        BatchLoader.Views views = views(starts, ends, Validity.NO_NULLS, 4);
        assertThat(views.totalLength()).isEqualTo(9);
        assertThat(views.contiguous()).isTrue();
        assertThat(views.firstStart()).isEqualTo(0);
    }

    @Test
    void nullsDoNotBreakContiguity() {
        int[] starts = { 5, 0, 8, 10 };
        int[] ends = { 8, 0, 10, 10 };
        Validity validity = Validity.ofNulls(new boolean[]{ false, true, false, false });
        BatchLoader.Views views = views(starts, ends, validity, 4);
        assertThat(views.totalLength()).isEqualTo(5);
        assertThat(views.contiguous()).isTrue();
        assertThat(views.firstStart()).isEqualTo(5);
    }

    @Test
    void dictionaryViewsAreNotContiguous() {
        int[] starts = { 0, 0, 4 };
        int[] ends = { 4, 4, 6 };
        BatchLoader.Views views = views(starts, ends, Validity.NO_NULLS, 3);
        assertThat(views.totalLength()).isEqualTo(10);
        assertThat(views.contiguous()).isFalse();
    }

    @Test
    void expandedLengthPastIntRangeIsRefused() {
        int entry = 1 << 30;
        int[] starts = { 0, 0, 0 };
        int[] ends = { entry, entry, entry };
        BatchLoader.Views views = views(starts, ends, Validity.NO_NULLS, 3);
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
                1, 0, LogicalType.intType(8, true), null);
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
        assertThatThrownBy(() -> BatchLoader.epochNanos(COLUMN, 3, 2333836, 763_145_224_191L))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'words' holds an INT96 timestamp at row 3 (Julian day 2333836, 763145224191 "
                        + "nanoseconds into it) outside the range of an Arrow timestamp in nanoseconds");
        assertThatThrownBy(() -> BatchLoader.epochNanos(COLUMN, 3, 2_000_000_000, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column 'words' holds an INT96 timestamp at row 3 (Julian day 2000000000, 0 nanoseconds "
                        + "into it) outside the range of an Arrow timestamp in nanoseconds");
    }

    @Test
    void presentValueOnNullColumnIsRefused() {
        ColumnSchema nothing = new ColumnSchema(FieldPath.of("nothing"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.unknown(), null);
        BatchLoader.requireAllNull(nothing, Validity.ofNulls(new boolean[]{ true, true }), 2);
        assertThatThrownBy(() -> BatchLoader.requireAllNull(nothing, Validity.ofNulls(new boolean[]{ true, false }), 2))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Column 'nothing' is annotated UNKNOWN but holds a value at row 1");
    }
}
