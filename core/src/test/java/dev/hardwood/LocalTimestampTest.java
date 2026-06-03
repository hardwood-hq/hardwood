/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// End-to-end coverage for the local-wall-clock TIMESTAMP accessors (#568).
///
/// `isAdjustedToUTC = false` columns are *not* instants; the spec defines them
/// as wall-clock timestamps with no zone. Returning them via [#getTimestamp] as
/// an [Instant] silently reinterprets them at UTC, producing the wrong absolute
/// time. The reader keeps the typed-accessor split: `getTimestamp` is
/// instant-only, `getLocalTimestamp` is local-only, and each throws on the
/// other kind.
///
/// Fixture: `tools/simple-datagen.py` → `local_timestamp_test.parquet`. Two
/// rows; three columns with the same wall-clock values stored as NTZ MILLIS,
/// NTZ MICROS, and UTC MICROS so the same field surfaces through both accessor
/// kinds.
class LocalTimestampTest {

    private static final Path FILE = Paths.get("src/test/resources/local_timestamp_test.parquet");

    private static final LocalDateTime ROW0_WALL = LocalDateTime.of(2026, 3, 5, 9, 30, 0);
    private static final LocalDateTime ROW1_WALL = LocalDateTime.of(2026, 3, 5, 17, 45, 30);

    private static ParquetFileReader open() throws IOException {
        return ParquetFileReader.open(InputFile.of(FILE));
    }

    @Test
    void getLocalTimestampReturnsWallClockValue() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLocalTimestamp("local_millis")).isEqualTo(ROW0_WALL);
            assertThat(rows.getLocalTimestamp("local_micros"))
                    .isEqualTo(ROW0_WALL.withNano(123_456_000));

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLocalTimestamp("local_millis")).isEqualTo(ROW1_WALL);
            assertThat(rows.getLocalTimestamp("local_micros"))
                    .isEqualTo(ROW1_WALL.withNano(654_321_000));
        }
    }

    @Test
    void getTimestampRejectsLocalColumn() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getTimestamp("local_micros"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("local_micros")
                    .hasMessageContaining("isAdjustedToUTC=false");
        }
    }

    @Test
    void getLocalTimestampRejectsUtcColumn() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getLocalTimestamp("utc_micros"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("utc_micros")
                    .hasMessageContaining("isAdjustedToUTC=true");
        }
    }

    @Test
    void getTimestampStillReturnsInstantOnUtcColumn() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            Instant expected = ROW0_WALL.withNano(123_456_000).toInstant(java.time.ZoneOffset.UTC);
            assertThat(rows.getTimestamp("utc_micros")).isEqualTo(expected);
        }
    }

    @Test
    void getValueReturnsLocalDateTimeForLocalColumn() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getValue("local_micros"))
                    .isInstanceOf(LocalDateTime.class)
                    .isEqualTo(ROW0_WALL.withNano(123_456_000));
            assertThat(rows.getValue("utc_micros"))
                    .isInstanceOf(Instant.class);
        }
    }

    // ==================== Nested coverage (#568 review) ====================

    @Test
    void pqStructGetLocalTimestampReturnsWallClockValue() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqStruct nested = rows.getStruct("nested");
            assertThat(nested.getLocalTimestamp("local_ts"))
                    .isEqualTo(ROW0_WALL.withNano(123_456_000));
            // wrong-kind rejection through PqStructImpl
            assertThatThrownBy(() -> nested.getTimestamp("local_ts"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("local_ts")
                    .hasMessageContaining("isAdjustedToUTC=false");
            assertThatThrownBy(() -> nested.getLocalTimestamp("utc_ts"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("utc_ts")
                    .hasMessageContaining("isAdjustedToUTC=true");
        }
    }

    @Test
    void pqListLocalTimestampsReturnsWallClockValues() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqList list = rows.getList("local_ts_list");
            assertThat(list.localTimestamps())
                    .containsExactly(ROW0_WALL, LocalDateTime.of(2026, 3, 5, 10, 0, 0));
            // wrong-kind rejection through PqListImpl
            assertThatThrownBy(list::timestamps)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("isAdjustedToUTC=false");
        }
    }

    @Test
    void pqMapGetLocalTimestampValueReturnsWallClockValue() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            PqMap map = rows.getMap("local_ts_map");
            java.util.List<PqMap.Entry> entries = map.getEntries();
            PqMap.Entry first = entries.get(0);
            assertThat(first.getStringKey()).isEqualTo("start");
            assertThat(first.getLocalTimestampValue()).isEqualTo(ROW0_WALL);
            PqMap.Entry second = entries.get(1);
            assertThat(second.getStringKey()).isEqualTo("end");
            assertThat(second.getLocalTimestampValue()).isEqualTo(LocalDateTime.of(2026, 3, 5, 10, 0, 0));
            // wrong-kind rejection through PqMapImpl
            assertThatThrownBy(first::getTimestampValue)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("isAdjustedToUTC=false");
        }
    }

    // ==================== INT96 fallthrough (#568 review) ====================

    @Test
    void int96AcceptedByGetTimestampRejectedByGetLocalTimestamp() throws IOException {
        Path file = Paths.get("src/test/resources/int96_timestamp_test.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            rows.next();
            // INT96 has no isAdjustedToUTC field — treat as UTC-adjusted instant
            // (Spark / Hive convention).
            assertThat(rows.getTimestamp("ts"))
                    .isEqualTo(ROW0_WALL.withNano(123_456_000).toInstant(java.time.ZoneOffset.UTC));
            // The companion accessor rejects INT96 cleanly (was NPE pre-review).
            assertThatThrownBy(() -> rows.getLocalTimestamp("ts"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("ts")
                    .hasMessageContaining("INT96");
        }
    }
}
