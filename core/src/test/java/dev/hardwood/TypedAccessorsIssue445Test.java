/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;

import static org.assertj.core.api.Assertions.assertThat;

/// Coverage for hardwood#445: typed accessors for INTERVAL inside
/// [PqList] / [PqMap.Entry], TIME / DECIMAL map keys, and decoded `getValue()`
/// for map entries.
class TypedAccessorsIssue445Test {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/typed_accessors_issue_445.parquet");

    @Test
    void intervalsListSurfacesAsPqInterval() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqList intervals = rowReader.getList("intervals");
            assertThat(intervals.size()).isEqualTo(2);

            // typed accessor
            List<PqInterval> viaTyped = new ArrayList<>();
            for (PqInterval i : intervals.intervals()) {
                viaTyped.add(i);
            }
            assertThat(viaTyped).containsExactly(
                    new PqInterval(1, 0, 0),
                    new PqInterval(0, 7, 0));

            // generic decoded values
            List<Object> viaValues = new ArrayList<>();
            for (Object o : intervals.values()) {
                viaValues.add(o);
            }
            assertThat(viaValues).containsExactlyElementsOf(viaTyped);

            // indexed accessor
            assertThat(intervals.get(0)).isEqualTo(new PqInterval(1, 0, 0));
            assertThat(intervals.get(1)).isEqualTo(new PqInterval(0, 7, 0));
        }
    }

    @Test
    void intervalsListWithNullElementSurfacesNull() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            // Row 2 carries a non-null list containing [null, 5mo].
            rowReader.next();
            rowReader.next();
            rowReader.next();
            PqList intervals = rowReader.getList("intervals");
            assertThat(intervals.size()).isEqualTo(2);
            assertThat(intervals.isNull(0)).isTrue();
            assertThat(intervals.isNull(1)).isFalse();
            assertThat(intervals.get(0)).isNull();
            assertThat(intervals.get(1)).isEqualTo(new PqInterval(5, 0, 0));

            List<PqInterval> viaTyped = new ArrayList<>();
            for (PqInterval i : intervals.intervals()) {
                viaTyped.add(i);
            }
            assertThat(viaTyped).containsExactly(null, new PqInterval(5, 0, 0));
        }
    }

    @Test
    void intervalValueMapWithNullValueSurfacesNull() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            // Row 2 has a single entry with a non-null key "d" and null INTERVAL value.
            rowReader.next();
            rowReader.next();
            rowReader.next();
            PqMap map = rowReader.getMap("interval_map");
            List<PqMap.Entry> entries = map.getEntries();
            assertThat(entries).hasSize(1);

            PqMap.Entry e = entries.get(0);
            assertThat(e.getStringKey()).isEqualTo("d");
            assertThat(e.isValueNull()).isTrue();
            assertThat(e.getIntervalValue()).isNull();
            assertThat(e.getValue()).isNull();
            assertThat(e.getRawValue()).isNull();
        }
    }

    @Test
    void intervalValueMapSurfacesAsPqInterval() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqMap map = rowReader.getMap("interval_map");
            List<PqMap.Entry> entries = map.getEntries();
            assertThat(entries).hasSize(1);

            PqMap.Entry e = entries.get(0);
            assertThat(e.getStringKey()).isEqualTo("a");
            assertThat(e.getIntervalValue()).isEqualTo(new PqInterval(0, 1, 0));
            // getValue() now decodes the value
            assertThat(e.getValue()).isEqualTo(new PqInterval(0, 1, 0));
            // getRawValue() exposes the underlying physical FLBA(12) bytes, byte
            // for byte — months/days/millis as little-endian unsigned 32-bit.
            assertThat(e.getRawValue()).isInstanceOf(byte[].class);
            byte[] raw = (byte[]) e.getRawValue();
            assertThat(raw).hasSize(12);
            ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
            assertThat(buf.getInt(0)).isEqualTo(0);   // months
            assertThat(buf.getInt(4)).isEqualTo(1);   // days
            assertThat(buf.getInt(8)).isEqualTo(0);   // millis
        }
    }

    @Test
    void timeKeyedMapSupportsGetTimeKey() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqMap map = rowReader.getMap("time_keyed");
            List<PqMap.Entry> entries = map.getEntries();
            assertThat(entries).hasSize(1);

            PqMap.Entry e = entries.get(0);
            assertThat(e.getTimeKey()).isEqualTo(LocalTime.ofNanoOfDay(12345L * 1_000_000));
            // getKey() now decodes the key, mirroring getTimeKey()
            assertThat(e.getKey()).isEqualTo(e.getTimeKey());
            // getRawKey() exposes the underlying Integer millis
            assertThat(e.getRawKey()).isEqualTo(12345);
            assertThat(e.getIntValue()).isEqualTo(100);
        }
    }

    @Test
    void decimalKeyedMapSupportsGetDecimalKey() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            PqMap map = rowReader.getMap("decimal_keyed");
            List<PqMap.Entry> entries = map.getEntries();
            assertThat(entries).hasSize(1);

            PqMap.Entry e = entries.get(0);
            BigDecimal expected = new BigDecimal(java.math.BigInteger.valueOf(12345), 2);
            assertThat(e.getDecimalKey()).isEqualTo(expected);
            // getKey() now decodes the key, mirroring getDecimalKey()
            assertThat(e.getKey()).isEqualTo(expected);
            assertThat(e.getRawKey()).isInstanceOf(byte[].class);
            assertThat(e.getIntValue()).isEqualTo(1);
        }
    }

    @Test
    void rowReaderGetValueReturnsDecodedAndRawSurfacesPhysical() throws Exception {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rowReader = fileReader.rowReader()) {
            rowReader.next();
            // The id column has no logical type — both forms agree.
            assertThat(rowReader.getValue("id")).isEqualTo(1);
            assertThat(rowReader.getRawValue("id")).isEqualTo(1);
        }
    }
}
