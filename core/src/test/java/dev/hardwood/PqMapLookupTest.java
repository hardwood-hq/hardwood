/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Coverage for the `PqMap` key-based lookups added in hardwood#463
/// (`getValue` / `getRawValue` / `containsKey` over String / int / long /
/// byte[] keys). The implementation walks the entries linearly and surfaces
/// the first match for duplicate keys; absent keys return `null`.
class PqMapLookupTest {

    private static final Path SIMPLE = Paths.get("src/test/resources/simple_map_test.parquet");
    private static final Path TYPED = Paths.get("src/test/resources/map_types_test.parquet");
    private static final Path TYPED_KEYS = Paths.get("src/test/resources/map_typed_keys_test.parquet");

    @Test
    void getValueByStringKeyReturnsPresent() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            assertThat(m.getValue("age")).isEqualTo(30);
            assertThat(m.getValue("score")).isEqualTo(95);
            assertThat(m.getValue("level")).isEqualTo(5);
        }
    }

    @Test
    void getValueByStringKeyReturnsNullWhenAbsent() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            assertThat(m.getValue("missing")).isNull();
        }
    }

    @Test
    void containsKeyReflectsPresence() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            assertThat(m.containsKey("age")).isTrue();
            assertThat(m.containsKey("missing")).isFalse();
        }
    }

    @Test
    void emptyMapHasNoKeys() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next(); r.next(); r.next(); // row 2: Charlie, empty map
            PqMap m = r.getMap("attributes");
            assertThat(m.isEmpty()).isTrue();
            assertThat(m.containsKey("age")).isFalse();
            assertThat(m.getValue("age")).isNull();
            assertThat(m.getRawValue("age")).isNull();
        }
    }

    @Test
    void singleEntryMapResolves() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next(); r.next(); r.next(); r.next(); r.next(); // row 4: Eve, {single_key: 42}
            PqMap m = r.getMap("attributes");
            assertThat(m.containsKey("single_key")).isTrue();
            assertThat(m.getValue("single_key")).isEqualTo(42);
            assertThat(m.containsKey("other")).isFalse();
        }
    }

    @Test
    void getRawValueMirrorsGetValueForPrimitives() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            // INT32 has no logical-type decoding, so raw and decoded match.
            assertThat(m.getRawValue("age")).isEqualTo(30);
            assertThat(m.getRawValue("missing")).isNull();
        }
    }

    @Test
    void getValueByIntKey() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(TYPED));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("int_map");
            assertThat(m.containsKey(1)).isTrue();
            assertThat(m.getValue(1)).isEqualTo(100L);
            assertThat(m.getValue(2)).isEqualTo(200L);
            assertThat(m.getValue(99)).isNull();
            assertThat(m.containsKey(99)).isFalse();
        }
    }

    @Test
    void nullKeyArgThrowsNpe() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            assertThatNullPointerException().isThrownBy(() -> m.getValue((String) null));
            assertThatNullPointerException().isThrownBy(() -> m.containsKey((byte[]) null));
        }
    }

    @Test
    void getValueByLongKey() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(TYPED_KEYS));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("long_keyed");
            assertThat(m.containsKey(100L)).isTrue();
            assertThat(m.containsKey(999L)).isFalse();
            assertThat(m.getValue(100L)).isEqualTo("one-hundred");
            assertThat(m.getValue(200L)).isEqualTo("two-hundred");
            assertThat(m.getValue(300L)).isEqualTo("three-hundred");
            assertThat(m.getValue(999L)).isNull();
            assertThat(m.getRawValue(100L)).isInstanceOf(byte[].class);
            assertThat(m.getRawValue(999L)).isNull();
        }
    }

    @Test
    void getValueByBinaryKey() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(TYPED_KEYS));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("binary_keyed");
            byte[] firstKey = {0x01, 0x02};
            byte[] secondKey = {0x03, 0x04};
            byte[] absent = {0x05, 0x06};
            assertThat(m.containsKey(firstKey)).isTrue();
            assertThat(m.containsKey(absent)).isFalse();
            assertThat(m.getValue(firstKey)).isEqualTo(10);
            assertThat(m.getValue(secondKey)).isEqualTo(20);
            assertThat(m.getValue(absent)).isNull();
            assertThat(m.getRawValue(firstKey)).isEqualTo(10);
            assertThat(m.getRawValue(absent)).isNull();
            // Arrays.equals semantics — a fresh array with the same content matches.
            byte[] firstKeyCopy = {0x01, 0x02};
            assertThat(m.containsKey(firstKeyCopy)).isTrue();
            assertThat(m.getValue(firstKeyCopy)).isEqualTo(10);
        }
    }

    @Test
    void wrongKeyTypeForColumnThrows() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes"); // map<string, int32>
            // Map has String keys; lookup as int casts the byte[][] column to int[].
            assertThatThrownBy(() -> m.getValue(42))
                    .isInstanceOf(ClassCastException.class);
        }
    }
}
