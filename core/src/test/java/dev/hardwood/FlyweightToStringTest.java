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
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Coverage for hardwood#472 toString overrides on the flyweight types.
/// Default `Object.toString` returns identity hashes; the overrides render the
/// currently-positioned values so debug logging is useful.
class FlyweightToStringTest {

    private static final Path SIMPLE_MAP = Paths.get("src/test/resources/simple_map_test.parquet");
    private static final Path PRIMITIVE_LISTS = Paths.get("src/test/resources/primitive_lists_test.parquet");
    private static final Path ADDRESS_BOOK = Paths.get("src/test/resources/address_book_test.parquet");

    @Test
    void pqMapToStringRendersBraceKeyEqualsValue() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE_MAP));
             RowReader r = f.rowReader()) {
            r.next(); // Alice, attributes = {age: 30, score: 95, level: 5}
            PqMap m = r.getMap("attributes");
            assertThat(m.toString()).isEqualTo("{age=30, score=95, level=5}");
        }
    }

    @Test
    void pqMapEntryToStringRendersKeyEqualsValue() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE_MAP));
             RowReader r = f.rowReader()) {
            r.next();
            PqMap m = r.getMap("attributes");
            assertThat(m.getEntries().get(0).toString()).isEqualTo("age=30");
        }
    }

    @Test
    void emptyMapToStringIsBraces() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(SIMPLE_MAP));
             RowReader r = f.rowReader()) {
            r.next(); r.next(); r.next(); // Charlie, empty attributes
            PqMap m = r.getMap("attributes");
            assertThat(m.toString()).isEqualTo("{}");
        }
    }

    @Test
    void pqListToStringRendersBracketedElements() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(PRIMITIVE_LISTS));
             RowReader r = f.rowReader()) {
            r.next(); // int_list = [1, 2, 3]
            PqList list = r.getList("int_list");
            assertThat(list.toString()).isEqualTo("[1, 2, 3]");
        }
    }

    @Test
    void pqIntListToStringRendersBracketedInts() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(PRIMITIVE_LISTS));
             RowReader r = f.rowReader()) {
            r.next();
            PqIntList ints = r.getList("int_list").ints();
            assertThat(ints.toString()).isEqualTo("[1, 2, 3]");
        }
    }

    @Test
    void pqStructToStringRendersFieldEqualsValue() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(ADDRESS_BOOK));
             RowReader r = f.rowReader()) {
            r.next();
            PqList contacts = r.getList("contacts");
            PqStruct first = contacts.structs().get(0);
            String s = first.toString();
            assertThat(s).startsWith("PqStruct{").endsWith("}");
            assertThat(s).contains("=");
            assertThat(s).doesNotContain("PqStructImpl@");
        }
    }

    @Test
    void pqStringListToStringRendersQuotedlessStrings() throws Exception {
        // ownerPhoneNumbers is a list<string> on the address_book fixture.
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(ADDRESS_BOOK));
             RowReader r = f.rowReader()) {
            r.next();
            PqList phones = r.getList("ownerPhoneNumbers");
            String s = phones.toString();
            assertThat(s).startsWith("[").endsWith("]");
            assertThat(s).doesNotContain("PqListImpl@");
            assertThat(s).contains("555 ");
        }
    }
}
