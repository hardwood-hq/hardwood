/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading Parquet files with nested data structures.
 */
public class NestedTypesTest {

    @Test
    void testNestedStruct() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, address={street="123 Main St", city="New York", zip=10001}
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);

                Row address0 = row0.getStruct("address");
                assertThat(address0).isNotNull();
                assertThat(address0.getString("street")).isEqualTo("123 Main St");
                assertThat(address0.getString("city")).isEqualTo("New York");
                assertThat(address0.getInt("zip")).isEqualTo(10001);

                // Row 1: id=2, address={street="456 Oak Ave", city="Los Angeles", zip=90001}
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);

                Row address1 = row1.getStruct("address");
                assertThat(address1).isNotNull();
                assertThat(address1.getString("street")).isEqualTo("456 Oak Ave");
                assertThat(address1.getString("city")).isEqualTo("Los Angeles");
                assertThat(address1.getInt("zip")).isEqualTo(90001);

                // Row 2: id=3, address=null
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                assertThat(row2.getStruct("address")).isNull();
            }
        }
    }

    @Test
    void testListOfBasicTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(4);

                // Row 0: id=1, tags=["a","b","c"], scores=[10,20,30]
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                assertThat(row0.getStringList("tags")).containsExactly("a", "b", "c");
                assertThat(row0.getIntList("scores")).containsExactly(10, 20, 30);

                // Row 1: id=2, tags=[], scores=[100]
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);
                assertThat(row1.getStringList("tags")).isEmpty();
                assertThat(row1.getIntList("scores")).containsExactly(100);

                // Row 2: id=3, tags=null, scores=[1,2]
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                assertThat(row2.getStringList("tags")).isNull();
                assertThat(row2.getIntList("scores")).containsExactly(1, 2);

                // Row 3: id=4, tags=["single"], scores=null
                Row row3 = rows.get(3);
                assertThat(row3.getInt("id")).isEqualTo(4);
                assertThat(row3.getStringList("tags")).containsExactly("single");
                assertThat(row3.getIntList("scores")).isNull();
            }
        }
    }

    @Test
    void testNestedListOfStructs() throws Exception {
        // Schema: Book -> chapters (list) -> Chapter (struct) -> sections (list) -> Section (struct)
        Path parquetFile = Paths.get("src/test/resources/nested_list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Book 0: "Parquet Guide" with 2 chapters
                Row book0 = rows.get(0);
                assertThat(book0.getString("title")).isEqualTo("Parquet Guide");
                List<Row> chapters0 = book0.getStructList("chapters");
                assertThat(chapters0).hasSize(2);

                // Chapter 0: "Introduction" with 2 sections
                Row chapter0_0 = chapters0.get(0);
                assertThat(chapter0_0.getString("name")).isEqualTo("Introduction");
                List<Row> sections0_0 = chapter0_0.getStructList("sections");
                assertThat(sections0_0).hasSize(2);
                assertThat(sections0_0.get(0).getString("name")).isEqualTo("What is Parquet");
                assertThat(sections0_0.get(0).getInt("page_count")).isEqualTo(5);
                assertThat(sections0_0.get(1).getString("name")).isEqualTo("History");
                assertThat(sections0_0.get(1).getInt("page_count")).isEqualTo(3);

                // Chapter 1: "Schema" with 3 sections
                Row chapter0_1 = chapters0.get(1);
                assertThat(chapter0_1.getString("name")).isEqualTo("Schema");
                List<Row> sections0_1 = chapter0_1.getStructList("sections");
                assertThat(sections0_1).hasSize(3);
                assertThat(sections0_1.get(0).getString("name")).isEqualTo("Types");
                assertThat(sections0_1.get(0).getInt("page_count")).isEqualTo(10);
                assertThat(sections0_1.get(1).getString("name")).isEqualTo("Nesting");
                assertThat(sections0_1.get(1).getInt("page_count")).isEqualTo(8);
                assertThat(sections0_1.get(2).getString("name")).isEqualTo("Repetition");
                assertThat(sections0_1.get(2).getInt("page_count")).isEqualTo(12);

                // Book 1: "Empty Chapters" with 1 chapter that has no sections
                Row book1 = rows.get(1);
                assertThat(book1.getString("title")).isEqualTo("Empty Chapters");
                List<Row> chapters1 = book1.getStructList("chapters");
                assertThat(chapters1).hasSize(1);
                assertThat(chapters1.get(0).getString("name")).isEqualTo("The Only Chapter");
                List<Row> sections1_0 = chapters1.get(0).getStructList("sections");
                assertThat(sections1_0).isEmpty();

                // Book 2: "No Chapters" with empty chapters list
                Row book2 = rows.get(2);
                assertThat(book2.getString("title")).isEqualTo("No Chapters");
                List<Row> chapters2 = book2.getStructList("chapters");
                assertThat(chapters2).isEmpty();
            }
        }
    }

    @Test
    void testListOfStructs() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, items=[{name="apple",quantity=5},{name="banana",quantity=10}]
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                List<Row> items0 = row0.getStructList("items");
                assertThat(items0).hasSize(2);
                assertThat(items0.get(0).getString("name")).isEqualTo("apple");
                assertThat(items0.get(0).getInt("quantity")).isEqualTo(5);
                assertThat(items0.get(1).getString("name")).isEqualTo("banana");
                assertThat(items0.get(1).getInt("quantity")).isEqualTo(10);

                // Row 1: id=2, items=[{name="orange",quantity=3}]
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);
                List<Row> items1 = row1.getStructList("items");
                assertThat(items1).hasSize(1);
                assertThat(items1.get(0).getString("name")).isEqualTo("orange");
                assertThat(items1.get(0).getInt("quantity")).isEqualTo(3);

                // Row 2: id=3, items=[]
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                List<Row> items2 = row2.getStructList("items");
                assertThat(items2).isEmpty();
            }
        }
    }

    @Test
    void testDeepNestedStruct() throws Exception {
        // Schema: Customer -> Account -> Organization -> Address (4 levels of nesting)
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(4);

                // Row 0: Alice with full nested structure
                Row row0 = rows.get(0);
                assertThat(row0.getInt("customer_id")).isEqualTo(1);
                assertThat(row0.getString("name")).isEqualTo("Alice");

                Row account0 = row0.getStruct("account");
                assertThat(account0).isNotNull();
                assertThat(account0.getString("id")).isEqualTo("ACC-001");

                Row org0 = account0.getStruct("organization");
                assertThat(org0).isNotNull();
                assertThat(org0.getString("name")).isEqualTo("Acme Corp");

                Row addr0 = org0.getStruct("address");
                assertThat(addr0).isNotNull();
                assertThat(addr0.getString("street")).isEqualTo("123 Main St");
                assertThat(addr0.getString("city")).isEqualTo("New York");
                assertThat(addr0.getInt("zip")).isEqualTo(10001);

                // Row 1: Bob with null address (3rd level null)
                Row row1 = rows.get(1);
                assertThat(row1.getInt("customer_id")).isEqualTo(2);
                assertThat(row1.getString("name")).isEqualTo("Bob");

                Row account1 = row1.getStruct("account");
                assertThat(account1).isNotNull();
                assertThat(account1.getString("id")).isEqualTo("ACC-002");

                Row org1 = account1.getStruct("organization");
                assertThat(org1).isNotNull();
                assertThat(org1.getString("name")).isEqualTo("TechStart");
                assertThat(org1.getStruct("address")).isNull();

                // Row 2: Charlie with null organization (2nd level null)
                Row row2 = rows.get(2);
                assertThat(row2.getInt("customer_id")).isEqualTo(3);
                assertThat(row2.getString("name")).isEqualTo("Charlie");

                Row account2 = row2.getStruct("account");
                assertThat(account2).isNotNull();
                assertThat(account2.getString("id")).isEqualTo("ACC-003");
                assertThat(account2.getStruct("organization")).isNull();

                // Row 3: Diana with null account (1st level null)
                Row row3 = rows.get(3);
                assertThat(row3.getInt("customer_id")).isEqualTo(4);
                assertThat(row3.getString("name")).isEqualTo("Diana");
                assertThat(row3.getStruct("account")).isNull();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNestedLists() throws Exception {
        // Schema: id, matrix: list<list<int32>>, string_matrix: list<list<string>>, timestamp_matrix: list<list<timestamp>>
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: id=1, matrix=[[1,2],[3,4,5],[6]], string_matrix=[["a","b"],["c"]]
                // timestamp_matrix=[[2025-01-01T10:00:00Z, 2025-01-01T11:00:00Z], [2025-01-02T12:00:00Z]]
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);

                List<List<Integer>> matrix0 = (List<List<Integer>>) row0.getList("matrix");
                assertThat(matrix0).hasSize(3);
                assertThat(matrix0.get(0)).containsExactly(1, 2);
                assertThat(matrix0.get(1)).containsExactly(3, 4, 5);
                assertThat(matrix0.get(2)).containsExactly(6);

                List<List<String>> stringMatrix0 = convertNestedByteArraysToStrings(row0.getList("string_matrix"));
                assertThat(stringMatrix0).hasSize(2);
                assertThat(stringMatrix0.get(0)).containsExactly("a", "b");
                assertThat(stringMatrix0.get(1)).containsExactly("c");

                // Verify timestamps are converted to Instant (not raw Long)
                List<List<Instant>> tsMatrix0 = (List<List<Instant>>) row0.getList("timestamp_matrix");
                assertThat(tsMatrix0).hasSize(2);
                assertThat(tsMatrix0.get(0)).hasSize(2);
                assertThat(tsMatrix0.get(0).get(0)).isEqualTo(Instant.parse("2025-01-01T10:00:00Z"));
                assertThat(tsMatrix0.get(0).get(1)).isEqualTo(Instant.parse("2025-01-01T11:00:00Z"));
                assertThat(tsMatrix0.get(1)).hasSize(1);
                assertThat(tsMatrix0.get(1).get(0)).isEqualTo(Instant.parse("2025-01-02T12:00:00Z"));

                // Row 1: id=2, matrix=[[10,20]], string_matrix=[["x","y","z"]]
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);

                List<List<Integer>> matrix1 = (List<List<Integer>>) row1.getList("matrix");
                assertThat(matrix1).hasSize(1);
                assertThat(matrix1.get(0)).containsExactly(10, 20);

                List<List<String>> stringMatrix1 = convertNestedByteArraysToStrings(row1.getList("string_matrix"));
                assertThat(stringMatrix1).hasSize(1);
                assertThat(stringMatrix1.get(0)).containsExactly("x", "y", "z");

                // Row 2: id=3, matrix=[[],[100],[]], string_matrix=[[]]
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);

                List<List<Integer>> matrix2 = (List<List<Integer>>) row2.getList("matrix");
                assertThat(matrix2).hasSize(3);
                assertThat(matrix2.get(0)).isEmpty();
                assertThat(matrix2.get(1)).containsExactly(100);
                assertThat(matrix2.get(2)).isEmpty();

                List<List<String>> stringMatrix2 = convertNestedByteArraysToStrings(row2.getList("string_matrix"));
                assertThat(stringMatrix2).hasSize(1);
                assertThat(stringMatrix2.get(0)).isEmpty();

                // Row 3: id=4, matrix=[], string_matrix=[]
                Row row3 = rows.get(3);
                assertThat(row3.getInt("id")).isEqualTo(4);

                List<List<Integer>> matrix3 = (List<List<Integer>>) row3.getList("matrix");
                assertThat(matrix3).isEmpty();

                List<List<String>> stringMatrix3 = convertNestedByteArraysToStrings(row3.getList("string_matrix"));
                assertThat(stringMatrix3).isEmpty();

                // Row 4: id=5, matrix=null, string_matrix=null, timestamp_matrix=null
                Row row4 = rows.get(4);
                assertThat(row4.getInt("id")).isEqualTo(5);
                assertThat(row4.getList("matrix")).isNull();
                assertThat(row4.getList("string_matrix")).isNull();
                assertThat(row4.getList("timestamp_matrix")).isNull();
            }
        }
    }

    /**
     * Test for triple-nested lists: list<list<list<int32>>>.
     * Verifies the generalized nested list assembly works for arbitrary depth.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testTripleNestedLists() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/triple_nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: id=1, cube=[[[1,2],[3,4]], [[5,6],[7,8]]]
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                List<List<List<Integer>>> cube0 = (List<List<List<Integer>>>) row0.getList("cube");
                assertThat(cube0).hasSize(2);
                assertThat(cube0.get(0)).hasSize(2);
                assertThat(cube0.get(0).get(0)).containsExactly(1, 2);
                assertThat(cube0.get(0).get(1)).containsExactly(3, 4);
                assertThat(cube0.get(1)).hasSize(2);
                assertThat(cube0.get(1).get(0)).containsExactly(5, 6);
                assertThat(cube0.get(1).get(1)).containsExactly(7, 8);

                // Row 1: id=2, cube=[[[10]], [[20,21],[22]]]
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);
                List<List<List<Integer>>> cube1 = (List<List<List<Integer>>>) row1.getList("cube");
                assertThat(cube1).hasSize(2);
                assertThat(cube1.get(0)).hasSize(1);
                assertThat(cube1.get(0).get(0)).containsExactly(10);
                assertThat(cube1.get(1)).hasSize(2);
                assertThat(cube1.get(1).get(0)).containsExactly(20, 21);
                assertThat(cube1.get(1).get(1)).containsExactly(22);

                // Row 2: id=3, cube=[[[]], [[100]]] - with empty innermost list
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                List<List<List<Integer>>> cube2 = (List<List<List<Integer>>>) row2.getList("cube");
                assertThat(cube2).hasSize(2);
                assertThat(cube2.get(0)).hasSize(1);
                assertThat(cube2.get(0).get(0)).isEmpty();
                assertThat(cube2.get(1)).hasSize(1);
                assertThat(cube2.get(1).get(0)).containsExactly(100);

                // Row 3: id=4, cube=[] - empty outer list
                Row row3 = rows.get(3);
                assertThat(row3.getInt("id")).isEqualTo(4);
                List<?> cube3 = row3.getList("cube");
                assertThat(cube3).isEmpty();

                // Row 4: id=5, cube=null
                Row row4 = rows.get(4);
                assertThat(row4.getInt("id")).isEqualTo(5);
                assertThat(row4.getList("cube")).isNull();
            }
        }
    }

    /**
     * Test for the classic AddressBook example from the Dremel paper / Twitter blog post.
     * Schema:
     *   message AddressBook {
     *     required string owner;
     *     repeated string ownerPhoneNumbers;
     *     repeated group contacts {
     *       required string name;
     *       optional string phoneNumber;
     *     }
     *   }
     *
     * Data:
     *   r1: owner="Julien Le Dem", ownerPhoneNumbers=["555 123 4567","555 666 1337"],
     *       contacts=[{name:"Dmitriy Ryaboy", phoneNumber:"555 987 6543"},
     *                 {name:"Chris Aniszczyk", phoneNumber:null}]
     *   r2: owner="A. Nonymous", ownerPhoneNumbers=[], contacts=[]
     *
     * Column levels for contacts.phoneNumber:
     *   R=0, D=2, "555 987 6543"  (first contact of r1, has phone)
     *   R=1, D=1, null            (second contact of r1, no phone)
     *   R=0, D=0, null            (r2 has no contacts)
     */
    @Test
    void testAddressBook() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/address_book_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(2);

                // Record 1: Julien Le Dem
                Row r1 = rows.get(0);
                assertThat(r1.getString("owner")).isEqualTo("Julien Le Dem");

                // ownerPhoneNumbers: ["555 123 4567", "555 666 1337"]
                List<String> phones1 = r1.getStringList("ownerPhoneNumbers");
                assertThat(phones1).containsExactly("555 123 4567", "555 666 1337");

                // contacts: [{name: "Dmitriy Ryaboy", phoneNumber: "555 987 6543"},
                // {name: "Chris Aniszczyk", phoneNumber: null}]
                List<Row> contacts1 = r1.getStructList("contacts");
                assertThat(contacts1).hasSize(2);

                Row contact1_0 = contacts1.get(0);
                assertThat(contact1_0.getString("name")).isEqualTo("Dmitriy Ryaboy");
                assertThat(contact1_0.getString("phoneNumber")).isEqualTo("555 987 6543");

                Row contact1_1 = contacts1.get(1);
                assertThat(contact1_1.getString("name")).isEqualTo("Chris Aniszczyk");
                assertThat(contact1_1.getString("phoneNumber")).isNull(); // optional field is null

                // Record 2: A. Nonymous (no phone numbers, no contacts)
                Row r2 = rows.get(1);
                assertThat(r2.getString("owner")).isEqualTo("A. Nonymous");

                // ownerPhoneNumbers: [] (empty list)
                List<String> phones2 = r2.getStringList("ownerPhoneNumbers");
                assertThat(phones2).isEmpty();

                // contacts: [] (empty list)
                List<Row> contacts2 = r2.getStructList("contacts");
                assertThat(contacts2).isEmpty();
            }
        }
    }

    /**
     * Helper to convert nested lists of byte[] to nested lists of String.
     */
    @SuppressWarnings("unchecked")
    private List<List<String>> convertNestedByteArraysToStrings(List<?> nestedList) {
        if (nestedList == null) {
            return null;
        }
        List<List<String>> result = new ArrayList<>();
        for (Object innerObj : nestedList) {
            List<?> innerList = (List<?>) innerObj;
            List<String> convertedInner = new ArrayList<>();
            for (Object item : innerList) {
                if (item instanceof byte[]) {
                    convertedInner.add(new String((byte[]) item, StandardCharsets.UTF_8));
                }
                else if (item instanceof String) {
                    convertedInner.add((String) item);
                }
            }
            result.add(convertedInner);
        }
        return result;
    }
}
