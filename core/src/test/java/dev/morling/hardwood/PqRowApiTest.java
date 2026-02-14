/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the RowReader and PqStruct API.
 */
public class PqRowApiTest {

    @Test
    void testPrimitiveTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
            RowReader rowReader = fileReader.createRowReader()) {

            // Test first row
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getFieldCount()).isEqualTo(2);
            assertThat(rowReader.getFieldName(0)).isEqualTo("id");
            assertThat(rowReader.getFieldName(1)).isEqualTo("value");
            assertThat(rowReader.getLong("id")).isEqualTo(1L);
            assertThat(rowReader.getLong("value")).isEqualTo(100L);
            assertThat(rowReader.isNull("id")).isFalse();
            assertThat(rowReader.isNull("value")).isFalse();

            // Verify row 2: id=2, value=200
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(2L);
            assertThat(rowReader.getLong("value")).isEqualTo(200L);

            // Verify row 3: id=3, value=300
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(3L);
            assertThat(rowReader.getLong("value")).isEqualTo(300L);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, birth_date=1990-01-15, created_at_millis=2025-01-01T10:30:00Z
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);
            assertThat(rowReader.getDate("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));
            assertThat(rowReader.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));

            // Row 1: id=2, birth_date=1985-06-30, created_at_millis=2025-01-02T14:45:30Z
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            assertThat(rowReader.getDate("birth_date")).isEqualTo(LocalDate.of(1985, 6, 30));

            // Skip row 2
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testNullValues() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Skip rows 0 and 1
            rowReader.next();
            rowReader.next();

            // Row 2: id=3, address=null
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            assertThat(rowReader.isNull("address")).isTrue();
            assertThat(rowReader.getStruct("address")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedStruct() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, address={street="123 Main St", city="New York", zip=10001}
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);
            PqStruct address0 = rowReader.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("street")).isEqualTo("123 Main St");
            assertThat(address0.getString("city")).isEqualTo("New York");
            assertThat(address0.getInt("zip")).isEqualTo(10001);

            // Row 1: id=2, address={street="456 Oak Ave", city="Los Angeles", zip=90001}
            assertThat(rowReader.hasNext()).isTrue();
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqStruct address1 = rowReader.getStruct("address");
            assertThat(address1).isNotNull();
            assertThat(address1.getString("street")).isEqualTo("456 Oak Ave");
            assertThat(address1.getString("city")).isEqualTo("Los Angeles");
            assertThat(address1.getInt("zip")).isEqualTo(90001);
        }
    }

    @Test
    void testSimpleList() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, tags=["a","b","c"], scores=[10,20,30]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqList tags0 = rowReader.getList("tags");
            assertThat(tags0).isNotNull();
            assertThat(tags0.size()).isEqualTo(3);
            List<String> tagValues0 = new ArrayList<>();
            for (String tag : tags0.strings()) {
                tagValues0.add(tag);
            }
            assertThat(tagValues0).containsExactly("a", "b", "c");

            PqIntList scores0 = rowReader.getListOfInts("scores");
            assertThat(scores0).isNotNull();
            assertThat(scores0.size()).isEqualTo(3);
            List<Integer> scoreValues0 = new ArrayList<>();
            for (int i = 0; i < scores0.size(); i++) {
                scoreValues0.add(scores0.get(i));
            }
            assertThat(scoreValues0).containsExactly(10, 20, 30);

            // Row 1: id=2, tags=[], scores=[100]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqList tags1 = rowReader.getList("tags");
            assertThat(tags1).isNotNull();
            assertThat(tags1.isEmpty()).isTrue();

            // Row 2: id=3, tags=null, scores=[1,2]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            assertThat(rowReader.getList("tags")).isNull();
            PqIntList scores2 = rowReader.getListOfInts("scores");
            assertThat(scores2).isNotNull();
            List<Integer> scoreValues2 = new ArrayList<>();
            for (int i = 0; i < scores2.size(); i++) {
                scoreValues2.add(scores2.get(i));
            }
            assertThat(scoreValues2).containsExactly(1, 2);

            // Row 3: id=4, tags=["single"], scores=null
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(4);
            PqList tags3 = rowReader.getList("tags");
            assertThat(tags3).isNotNull();
            List<String> tagValues3 = new ArrayList<>();
            for (String tag : tags3.strings()) {
                tagValues3.add(tag);
            }
            assertThat(tagValues3).containsExactly("single");
            assertThat(rowReader.getListOfInts("scores")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testListOfStructs() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, items=[{name="apple",quantity=5},{name="banana",quantity=10}]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqList items0 = rowReader.getList("items");
            assertThat(items0).isNotNull();
            assertThat(items0.size()).isEqualTo(2);

            List<PqStruct> itemRows0 = new ArrayList<>();
            for (PqStruct item : items0.structs()) {
                itemRows0.add(item);
            }

            assertThat(itemRows0.get(0).getString("name")).isEqualTo("apple");
            assertThat(itemRows0.get(0).getInt("quantity")).isEqualTo(5);
            assertThat(itemRows0.get(1).getString("name")).isEqualTo("banana");
            assertThat(itemRows0.get(1).getInt("quantity")).isEqualTo(10);

            // Row 1: id=2, items=[{name="orange",quantity=3}]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqList items1 = rowReader.getList("items");
            assertThat(items1).isNotNull();
            assertThat(items1.size()).isEqualTo(1);

            List<PqStruct> itemRows1 = new ArrayList<>();
            for (PqStruct item : items1.structs()) {
                itemRows1.add(item);
            }
            assertThat(itemRows1.get(0).getString("name")).isEqualTo("orange");
            assertThat(itemRows1.get(0).getInt("quantity")).isEqualTo(3);

            // Row 2: id=3, items=[]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            PqList items2 = rowReader.getList("items");
            assertThat(items2).isNotNull();
            assertThat(items2.isEmpty()).isTrue();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedLists() throws Exception {
        // Schema: id, matrix: list<list<int32>>
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, matrix=[[1,2],[3,4,5],[6]]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqList matrix0 = rowReader.getList("matrix");
            assertThat(matrix0).isNotNull();
            assertThat(matrix0.size()).isEqualTo(3);

            List<List<Integer>> matrixValues0 = new ArrayList<>();
            for (PqIntList innerList : matrix0.intLists()) {
                List<Integer> innerValues = new ArrayList<>();
                for (int i = 0; i < innerList.size(); i++) {
                    innerValues.add(innerList.get(i));
                }
                matrixValues0.add(innerValues);
            }

            assertThat(matrixValues0).hasSize(3);
            assertThat(matrixValues0.get(0)).containsExactly(1, 2);
            assertThat(matrixValues0.get(1)).containsExactly(3, 4, 5);
            assertThat(matrixValues0.get(2)).containsExactly(6);

            // Row 1: id=2, matrix=[[10,20]]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            PqList matrix1 = rowReader.getList("matrix");
            assertThat(matrix1).isNotNull();
            assertThat(matrix1.size()).isEqualTo(1);

            List<List<Integer>> matrixValues1 = new ArrayList<>();
            for (PqIntList innerList : matrix1.intLists()) {
                List<Integer> innerValues = new ArrayList<>();
                for (int i = 0; i < innerList.size(); i++) {
                    innerValues.add(innerList.get(i));
                }
                matrixValues1.add(innerValues);
            }
            assertThat(matrixValues1.get(0)).containsExactly(10, 20);

            // Row 2: id=3, matrix=[[],[100],[]]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            PqList matrix2 = rowReader.getList("matrix");
            assertThat(matrix2).isNotNull();
            assertThat(matrix2.size()).isEqualTo(3);

            List<List<Integer>> matrixValues2 = new ArrayList<>();
            for (PqIntList innerList : matrix2.intLists()) {
                List<Integer> innerValues = new ArrayList<>();
                for (int i = 0; i < innerList.size(); i++) {
                    innerValues.add(innerList.get(i));
                }
                matrixValues2.add(innerValues);
            }
            assertThat(matrixValues2.get(0)).isEmpty();
            assertThat(matrixValues2.get(1)).containsExactly(100);
            assertThat(matrixValues2.get(2)).isEmpty();

            // Row 3: id=4, matrix=[]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(4);
            PqList matrix3 = rowReader.getList("matrix");
            assertThat(matrix3).isNotNull();
            assertThat(matrix3.isEmpty()).isTrue();

            // Row 4: id=5, matrix=null
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(5);
            assertThat(rowReader.getList("matrix")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testAddressBookExample() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/address_book_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Record 1: Julien Le Dem
            rowReader.next();
            assertThat(rowReader.getString("owner")).isEqualTo("Julien Le Dem");

            // ownerPhoneNumbers: ["555 123 4567", "555 666 1337"]
            PqList phones1 = rowReader.getList("ownerPhoneNumbers");
            assertThat(phones1).isNotNull();
            assertThat(phones1.size()).isEqualTo(2);
            List<String> phoneValues1 = new ArrayList<>();
            for (String phone : phones1.strings()) {
                phoneValues1.add(phone);
            }
            assertThat(phoneValues1).containsExactly("555 123 4567", "555 666 1337");

            // contacts: [{name: "Dmitriy Ryaboy", phoneNumber: "555 987 6543"},
            // {name: "Chris Aniszczyk", phoneNumber: null}]
            PqList contacts1 = rowReader.getList("contacts");
            assertThat(contacts1).isNotNull();
            assertThat(contacts1.size()).isEqualTo(2);

            List<PqStruct> contactRows1 = new ArrayList<>();
            for (PqStruct contact : contacts1.structs()) {
                contactRows1.add(contact);
            }

            PqStruct contact1_0 = contactRows1.get(0);
            assertThat(contact1_0.getString("name")).isEqualTo("Dmitriy Ryaboy");
            assertThat(contact1_0.getString("phoneNumber")).isEqualTo("555 987 6543");

            PqStruct contact1_1 = contactRows1.get(1);
            assertThat(contact1_1.getString("name")).isEqualTo("Chris Aniszczyk");
            assertThat(contact1_1.getString("phoneNumber")).isNull();

            // Record 2: A. Nonymous (no phone numbers, no contacts)
            rowReader.next();
            assertThat(rowReader.getString("owner")).isEqualTo("A. Nonymous");

            PqList phones2 = rowReader.getList("ownerPhoneNumbers");
            assertThat(phones2).isNotNull();
            assertThat(phones2.isEmpty()).isTrue();

            PqList contacts2 = rowReader.getList("contacts");
            assertThat(contacts2).isNotNull();
            assertThat(contacts2.isEmpty()).isTrue();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testTypeValidationErrorForWrongPhysicalType() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {
            rowReader.next();
            // name is a STRING field, not INT32
            assertThatThrownBy(() -> rowReader.getInt("name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("name");
        }
    }

    @Test
    void testTypeValidationErrorForListExpected() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {
            rowReader.next();
            // address is a struct, not a list
            assertThatThrownBy(() -> rowReader.getList("address"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not a list");
        }
    }

    @Test
    void testDeepNestedStruct() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: Alice with full nested structure
            rowReader.next();
            assertThat(rowReader.getInt("customer_id")).isEqualTo(1);
            assertThat(rowReader.getString("name")).isEqualTo("Alice");

            PqStruct account0 = rowReader.getStruct("account");
            assertThat(account0).isNotNull();
            assertThat(account0.getString("id")).isEqualTo("ACC-001");

            PqStruct org0 = account0.getStruct("organization");
            assertThat(org0).isNotNull();
            assertThat(org0.getString("name")).isEqualTo("Acme Corp");

            PqStruct addr0 = org0.getStruct("address");
            assertThat(addr0).isNotNull();
            assertThat(addr0.getString("street")).isEqualTo("123 Main St");
            assertThat(addr0.getString("city")).isEqualTo("New York");
            assertThat(addr0.getInt("zip")).isEqualTo(10001);

            // Row 1: Bob with null address (3rd level null)
            rowReader.next();
            assertThat(rowReader.getInt("customer_id")).isEqualTo(2);
            assertThat(rowReader.getString("name")).isEqualTo("Bob");

            PqStruct account1 = rowReader.getStruct("account");
            assertThat(account1).isNotNull();
            assertThat(account1.getString("id")).isEqualTo("ACC-002");

            PqStruct org1 = account1.getStruct("organization");
            assertThat(org1).isNotNull();
            assertThat(org1.getString("name")).isEqualTo("TechStart");
            assertThat(org1.getStruct("address")).isNull();

            // Row 2: Charlie with null organization (2nd level null)
            rowReader.next();
            assertThat(rowReader.getInt("customer_id")).isEqualTo(3);
            assertThat(rowReader.getString("name")).isEqualTo("Charlie");

            PqStruct account2 = rowReader.getStruct("account");
            assertThat(account2).isNotNull();
            assertThat(account2.getString("id")).isEqualTo("ACC-003");
            assertThat(account2.getStruct("organization")).isNull();

            // Row 3: Diana with null account (1st level null)
            rowReader.next();
            assertThat(rowReader.getInt("customer_id")).isEqualTo(4);
            assertThat(rowReader.getString("name")).isEqualTo("Diana");
            assertThat(rowReader.getStruct("account")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testFieldMetadata() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {
            rowReader.next();

            // Test field count and names
            assertThat(rowReader.getFieldCount()).isEqualTo(2); // id, address
            assertThat(rowReader.getFieldName(0)).isEqualTo("id");
            assertThat(rowReader.getFieldName(1)).isEqualTo("address");

            // Test nested struct field metadata
            PqStruct address = rowReader.getStruct("address");
            if (address != null) {
                assertThat(address.getFieldCount()).isEqualTo(3); // street, city, zip
                assertThat(address.getFieldName(0)).isEqualTo("street");
                assertThat(address.getFieldName(1)).isEqualTo("city");
                assertThat(address.getFieldName(2)).isEqualTo("zip");
            }
        }
    }

    @Test
    void testStringNullHandling() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, name="alice"
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(1L);
            assertThat(rowReader.isNull("name")).isFalse();
            assertThat(rowReader.getString("name")).isEqualTo("alice");

            // Row 1: id=2, name=null
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(2L);
            assertThat(rowReader.isNull("name")).isTrue();
            assertThat(rowReader.getString("name")).isNull();

            // Row 2: id=3, name="charlie"
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(3L);
            assertThat(rowReader.isNull("name")).isFalse();
            assertThat(rowReader.getString("name")).isEqualTo("charlie");

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testSnappyCompression() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_snappy.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(1L);
            assertThat(rowReader.getLong("value")).isEqualTo(100L);

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(2L);
            assertThat(rowReader.getLong("value")).isEqualTo(200L);

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(3L);
            assertThat(rowReader.getLong("value")).isEqualTo(300L);

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testAllLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: Alice - comprehensive logical type test
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);
            assertThat(rowReader.getString("name")).isEqualTo("Alice");

            // DATE
            assertThat(rowReader.getDate("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));

            // TIMESTAMP with different units
            assertThat(rowReader.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));
            assertThat(rowReader.getTimestamp("created_at_micros")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456Z"));
            assertThat(rowReader.getTimestamp("created_at_nanos")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456789Z"));

            // TIME with different units
            assertThat(rowReader.getTime("wake_time_millis")).isEqualTo(LocalTime.of(7, 30, 0));
            assertThat(rowReader.getTime("wake_time_micros")).isEqualTo(LocalTime.of(7, 30, 0, 123456000));
            assertThat(rowReader.getTime("wake_time_nanos")).isEqualTo(LocalTime.of(7, 30, 0, 123456789));

            // DECIMAL
            assertThat(rowReader.getDecimal("balance")).isEqualTo(new BigDecimal("1234.56"));

            // UUID
            assertThat(rowReader.getUuid("account_id")).isEqualTo(UUID.fromString("12345678-1234-5678-1234-567812345678"));

            // Signed integer types
            assertThat(rowReader.getInt("tiny_int")).isEqualTo(10);
            assertThat(rowReader.getInt("small_int")).isEqualTo(1000);
            assertThat(rowReader.getInt("medium_int")).isEqualTo(100000);
            assertThat(rowReader.getLong("big_int")).isEqualTo(10000000000L);

            // Unsigned integer types
            assertThat(rowReader.getInt("tiny_uint")).isEqualTo(255);
            assertThat(rowReader.getInt("small_uint")).isEqualTo(65535);
            assertThat(rowReader.getInt("medium_uint")).isEqualTo(2147483647);
            assertThat(rowReader.getLong("big_uint")).isEqualTo(9223372036854775807L);

            // Row 1: Bob
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);
            assertThat(rowReader.getString("name")).isEqualTo("Bob");
            assertThat(rowReader.getDate("birth_date")).isEqualTo(LocalDate.of(1985, 6, 30));
            assertThat(rowReader.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-02T14:45:30Z"));
            assertThat(rowReader.getTime("wake_time_millis")).isEqualTo(LocalTime.of(8, 0, 0));
            assertThat(rowReader.getDecimal("balance")).isEqualTo(new BigDecimal("9876.54"));
            assertThat(rowReader.getUuid("account_id")).isEqualTo(UUID.fromString("87654321-4321-8765-4321-876543218765"));

            // Row 2: Charlie
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);
            assertThat(rowReader.getString("name")).isEqualTo("Charlie");
            assertThat(rowReader.getDate("birth_date")).isEqualTo(LocalDate.of(2000, 12, 25));

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedListOfStructs() throws Exception {
        // Schema: Book -> chapters (list) -> Chapter (struct) -> sections (list) -> Section (struct)
        Path parquetFile = Paths.get("src/test/resources/nested_list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Book 0: "Parquet Guide" with 2 chapters
            rowReader.next();
            assertThat(rowReader.getString("title")).isEqualTo("Parquet Guide");

            PqList chapters0 = rowReader.getList("chapters");
            assertThat(chapters0).isNotNull();
            assertThat(chapters0.size()).isEqualTo(2);

            List<PqStruct> chapterRows0 = new ArrayList<>();
            for (PqStruct chapter : chapters0.structs()) {
                chapterRows0.add(chapter);
            }

            // Chapter 0: "Introduction" with 2 sections
            PqStruct chapter0_0 = chapterRows0.get(0);
            assertThat(chapter0_0.getString("name")).isEqualTo("Introduction");

            PqList sections0_0 = chapter0_0.getList("sections");
            assertThat(sections0_0).isNotNull();
            assertThat(sections0_0.size()).isEqualTo(2);

            List<PqStruct> sectionRows0_0 = new ArrayList<>();
            for (PqStruct section : sections0_0.structs()) {
                sectionRows0_0.add(section);
            }

            assertThat(sectionRows0_0.get(0).getString("name")).isEqualTo("What is Parquet");
            assertThat(sectionRows0_0.get(0).getInt("page_count")).isEqualTo(5);
            assertThat(sectionRows0_0.get(1).getString("name")).isEqualTo("History");
            assertThat(sectionRows0_0.get(1).getInt("page_count")).isEqualTo(3);

            // Chapter 1: "Schema" with 3 sections
            PqStruct chapter0_1 = chapterRows0.get(1);
            assertThat(chapter0_1.getString("name")).isEqualTo("Schema");

            PqList sections0_1 = chapter0_1.getList("sections");
            assertThat(sections0_1).isNotNull();
            assertThat(sections0_1.size()).isEqualTo(3);

            List<PqStruct> sectionRows0_1 = new ArrayList<>();
            for (PqStruct section : sections0_1.structs()) {
                sectionRows0_1.add(section);
            }

            assertThat(sectionRows0_1.get(0).getString("name")).isEqualTo("Types");
            assertThat(sectionRows0_1.get(0).getInt("page_count")).isEqualTo(10);
            assertThat(sectionRows0_1.get(1).getString("name")).isEqualTo("Nesting");
            assertThat(sectionRows0_1.get(1).getInt("page_count")).isEqualTo(8);
            assertThat(sectionRows0_1.get(2).getString("name")).isEqualTo("Repetition");
            assertThat(sectionRows0_1.get(2).getInt("page_count")).isEqualTo(12);

            // Book 1: "Empty Chapters" with 1 chapter that has no sections
            rowReader.next();
            assertThat(rowReader.getString("title")).isEqualTo("Empty Chapters");

            PqList chapters1 = rowReader.getList("chapters");
            assertThat(chapters1).isNotNull();
            assertThat(chapters1.size()).isEqualTo(1);

            List<PqStruct> chapterRows1 = new ArrayList<>();
            for (PqStruct chapter : chapters1.structs()) {
                chapterRows1.add(chapter);
            }

            assertThat(chapterRows1.get(0).getString("name")).isEqualTo("The Only Chapter");

            PqList sections1_0 = chapterRows1.get(0).getList("sections");
            assertThat(sections1_0).isNotNull();
            assertThat(sections1_0.isEmpty()).isTrue();

            // Book 2: "No Chapters" with empty chapters list
            rowReader.next();
            assertThat(rowReader.getString("title")).isEqualTo("No Chapters");

            PqList chapters2 = rowReader.getList("chapters");
            assertThat(chapters2).isNotNull();
            assertThat(chapters2.isEmpty()).isTrue();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testTripleNestedLists() throws Exception {
        // Schema: id, cube: list<list<list<int32>>>
        Path parquetFile = Paths.get("src/test/resources/triple_nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, cube=[[[1,2],[3,4]], [[5,6],[7,8]]]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqList cube0 = rowReader.getList("cube");
            assertThat(cube0).isNotNull();
            assertThat(cube0.size()).isEqualTo(2);

            List<List<List<Integer>>> cubeValues0 = new ArrayList<>();
            for (PqList plane : cube0.lists()) {
                List<List<Integer>> middleValues = new ArrayList<>();
                for (PqIntList intRow : plane.intLists()) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (int i = 0; i < intRow.size(); i++) {
                        innerValues.add(intRow.get(i));
                    }
                    middleValues.add(innerValues);
                }
                cubeValues0.add(middleValues);
            }

            assertThat(cubeValues0).hasSize(2);
            assertThat(cubeValues0.get(0)).hasSize(2);
            assertThat(cubeValues0.get(0).get(0)).containsExactly(1, 2);
            assertThat(cubeValues0.get(0).get(1)).containsExactly(3, 4);
            assertThat(cubeValues0.get(1)).hasSize(2);
            assertThat(cubeValues0.get(1).get(0)).containsExactly(5, 6);
            assertThat(cubeValues0.get(1).get(1)).containsExactly(7, 8);

            // Row 1: id=2, cube=[[[10]], [[20,21],[22]]]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(2);

            PqList cube1 = rowReader.getList("cube");
            List<List<List<Integer>>> cubeValues1 = new ArrayList<>();
            for (PqList plane : cube1.lists()) {
                List<List<Integer>> middleValues = new ArrayList<>();
                for (PqIntList intRow : plane.intLists()) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (int i = 0; i < intRow.size(); i++) {
                        innerValues.add(intRow.get(i));
                    }
                    middleValues.add(innerValues);
                }
                cubeValues1.add(middleValues);
            }

            assertThat(cubeValues1).hasSize(2);
            assertThat(cubeValues1.get(0)).hasSize(1);
            assertThat(cubeValues1.get(0).get(0)).containsExactly(10);
            assertThat(cubeValues1.get(1)).hasSize(2);
            assertThat(cubeValues1.get(1).get(0)).containsExactly(20, 21);
            assertThat(cubeValues1.get(1).get(1)).containsExactly(22);

            // Row 2: id=3, cube=[[[]], [[100]]] - with empty innermost list
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(3);

            PqList cube2 = rowReader.getList("cube");
            List<List<List<Integer>>> cubeValues2 = new ArrayList<>();
            for (PqList plane : cube2.lists()) {
                List<List<Integer>> middleValues = new ArrayList<>();
                for (PqIntList intRow : plane.intLists()) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (int i = 0; i < intRow.size(); i++) {
                        innerValues.add(intRow.get(i));
                    }
                    middleValues.add(innerValues);
                }
                cubeValues2.add(middleValues);
            }

            assertThat(cubeValues2).hasSize(2);
            assertThat(cubeValues2.get(0)).hasSize(1);
            assertThat(cubeValues2.get(0).get(0)).isEmpty();
            assertThat(cubeValues2.get(1)).hasSize(1);
            assertThat(cubeValues2.get(1).get(0)).containsExactly(100);

            // Row 3: id=4, cube=[] - empty outer list
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(4);

            PqList cube3 = rowReader.getList("cube");
            assertThat(cube3).isNotNull();
            assertThat(cube3.isEmpty()).isTrue();

            // Row 4: id=5, cube=null
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(5);
            assertThat(rowReader.getList("cube")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedListsWithTimestamps() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: timestamp_matrix=[[2025-01-01T10:00:00Z, 2025-01-01T11:00:00Z], [2025-01-02T12:00:00Z]]
            rowReader.next();

            PqList tsMatrix0 = rowReader.getList("timestamp_matrix");
            assertThat(tsMatrix0).isNotNull();
            assertThat(tsMatrix0.size()).isEqualTo(2);

            List<List<Instant>> tsMatrixValues0 = new ArrayList<>();
            for (PqList innerList : tsMatrix0.lists()) {
                List<Instant> innerValues = new ArrayList<>();
                for (Instant ts : innerList.timestamps()) {
                    innerValues.add(ts);
                }
                tsMatrixValues0.add(innerValues);
            }

            assertThat(tsMatrixValues0).hasSize(2);
            assertThat(tsMatrixValues0.get(0)).hasSize(2);
            assertThat(tsMatrixValues0.get(0).get(0)).isEqualTo(Instant.parse("2025-01-01T10:00:00Z"));
            assertThat(tsMatrixValues0.get(0).get(1)).isEqualTo(Instant.parse("2025-01-01T11:00:00Z"));
            assertThat(tsMatrixValues0.get(1)).hasSize(1);
            assertThat(tsMatrixValues0.get(1).get(0)).isEqualTo(Instant.parse("2025-01-02T12:00:00Z"));

            // Skip to row 4: timestamp_matrix=null
            rowReader.next();
            rowReader.next();
            rowReader.next();
            rowReader.next();
            assertThat(rowReader.getList("timestamp_matrix")).isNull();
        }
    }

    @Test
    void testNestedListsWithStrings() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: string_matrix=[["a","b"],["c"]]
            rowReader.next();

            PqList strMatrix0 = rowReader.getList("string_matrix");
            assertThat(strMatrix0).isNotNull();
            assertThat(strMatrix0.size()).isEqualTo(2);

            List<List<String>> strMatrixValues0 = new ArrayList<>();
            for (PqList innerList : strMatrix0.lists()) {
                List<String> innerValues = new ArrayList<>();
                for (String s : innerList.strings()) {
                    innerValues.add(s);
                }
                strMatrixValues0.add(innerValues);
            }

            assertThat(strMatrixValues0).hasSize(2);
            assertThat(strMatrixValues0.get(0)).containsExactly("a", "b");
            assertThat(strMatrixValues0.get(1)).containsExactly("c");

            // Row 1: string_matrix=[["x","y","z"]]
            rowReader.next();

            PqList strMatrix1 = rowReader.getList("string_matrix");
            List<List<String>> strMatrixValues1 = new ArrayList<>();
            for (PqList innerList : strMatrix1.lists()) {
                List<String> innerValues = new ArrayList<>();
                for (String s : innerList.strings()) {
                    innerValues.add(s);
                }
                strMatrixValues1.add(innerValues);
            }

            assertThat(strMatrixValues1).hasSize(1);
            assertThat(strMatrixValues1.get(0)).containsExactly("x", "y", "z");

            // Row 2: string_matrix=[[]] - empty inner list
            rowReader.next();

            PqList strMatrix2 = rowReader.getList("string_matrix");
            List<List<String>> strMatrixValues2 = new ArrayList<>();
            for (PqList innerList : strMatrix2.lists()) {
                List<String> innerValues = new ArrayList<>();
                for (String s : innerList.strings()) {
                    innerValues.add(s);
                }
                strMatrixValues2.add(innerValues);
            }

            assertThat(strMatrixValues2).hasSize(1);
            assertThat(strMatrixValues2.get(0)).isEmpty();

            // Row 3: string_matrix=[] - empty outer list
            rowReader.next();

            PqList strMatrix3 = rowReader.getList("string_matrix");
            assertThat(strMatrix3).isNotNull();
            assertThat(strMatrix3.isEmpty()).isTrue();

            // Row 4: string_matrix=null
            rowReader.next();
            assertThat(rowReader.getList("string_matrix")).isNull();
        }
    }

    @Test
    void testDictionaryEncoding() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Verify id column (PLAIN encoded) and category column (DICTIONARY encoded)
            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(1L);
            assertThat(rowReader.getString("category")).isEqualTo("A");

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(2L);
            assertThat(rowReader.getString("category")).isEqualTo("B");

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(3L);
            assertThat(rowReader.getString("category")).isEqualTo("A");

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(4L);
            assertThat(rowReader.getString("category")).isEqualTo("C");

            rowReader.next();
            assertThat(rowReader.getLong("id")).isEqualTo(5L);
            assertThat(rowReader.getString("category")).isEqualTo("B");

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testIteratorReuse() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // First iteration
            try (RowReader rowReader1 = fileReader.createRowReader()) {
                int count1 = 0;
                while (rowReader1.hasNext()) {
                    rowReader1.next();
                    count1++;
                }
                assertThat(count1).isEqualTo(3);
            }

            // Second iteration (new reader)
            try (RowReader rowReader2 = fileReader.createRowReader()) {
                int count2 = 0;
                while (rowReader2.hasNext()) {
                    rowReader2.next();
                    count2++;
                }
                assertThat(count2).isEqualTo(3);
            }
        }
    }

    // ==================== Index-based Accessor Tests ====================

    @Test
    void testPrimitiveTypesByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0
            rowReader.next();
            assertThat(rowReader.getInt(0)).isEqualTo(1);
            assertThat(rowReader.getLong(1)).isEqualTo(100L);
            assertThat(rowReader.getFloat(2)).isEqualTo(1.5f);
            assertThat(rowReader.getDouble(3)).isEqualTo(10.5);
            assertThat(rowReader.getBoolean(4)).isTrue();
            assertThat(rowReader.getString(5)).isEqualTo("hello");
            assertThat(rowReader.getBinary(6)).isEqualTo(new byte[]{0x00, 0x01, 0x02});

            // Row 1
            rowReader.next();
            assertThat(rowReader.getInt(0)).isEqualTo(2);
            assertThat(rowReader.getLong(1)).isEqualTo(200L);
            assertThat(rowReader.getFloat(2)).isEqualTo(2.5f);
            assertThat(rowReader.getDouble(3)).isEqualTo(20.5);
            assertThat(rowReader.getBoolean(4)).isFalse();
            assertThat(rowReader.getString(5)).isEqualTo("world");
            assertThat(rowReader.getBinary(6)).isEqualTo(new byte[]{0x03, 0x04, 0x05});

            // Row 2
            rowReader.next();
            assertThat(rowReader.getInt(0)).isEqualTo(3);
            assertThat(rowReader.getLong(1)).isEqualTo(300L);
            assertThat(rowReader.getFloat(2)).isEqualTo(3.5f);
            assertThat(rowReader.getDouble(3)).isEqualTo(30.5);
            assertThat(rowReader.getBoolean(4)).isTrue();
            assertThat(rowReader.getString(5)).isEqualTo("test");
            assertThat(rowReader.getBinary(6)).isEqualTo(new byte[]{0x06, 0x07, 0x08});

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testBinaryAccessor() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            rowReader.next();

            // By name
            assertThat(rowReader.getBinary("binary_col")).isEqualTo(new byte[]{0x00, 0x01, 0x02});

            // By index
            assertThat(rowReader.getBinary(6)).isEqualTo(new byte[]{0x00, 0x01, 0x02});
        }
    }

    @Test
    void testLogicalTypesByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            rowReader.next();

            // Get field indices by checking field names
            int idIdx = -1, birthDateIdx = -1, createdAtMillisIdx = -1;
            int wakeTimeMillisIdx = -1, balanceIdx = -1, accountIdIdx = -1;

            for (int i = 0; i < rowReader.getFieldCount(); i++) {
                String name = rowReader.getFieldName(i);
                switch (name) {
                    case "id" -> idIdx = i;
                    case "birth_date" -> birthDateIdx = i;
                    case "created_at_millis" -> createdAtMillisIdx = i;
                    case "wake_time_millis" -> wakeTimeMillisIdx = i;
                    case "balance" -> balanceIdx = i;
                    case "account_id" -> accountIdIdx = i;
                }
            }

            // Test index-based access
            assertThat(rowReader.getInt(idIdx)).isEqualTo(1);
            assertThat(rowReader.getDate(birthDateIdx)).isEqualTo(LocalDate.of(1990, 1, 15));
            assertThat(rowReader.getTimestamp(createdAtMillisIdx)).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));
            assertThat(rowReader.getTime(wakeTimeMillisIdx)).isEqualTo(LocalTime.of(7, 30, 0));
            assertThat(rowReader.getDecimal(balanceIdx)).isEqualTo(new BigDecimal("1234.56"));
            assertThat(rowReader.getUuid(accountIdIdx)).isEqualTo(UUID.fromString("12345678-1234-5678-1234-567812345678"));
        }
    }

    @Test
    void testIsNullByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: name is not null
            rowReader.next();
            assertThat(rowReader.isNull(1)).isFalse();

            // Row 1: name is null
            rowReader.next();
            assertThat(rowReader.isNull(1)).isTrue();

            // Row 2: name is not null
            rowReader.next();
            assertThat(rowReader.isNull(1)).isFalse();
        }
    }

    @Test
    void testGetValueByNameAndIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            rowReader.next();

            // By name
            assertThat(rowReader.getValue("int_col")).isEqualTo(1);
            assertThat(rowReader.getValue("string_col")).isNotNull();

            // By index
            assertThat(rowReader.getValue(0)).isEqualTo(1);
            assertThat(rowReader.getValue(1)).isEqualTo(100L);
        }
    }

    @Test
    void testByNameAndByIndexConsistency() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();

                assertThat(rowReader.getInt(0)).isEqualTo(rowReader.getInt("int_col"));
                assertThat(rowReader.getLong(1)).isEqualTo(rowReader.getLong("long_col"));
                assertThat(rowReader.getFloat(2)).isEqualTo(rowReader.getFloat("float_col"));
                assertThat(rowReader.getDouble(3)).isEqualTo(rowReader.getDouble("double_col"));
                assertThat(rowReader.getBoolean(4)).isEqualTo(rowReader.getBoolean("bool_col"));
                assertThat(rowReader.getString(5)).isEqualTo(rowReader.getString("string_col"));
                assertThat(rowReader.getBinary(6)).isEqualTo(rowReader.getBinary("binary_col"));

                // isNull should also be consistent
                for (int i = 0; i < rowReader.getFieldCount(); i++) {
                    assertThat(rowReader.isNull(i))
                            .isEqualTo(rowReader.isNull(rowReader.getFieldName(i)));
                }
            }
        }
    }

    // ==================== Primitive List Tests ====================

    @Test
    void testListOfLongs() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: long_list=[100, 200, 300]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqLongList longList0 = rowReader.getListOfLongs("long_list");
            assertThat(longList0).isNotNull();
            assertThat(longList0.size()).isEqualTo(3);
            assertThat(longList0.get(0)).isEqualTo(100L);
            assertThat(longList0.get(1)).isEqualTo(200L);
            assertThat(longList0.get(2)).isEqualTo(300L);

            // Row 1: long_list=[1000]
            rowReader.next();
            PqLongList longList1 = rowReader.getListOfLongs("long_list");
            assertThat(longList1.size()).isEqualTo(1);
            assertThat(longList1.get(0)).isEqualTo(1000L);

            // Row 2: long_list=[1, 2, 3, 4, 5]
            rowReader.next();
            PqLongList longList2 = rowReader.getListOfLongs("long_list");
            assertThat(longList2.size()).isEqualTo(5);

            // Row 3: long_list=null
            rowReader.next();
            assertThat(rowReader.getListOfLongs("long_list")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    @Test
    void testListOfDoubles() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: double_list=[1.1, 2.2, 3.3]
            rowReader.next();
            assertThat(rowReader.getInt("id")).isEqualTo(1);

            PqDoubleList doubleList0 = rowReader.getListOfDoubles("double_list");
            assertThat(doubleList0).isNotNull();
            assertThat(doubleList0.size()).isEqualTo(3);
            assertThat(doubleList0.get(0)).isEqualTo(1.1);
            assertThat(doubleList0.get(1)).isEqualTo(2.2);
            assertThat(doubleList0.get(2)).isEqualTo(3.3);

            // Row 1: double_list=[10.5, 20.5]
            rowReader.next();
            PqDoubleList doubleList1 = rowReader.getListOfDoubles("double_list");
            assertThat(doubleList1.size()).isEqualTo(2);
            assertThat(doubleList1.get(0)).isEqualTo(10.5);
            assertThat(doubleList1.get(1)).isEqualTo(20.5);

            // Row 2: double_list=[]
            rowReader.next();
            PqDoubleList doubleList2 = rowReader.getListOfDoubles("double_list");
            assertThat(doubleList2).isNotNull();
            assertThat(doubleList2.isEmpty()).isTrue();

            // Row 3: double_list=null
            rowReader.next();
            assertThat(rowReader.getListOfDoubles("double_list")).isNull();

            assertThat(rowReader.hasNext()).isFalse();
        }
    }

    // ==================== Index-based Nested Type Tests ====================

    @Test
    void testNestedTypesByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, address={street="123 Main St", city="New York", zip=10001}
            rowReader.next();

            // getInt by index
            assertThat(rowReader.getInt(0)).isEqualTo(1);

            // getStruct by index
            PqStruct address0 = rowReader.getStruct(1);
            assertThat(address0).isNotNull();
            assertThat(address0.getString("street")).isEqualTo("123 Main St");
            assertThat(address0.getString("city")).isEqualTo("New York");
            assertThat(address0.getInt("zip")).isEqualTo(10001);

            // Row 2: address is null
            rowReader.next();
            rowReader.next();
            assertThat(rowReader.isNull(1)).isTrue();
            assertThat(rowReader.getStruct(1)).isNull();
        }
    }

    @Test
    void testListByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0: id=1, tags=["a","b","c"], scores=[10,20,30]
            rowReader.next();

            // Get field indices
            int tagsIdx = -1, scoresIdx = -1;
            for (int i = 0; i < rowReader.getFieldCount(); i++) {
                String name = rowReader.getFieldName(i);
                if ("tags".equals(name)) {
                    tagsIdx = i;
                }
                else if ("scores".equals(name)) {
                    scoresIdx = i;
                }
            }

            // getList by index
            PqList tags0 = rowReader.getList(tagsIdx);
            assertThat(tags0).isNotNull();
            assertThat(tags0.size()).isEqualTo(3);

            // getListOfInts by index
            PqIntList scores0 = rowReader.getListOfInts(scoresIdx);
            assertThat(scores0).isNotNull();
            assertThat(scores0.size()).isEqualTo(3);
            assertThat(scores0.get(0)).isEqualTo(10);
            assertThat(scores0.get(1)).isEqualTo(20);
            assertThat(scores0.get(2)).isEqualTo(30);
        }
    }

    @Test
    void testPrimitiveListsByIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/primitive_lists_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile);
             RowReader rowReader = fileReader.createRowReader()) {

            // Row 0
            rowReader.next();

            // Get field indices
            int intListIdx = -1, longListIdx = -1, doubleListIdx = -1;
            for (int i = 0; i < rowReader.getFieldCount(); i++) {
                String name = rowReader.getFieldName(i);
                switch (name) {
                    case "int_list" -> intListIdx = i;
                    case "long_list" -> longListIdx = i;
                    case "double_list" -> doubleListIdx = i;
                }
            }

            // getListOfInts by index
            PqIntList intList = rowReader.getListOfInts(intListIdx);
            assertThat(intList).isNotNull();
            assertThat(intList.size()).isEqualTo(3);
            assertThat(intList.get(0)).isEqualTo(1);

            // getListOfLongs by index
            PqLongList longList = rowReader.getListOfLongs(longListIdx);
            assertThat(longList).isNotNull();
            assertThat(longList.size()).isEqualTo(3);
            assertThat(longList.get(0)).isEqualTo(100L);

            // getListOfDoubles by index
            PqDoubleList doubleList = rowReader.getListOfDoubles(doubleListIdx);
            assertThat(doubleList).isNotNull();
            assertThat(doubleList.size()).isEqualTo(3);
            assertThat(doubleList.get(0)).isEqualTo(1.1);
        }
    }
}
