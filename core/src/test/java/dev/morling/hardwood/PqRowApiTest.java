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
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the type-safe PqRow API.
 */
public class PqRowApiTest {

    @Test
    void testPrimitiveTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Test first row
                PqRow row0 = rows.get(0);
                assertThat(row0.getFieldCount()).isEqualTo(2);
                assertThat(row0.getFieldName(0)).isEqualTo("id");
                assertThat(row0.getFieldName(1)).isEqualTo("value");

                // Access by index - id is INT64
                Long id0 = row0.getValue(PqType.INT64, 0);
                assertThat(id0).isEqualTo(1L);
                // Access by name
                assertThat(row0.getValue(PqType.INT64, "id")).isEqualTo(1L);
                assertThat(row0.getValue(PqType.INT64, "value")).isEqualTo(100L);
                assertThat(row0.isNull(0)).isFalse();
                assertThat(row0.isNull(1)).isFalse();

                // Verify row 2: id=2, value=200
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT64, 0)).isEqualTo(2L);
                assertThat(row1.getValue(PqType.INT64, "id")).isEqualTo(2L);
                assertThat(row1.getValue(PqType.INT64, 1)).isEqualTo(200L);
                assertThat(row1.getValue(PqType.INT64, "value")).isEqualTo(200L);

                // Verify row 3: id=3, value=300
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT64, 0)).isEqualTo(3L);
                assertThat(row2.getValue(PqType.INT64, "id")).isEqualTo(3L);
                assertThat(row2.getValue(PqType.INT64, 1)).isEqualTo(300L);
                assertThat(row2.getValue(PqType.INT64, "value")).isEqualTo(300L);
            }
        }
    }

    @Test
    void testLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, birth_date=1990-01-15, created_at_millis=2025-01-01T10:30:00Z
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                LocalDate date0 = row0.getValue(PqType.DATE, "birth_date");
                assertThat(date0).isEqualTo(LocalDate.of(1990, 1, 15));

                Instant timestamp0 = row0.getValue(PqType.TIMESTAMP, "created_at_millis");
                assertThat(timestamp0).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));

                // Row 1: id=2, birth_date=1985-06-30, created_at_millis=2025-01-02T14:45:30Z
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);

                LocalDate date1 = row1.getValue(PqType.DATE, "birth_date");
                assertThat(date1).isEqualTo(LocalDate.of(1985, 6, 30));
            }
        }
    }

    @Test
    void testNullValues() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 2: id=3, address=null
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);
                assertThat(row2.isNull("address")).isTrue();
                assertThat(row2.getValue(PqType.ROW, "address")).isNull();
            }
        }
    }

    @Test
    void testNestedStruct() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, address={street="123 Main St", city="New York", zip=10001}
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                PqRow address0 = row0.getValue(PqType.ROW, "address");
                assertThat(address0).isNotNull();
                assertThat(address0.getValue(PqType.STRING, "street")).isEqualTo("123 Main St");
                assertThat(address0.getValue(PqType.STRING, "city")).isEqualTo("New York");
                assertThat(address0.getValue(PqType.INT32, "zip")).isEqualTo(10001);

                // Row 1: id=2, address={street="456 Oak Ave", city="Los Angeles", zip=90001}
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);

                PqRow address1 = row1.getValue(PqType.ROW, "address");
                assertThat(address1).isNotNull();
                assertThat(address1.getValue(PqType.STRING, "street")).isEqualTo("456 Oak Ave");
                assertThat(address1.getValue(PqType.STRING, "city")).isEqualTo("Los Angeles");
                assertThat(address1.getValue(PqType.INT32, "zip")).isEqualTo(90001);
            }
        }
    }

    @Test
    void testSimpleList() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(4);

                // Row 0: id=1, tags=["a","b","c"], scores=[10,20,30]
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                PqList tags0 = row0.getValue(PqType.LIST, "tags");
                assertThat(tags0).isNotNull();
                assertThat(tags0.size()).isEqualTo(3);
                List<String> tagValues0 = new ArrayList<>();
                for (String tag : tags0.getValues(PqType.STRING)) {
                    tagValues0.add(tag);
                }
                assertThat(tagValues0).containsExactly("a", "b", "c");

                PqList scores0 = row0.getValue(PqType.LIST, "scores");
                assertThat(scores0).isNotNull();
                assertThat(scores0.size()).isEqualTo(3);
                List<Integer> scoreValues0 = new ArrayList<>();
                for (Integer score : scores0.getValues(PqType.INT32)) {
                    scoreValues0.add(score);
                }
                assertThat(scoreValues0).containsExactly(10, 20, 30);

                // Row 1: id=2, tags=[], scores=[100]
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);

                PqList tags1 = row1.getValue(PqType.LIST, "tags");
                assertThat(tags1).isNotNull();
                assertThat(tags1.isEmpty()).isTrue();

                // Row 2: id=3, tags=null, scores=[1,2]
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);
                assertThat(row2.getValue(PqType.LIST, "tags")).isNull();
                PqList scores2 = row2.getValue(PqType.LIST, "scores");
                assertThat(scores2).isNotNull();
                List<Integer> scoreValues2 = new ArrayList<>();
                for (Integer score : scores2.getValues(PqType.INT32)) {
                    scoreValues2.add(score);
                }
                assertThat(scoreValues2).containsExactly(1, 2);

                // Row 3: id=4, tags=["single"], scores=null
                PqRow row3 = rows.get(3);
                assertThat(row3.getValue(PqType.INT32, "id")).isEqualTo(4);
                PqList tags3 = row3.getValue(PqType.LIST, "tags");
                assertThat(tags3).isNotNull();
                List<String> tagValues3 = new ArrayList<>();
                for (String tag : tags3.getValues(PqType.STRING)) {
                    tagValues3.add(tag);
                }
                assertThat(tagValues3).containsExactly("single");
                assertThat(row3.getValue(PqType.LIST, "scores")).isNull();
            }
        }
    }

    @Test
    void testListOfStructs() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, items=[{name="apple",quantity=5},{name="banana",quantity=10}]
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                PqList items0 = row0.getValue(PqType.LIST, "items");
                assertThat(items0).isNotNull();
                assertThat(items0.size()).isEqualTo(2);

                List<PqRow> itemRows0 = new ArrayList<>();
                for (PqRow item : items0.getValues(PqType.ROW)) {
                    itemRows0.add(item);
                }

                assertThat(itemRows0.get(0).getValue(PqType.STRING, "name")).isEqualTo("apple");
                assertThat(itemRows0.get(0).getValue(PqType.INT32, "quantity")).isEqualTo(5);
                assertThat(itemRows0.get(1).getValue(PqType.STRING, "name")).isEqualTo("banana");
                assertThat(itemRows0.get(1).getValue(PqType.INT32, "quantity")).isEqualTo(10);

                // Row 1: id=2, items=[{name="orange",quantity=3}]
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);
                PqList items1 = row1.getValue(PqType.LIST, "items");
                assertThat(items1).isNotNull();
                assertThat(items1.size()).isEqualTo(1);

                List<PqRow> itemRows1 = new ArrayList<>();
                for (PqRow item : items1.getValues(PqType.ROW)) {
                    itemRows1.add(item);
                }
                assertThat(itemRows1.get(0).getValue(PqType.STRING, "name")).isEqualTo("orange");
                assertThat(itemRows1.get(0).getValue(PqType.INT32, "quantity")).isEqualTo(3);

                // Row 2: id=3, items=[]
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);

                PqList items2 = row2.getValue(PqType.LIST, "items");
                assertThat(items2).isNotNull();
                assertThat(items2.isEmpty()).isTrue();
            }
        }
    }

    @Test
    void testNestedLists() throws Exception {
        // Schema: id, matrix: list<list<int32>>
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: id=1, matrix=[[1,2],[3,4,5],[6]]
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                PqList matrix0 = row0.getValue(PqType.LIST, "matrix");
                assertThat(matrix0).isNotNull();
                assertThat(matrix0.size()).isEqualTo(3);

                // Iterate over nested lists
                List<List<Integer>> matrixValues0 = new ArrayList<>();
                for (PqList innerList : matrix0.getValues(PqType.LIST)) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (Integer val : innerList.getValues(PqType.INT32)) {
                        innerValues.add(val);
                    }
                    matrixValues0.add(innerValues);
                }

                assertThat(matrixValues0).hasSize(3);
                assertThat(matrixValues0.get(0)).containsExactly(1, 2);
                assertThat(matrixValues0.get(1)).containsExactly(3, 4, 5);
                assertThat(matrixValues0.get(2)).containsExactly(6);

                // Row 1: id=2, matrix=[[10,20]]
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);

                PqList matrix1 = row1.getValue(PqType.LIST, "matrix");
                assertThat(matrix1).isNotNull();
                assertThat(matrix1.size()).isEqualTo(1);

                List<List<Integer>> matrixValues1 = new ArrayList<>();
                for (PqList innerList : matrix1.getValues(PqType.LIST)) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (Integer val : innerList.getValues(PqType.INT32)) {
                        innerValues.add(val);
                    }
                    matrixValues1.add(innerValues);
                }
                assertThat(matrixValues1.get(0)).containsExactly(10, 20);

                // Row 2: id=3, matrix=[[],[100],[]]
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);

                PqList matrix2 = row2.getValue(PqType.LIST, "matrix");
                assertThat(matrix2).isNotNull();
                assertThat(matrix2.size()).isEqualTo(3);

                List<List<Integer>> matrixValues2 = new ArrayList<>();
                for (PqList innerList : matrix2.getValues(PqType.LIST)) {
                    List<Integer> innerValues = new ArrayList<>();
                    for (Integer val : innerList.getValues(PqType.INT32)) {
                        innerValues.add(val);
                    }
                    matrixValues2.add(innerValues);
                }
                assertThat(matrixValues2.get(0)).isEmpty();
                assertThat(matrixValues2.get(1)).containsExactly(100);
                assertThat(matrixValues2.get(2)).isEmpty();

                // Row 3: id=4, matrix=[]
                PqRow row3 = rows.get(3);
                assertThat(row3.getValue(PqType.INT32, "id")).isEqualTo(4);

                PqList matrix3 = row3.getValue(PqType.LIST, "matrix");
                assertThat(matrix3).isNotNull();
                assertThat(matrix3.isEmpty()).isTrue();

                // Row 4: id=5, matrix=null
                PqRow row4 = rows.get(4);
                assertThat(row4.getValue(PqType.INT32, "id")).isEqualTo(5);
                assertThat(row4.getValue(PqType.LIST, "matrix")).isNull();
            }
        }
    }

    @Test
    void testAddressBookExample() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/address_book_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(2);

                // Record 1: Julien Le Dem
                PqRow r1 = rows.get(0);
                assertThat(r1.getValue(PqType.STRING, "owner")).isEqualTo("Julien Le Dem");

                // ownerPhoneNumbers: ["555 123 4567", "555 666 1337"]
                PqList phones1 = r1.getValue(PqType.LIST, "ownerPhoneNumbers");
                assertThat(phones1).isNotNull();
                assertThat(phones1.size()).isEqualTo(2);
                List<String> phoneValues1 = new ArrayList<>();
                for (String phone : phones1.getValues(PqType.STRING)) {
                    phoneValues1.add(phone);
                }
                assertThat(phoneValues1).containsExactly("555 123 4567", "555 666 1337");

                // contacts: [{name: "Dmitriy Ryaboy", phoneNumber: "555 987 6543"},
                // {name: "Chris Aniszczyk", phoneNumber: null}]
                PqList contacts1 = r1.getValue(PqType.LIST, "contacts");
                assertThat(contacts1).isNotNull();
                assertThat(contacts1.size()).isEqualTo(2);

                List<PqRow> contactRows1 = new ArrayList<>();
                for (PqRow contact : contacts1.getValues(PqType.ROW)) {
                    contactRows1.add(contact);
                }

                PqRow contact1_0 = contactRows1.get(0);
                assertThat(contact1_0.getValue(PqType.STRING, "name")).isEqualTo("Dmitriy Ryaboy");
                assertThat(contact1_0.getValue(PqType.STRING, "phoneNumber")).isEqualTo("555 987 6543");

                PqRow contact1_1 = contactRows1.get(1);
                assertThat(contact1_1.getValue(PqType.STRING, "name")).isEqualTo("Chris Aniszczyk");
                assertThat(contact1_1.getValue(PqType.STRING, "phoneNumber")).isNull();

                // Record 2: A. Nonymous (no phone numbers, no contacts)
                PqRow r2 = rows.get(1);
                assertThat(r2.getValue(PqType.STRING, "owner")).isEqualTo("A. Nonymous");

                PqList phones2 = r2.getValue(PqType.LIST, "ownerPhoneNumbers");
                assertThat(phones2).isNotNull();
                assertThat(phones2.isEmpty()).isTrue();

                PqList contacts2 = r2.getValue(PqType.LIST, "contacts");
                assertThat(contacts2).isNotNull();
                assertThat(contacts2.isEmpty()).isTrue();
            }
        }
    }

    @Test
    void testTypeValidationErrorForWrongPhysicalType() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                for (PqRow row : rowReader) {
                    // name is a STRING field, not INT32
                    assertThatThrownBy(() -> row.getValue(PqType.INT32, "name"))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("name");
                    break;
                }
            }
        }
    }

    @Test
    void testTypeValidationErrorForListExpected() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                for (PqRow row : rowReader) {
                    // address is a struct, not a list
                    assertThatThrownBy(() -> row.getValue(PqType.LIST, "address"))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("not a list");
                    break;
                }
            }
        }
    }

    @Test
    void testDeepNestedStruct() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(4);

                // Row 0: Alice with full nested structure
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "customer_id")).isEqualTo(1);
                assertThat(row0.getValue(PqType.STRING, "name")).isEqualTo("Alice");

                PqRow account0 = row0.getValue(PqType.ROW, "account");
                assertThat(account0).isNotNull();
                assertThat(account0.getValue(PqType.STRING, "id")).isEqualTo("ACC-001");

                PqRow org0 = account0.getValue(PqType.ROW, "organization");
                assertThat(org0).isNotNull();
                assertThat(org0.getValue(PqType.STRING, "name")).isEqualTo("Acme Corp");

                PqRow addr0 = org0.getValue(PqType.ROW, "address");
                assertThat(addr0).isNotNull();
                assertThat(addr0.getValue(PqType.STRING, "street")).isEqualTo("123 Main St");
                assertThat(addr0.getValue(PqType.STRING, "city")).isEqualTo("New York");
                assertThat(addr0.getValue(PqType.INT32, "zip")).isEqualTo(10001);

                // Row 1: Bob with null address (3rd level null)
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "customer_id")).isEqualTo(2);
                assertThat(row1.getValue(PqType.STRING, "name")).isEqualTo("Bob");

                PqRow account1 = row1.getValue(PqType.ROW, "account");
                assertThat(account1).isNotNull();
                assertThat(account1.getValue(PqType.STRING, "id")).isEqualTo("ACC-002");

                PqRow org1 = account1.getValue(PqType.ROW, "organization");
                assertThat(org1).isNotNull();
                assertThat(org1.getValue(PqType.STRING, "name")).isEqualTo("TechStart");
                assertThat(org1.getValue(PqType.ROW, "address")).isNull();

                // Row 2: Charlie with null organization (2nd level null)
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "customer_id")).isEqualTo(3);
                assertThat(row2.getValue(PqType.STRING, "name")).isEqualTo("Charlie");

                PqRow account2 = row2.getValue(PqType.ROW, "account");
                assertThat(account2).isNotNull();
                assertThat(account2.getValue(PqType.STRING, "id")).isEqualTo("ACC-003");
                assertThat(account2.getValue(PqType.ROW, "organization")).isNull();

                // Row 3: Diana with null account (1st level null)
                PqRow row3 = rows.get(3);
                assertThat(row3.getValue(PqType.INT32, "customer_id")).isEqualTo(4);
                assertThat(row3.getValue(PqType.STRING, "name")).isEqualTo("Diana");
                assertThat(row3.getValue(PqType.ROW, "account")).isNull();
            }
        }
    }

    @Test
    void testFieldMetadata() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                for (PqRow row : rowReader) {
                    // Test field count and names
                    assertThat(row.getFieldCount()).isEqualTo(2); // id, address
                    assertThat(row.getFieldName(0)).isEqualTo("id");
                    assertThat(row.getFieldName(1)).isEqualTo("address");

                    // Test nested struct field metadata
                    PqRow address = row.getValue(PqType.ROW, "address");
                    if (address != null) {
                        assertThat(address.getFieldCount()).isEqualTo(3); // street, city, zip
                        assertThat(address.getFieldName(0)).isEqualTo("street");
                        assertThat(address.getFieldName(1)).isEqualTo("city");
                        assertThat(address.getFieldName(2)).isEqualTo("zip");
                    }
                    break;
                }
            }
        }
    }

    @Test
    void testStringNullHandling() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: id=1, name="alice"
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT64, "id")).isEqualTo(1L);
                assertThat(row0.isNull("name")).isFalse();
                assertThat(row0.getValue(PqType.STRING, "name")).isEqualTo("alice");

                // Row 1: id=2, name=null
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT64, "id")).isEqualTo(2L);
                assertThat(row1.isNull("name")).isTrue();
                assertThat(row1.getValue(PqType.STRING, "name")).isNull();

                // Row 2: id=3, name="charlie"
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT64, "id")).isEqualTo(3L);
                assertThat(row2.isNull("name")).isFalse();
                assertThat(row2.getValue(PqType.STRING, "name")).isEqualTo("charlie");
            }
        }
    }

    @Test
    void testSnappyCompression() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_snappy.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Verify the rows have correct values (same as uncompressed)
                assertThat(rows.get(0).getValue(PqType.INT64, "id")).isEqualTo(1L);
                assertThat(rows.get(0).getValue(PqType.INT64, "value")).isEqualTo(100L);

                assertThat(rows.get(1).getValue(PqType.INT64, "id")).isEqualTo(2L);
                assertThat(rows.get(1).getValue(PqType.INT64, "value")).isEqualTo(200L);

                assertThat(rows.get(2).getValue(PqType.INT64, "id")).isEqualTo(3L);
                assertThat(rows.get(2).getValue(PqType.INT64, "value")).isEqualTo(300L);
            }
        }
    }

    @Test
    void testAllLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Row 0: Alice - comprehensive logical type test
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);
                assertThat(row0.getValue(PqType.STRING, "name")).isEqualTo("Alice");

                // DATE
                assertThat(row0.getValue(PqType.DATE, "birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));

                // TIMESTAMP with different units
                assertThat(row0.getValue(PqType.TIMESTAMP, "created_at_millis")).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));
                assertThat(row0.getValue(PqType.TIMESTAMP, "created_at_micros")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456Z"));
                assertThat(row0.getValue(PqType.TIMESTAMP, "created_at_nanos")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456789Z"));

                // TIME with different units
                assertThat(row0.getValue(PqType.TIME, "wake_time_millis")).isEqualTo(LocalTime.of(7, 30, 0));
                assertThat(row0.getValue(PqType.TIME, "wake_time_micros")).isEqualTo(LocalTime.of(7, 30, 0, 123456000));
                assertThat(row0.getValue(PqType.TIME, "wake_time_nanos")).isEqualTo(LocalTime.of(7, 30, 0, 123456789));

                // DECIMAL
                assertThat(row0.getValue(PqType.DECIMAL, "balance")).isEqualTo(new BigDecimal("1234.56"));

                // UUID
                assertThat(row0.getValue(PqType.UUID, "account_id")).isEqualTo(UUID.fromString("12345678-1234-5678-1234-567812345678"));

                // Signed integer types (INT_8, INT_16 accessed via INT32; INT_32/INT_64 have no logical type)
                assertThat(row0.getValue(PqType.INT32, "tiny_int")).isEqualTo(10);
                assertThat(row0.getValue(PqType.INT32, "small_int")).isEqualTo(1000);
                assertThat(row0.getValue(PqType.INT32, "medium_int")).isEqualTo(100000);
                assertThat(row0.getValue(PqType.INT64, "big_int")).isEqualTo(10000000000L);

                // Unsigned integer types (UINT_8, UINT_16, UINT_32 via INT32; UINT_64 via INT64)
                assertThat(row0.getValue(PqType.INT32, "tiny_uint")).isEqualTo(255);
                assertThat(row0.getValue(PqType.INT32, "small_uint")).isEqualTo(65535);
                assertThat(row0.getValue(PqType.INT32, "medium_uint")).isEqualTo(2147483647);
                assertThat(row0.getValue(PqType.INT64, "big_uint")).isEqualTo(9223372036854775807L);

                // Row 1: Bob
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);
                assertThat(row1.getValue(PqType.STRING, "name")).isEqualTo("Bob");
                assertThat(row1.getValue(PqType.DATE, "birth_date")).isEqualTo(LocalDate.of(1985, 6, 30));
                assertThat(row1.getValue(PqType.TIMESTAMP, "created_at_millis")).isEqualTo(Instant.parse("2025-01-02T14:45:30Z"));
                assertThat(row1.getValue(PqType.TIME, "wake_time_millis")).isEqualTo(LocalTime.of(8, 0, 0));
                assertThat(row1.getValue(PqType.DECIMAL, "balance")).isEqualTo(new BigDecimal("9876.54"));
                assertThat(row1.getValue(PqType.UUID, "account_id")).isEqualTo(UUID.fromString("87654321-4321-8765-4321-876543218765"));
                assertThat(row1.getValue(PqType.INT32, "tiny_int")).isEqualTo(20);
                assertThat(row1.getValue(PqType.INT32, "small_int")).isEqualTo(2000);
                assertThat(row1.getValue(PqType.INT32, "medium_int")).isEqualTo(200000);
                assertThat(row1.getValue(PqType.INT64, "big_int")).isEqualTo(20000000000L);
                assertThat(row1.getValue(PqType.INT32, "tiny_uint")).isEqualTo(128);
                assertThat(row1.getValue(PqType.INT32, "small_uint")).isEqualTo(32768);
                assertThat(row1.getValue(PqType.INT32, "medium_uint")).isEqualTo(1000000);
                assertThat(row1.getValue(PqType.INT64, "big_uint")).isEqualTo(5000000000000000000L);

                // Row 2: Charlie
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);
                assertThat(row2.getValue(PqType.STRING, "name")).isEqualTo("Charlie");
                assertThat(row2.getValue(PqType.DATE, "birth_date")).isEqualTo(LocalDate.of(2000, 12, 25));
                assertThat(row2.getValue(PqType.DECIMAL, "balance")).isEqualTo(new BigDecimal("5555.55"));
                assertThat(row2.getValue(PqType.UUID, "account_id")).isEqualTo(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
                assertThat(row2.getValue(PqType.INT32, "tiny_int")).isEqualTo(30);
                assertThat(row2.getValue(PqType.INT32, "small_int")).isEqualTo(3000);
                assertThat(row2.getValue(PqType.INT32, "medium_int")).isEqualTo(300000);
                assertThat(row2.getValue(PqType.INT64, "big_int")).isEqualTo(30000000000L);
                assertThat(row2.getValue(PqType.INT32, "tiny_uint")).isEqualTo(64);
                assertThat(row2.getValue(PqType.INT32, "small_uint")).isEqualTo(16384);
                assertThat(row2.getValue(PqType.INT32, "medium_uint")).isEqualTo(500000);
                assertThat(row2.getValue(PqType.INT64, "big_uint")).isEqualTo(4611686018427387904L);
            }
        }
    }

    @Test
    void testNestedListOfStructs() throws Exception {
        // Schema: Book -> chapters (list) -> Chapter (struct) -> sections (list) -> Section (struct)
        Path parquetFile = Paths.get("src/test/resources/nested_list_struct_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(3);

                // Book 0: "Parquet Guide" with 2 chapters
                PqRow book0 = rows.get(0);
                assertThat(book0.getValue(PqType.STRING, "title")).isEqualTo("Parquet Guide");

                PqList chapters0 = book0.getValue(PqType.LIST, "chapters");
                assertThat(chapters0).isNotNull();
                assertThat(chapters0.size()).isEqualTo(2);

                List<PqRow> chapterRows0 = new ArrayList<>();
                for (PqRow chapter : chapters0.getValues(PqType.ROW)) {
                    chapterRows0.add(chapter);
                }

                // Chapter 0: "Introduction" with 2 sections
                PqRow chapter0_0 = chapterRows0.get(0);
                assertThat(chapter0_0.getValue(PqType.STRING, "name")).isEqualTo("Introduction");

                PqList sections0_0 = chapter0_0.getValue(PqType.LIST, "sections");
                assertThat(sections0_0).isNotNull();
                assertThat(sections0_0.size()).isEqualTo(2);

                List<PqRow> sectionRows0_0 = new ArrayList<>();
                for (PqRow section : sections0_0.getValues(PqType.ROW)) {
                    sectionRows0_0.add(section);
                }

                assertThat(sectionRows0_0.get(0).getValue(PqType.STRING, "name")).isEqualTo("What is Parquet");
                assertThat(sectionRows0_0.get(0).getValue(PqType.INT32, "page_count")).isEqualTo(5);
                assertThat(sectionRows0_0.get(1).getValue(PqType.STRING, "name")).isEqualTo("History");
                assertThat(sectionRows0_0.get(1).getValue(PqType.INT32, "page_count")).isEqualTo(3);

                // Chapter 1: "Schema" with 3 sections
                PqRow chapter0_1 = chapterRows0.get(1);
                assertThat(chapter0_1.getValue(PqType.STRING, "name")).isEqualTo("Schema");

                PqList sections0_1 = chapter0_1.getValue(PqType.LIST, "sections");
                assertThat(sections0_1).isNotNull();
                assertThat(sections0_1.size()).isEqualTo(3);

                List<PqRow> sectionRows0_1 = new ArrayList<>();
                for (PqRow section : sections0_1.getValues(PqType.ROW)) {
                    sectionRows0_1.add(section);
                }

                assertThat(sectionRows0_1.get(0).getValue(PqType.STRING, "name")).isEqualTo("Types");
                assertThat(sectionRows0_1.get(0).getValue(PqType.INT32, "page_count")).isEqualTo(10);
                assertThat(sectionRows0_1.get(1).getValue(PqType.STRING, "name")).isEqualTo("Nesting");
                assertThat(sectionRows0_1.get(1).getValue(PqType.INT32, "page_count")).isEqualTo(8);
                assertThat(sectionRows0_1.get(2).getValue(PqType.STRING, "name")).isEqualTo("Repetition");
                assertThat(sectionRows0_1.get(2).getValue(PqType.INT32, "page_count")).isEqualTo(12);

                // Book 1: "Empty Chapters" with 1 chapter that has no sections
                PqRow book1 = rows.get(1);
                assertThat(book1.getValue(PqType.STRING, "title")).isEqualTo("Empty Chapters");

                PqList chapters1 = book1.getValue(PqType.LIST, "chapters");
                assertThat(chapters1).isNotNull();
                assertThat(chapters1.size()).isEqualTo(1);

                List<PqRow> chapterRows1 = new ArrayList<>();
                for (PqRow chapter : chapters1.getValues(PqType.ROW)) {
                    chapterRows1.add(chapter);
                }

                assertThat(chapterRows1.get(0).getValue(PqType.STRING, "name")).isEqualTo("The Only Chapter");

                PqList sections1_0 = chapterRows1.get(0).getValue(PqType.LIST, "sections");
                assertThat(sections1_0).isNotNull();
                assertThat(sections1_0.isEmpty()).isTrue();

                // Book 2: "No Chapters" with empty chapters list
                PqRow book2 = rows.get(2);
                assertThat(book2.getValue(PqType.STRING, "title")).isEqualTo("No Chapters");

                PqList chapters2 = book2.getValue(PqType.LIST, "chapters");
                assertThat(chapters2).isNotNull();
                assertThat(chapters2.isEmpty()).isTrue();
            }
        }
    }

    @Test
    void testTripleNestedLists() throws Exception {
        // Schema: id, cube: list<list<list<int32>>>
        Path parquetFile = Paths.get("src/test/resources/triple_nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Row 0: id=1, cube=[[[1,2],[3,4]], [[5,6],[7,8]]]
                PqRow row0 = rows.get(0);
                assertThat(row0.getValue(PqType.INT32, "id")).isEqualTo(1);

                PqList cube0 = row0.getValue(PqType.LIST, "cube");
                assertThat(cube0).isNotNull();
                assertThat(cube0.size()).isEqualTo(2);

                // Navigate through triple nesting
                List<List<List<Integer>>> cubeValues0 = new ArrayList<>();
                for (PqList outerList : cube0.getValues(PqType.LIST)) {
                    List<List<Integer>> middleValues = new ArrayList<>();
                    for (PqList middleList : outerList.getValues(PqType.LIST)) {
                        List<Integer> innerValues = new ArrayList<>();
                        for (Integer val : middleList.getValues(PqType.INT32)) {
                            innerValues.add(val);
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
                PqRow row1 = rows.get(1);
                assertThat(row1.getValue(PqType.INT32, "id")).isEqualTo(2);

                PqList cube1 = row1.getValue(PqType.LIST, "cube");
                List<List<List<Integer>>> cubeValues1 = new ArrayList<>();
                for (PqList outerList : cube1.getValues(PqType.LIST)) {
                    List<List<Integer>> middleValues = new ArrayList<>();
                    for (PqList middleList : outerList.getValues(PqType.LIST)) {
                        List<Integer> innerValues = new ArrayList<>();
                        for (Integer val : middleList.getValues(PqType.INT32)) {
                            innerValues.add(val);
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
                PqRow row2 = rows.get(2);
                assertThat(row2.getValue(PqType.INT32, "id")).isEqualTo(3);

                PqList cube2 = row2.getValue(PqType.LIST, "cube");
                List<List<List<Integer>>> cubeValues2 = new ArrayList<>();
                for (PqList outerList : cube2.getValues(PqType.LIST)) {
                    List<List<Integer>> middleValues = new ArrayList<>();
                    for (PqList middleList : outerList.getValues(PqType.LIST)) {
                        List<Integer> innerValues = new ArrayList<>();
                        for (Integer val : middleList.getValues(PqType.INT32)) {
                            innerValues.add(val);
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
                PqRow row3 = rows.get(3);
                assertThat(row3.getValue(PqType.INT32, "id")).isEqualTo(4);

                PqList cube3 = row3.getValue(PqType.LIST, "cube");
                assertThat(cube3).isNotNull();
                assertThat(cube3.isEmpty()).isTrue();

                // Row 4: id=5, cube=null
                PqRow row4 = rows.get(4);
                assertThat(row4.getValue(PqType.INT32, "id")).isEqualTo(5);
                assertThat(row4.getValue(PqType.LIST, "cube")).isNull();
            }
        }
    }

    @Test
    void testNestedListsWithTimestamps() throws Exception {
        // Tests nested list with timestamp logical type conversion
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                // Row 0: timestamp_matrix=[[2025-01-01T10:00:00Z, 2025-01-01T11:00:00Z], [2025-01-02T12:00:00Z]]
                PqRow row0 = rows.get(0);

                PqList tsMatrix0 = row0.getValue(PqType.LIST, "timestamp_matrix");
                assertThat(tsMatrix0).isNotNull();
                assertThat(tsMatrix0.size()).isEqualTo(2);

                List<List<Instant>> tsMatrixValues0 = new ArrayList<>();
                for (PqList innerList : tsMatrix0.getValues(PqType.LIST)) {
                    List<Instant> innerValues = new ArrayList<>();
                    for (Instant ts : innerList.getValues(PqType.TIMESTAMP)) {
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

                // Row 4: timestamp_matrix=null
                PqRow row4 = rows.get(4);
                assertThat(row4.getValue(PqType.LIST, "timestamp_matrix")).isNull();
            }
        }
    }

    @Test
    void testNestedListsWithStrings() throws Exception {
        // Tests nested list with string type
        Path parquetFile = Paths.get("src/test/resources/nested_list_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                // Row 0: string_matrix=[["a","b"],["c"]]
                PqRow row0 = rows.get(0);

                PqList strMatrix0 = row0.getValue(PqType.LIST, "string_matrix");
                assertThat(strMatrix0).isNotNull();
                assertThat(strMatrix0.size()).isEqualTo(2);

                List<List<String>> strMatrixValues0 = new ArrayList<>();
                for (PqList innerList : strMatrix0.getValues(PqType.LIST)) {
                    List<String> innerValues = new ArrayList<>();
                    for (String s : innerList.getValues(PqType.STRING)) {
                        innerValues.add(s);
                    }
                    strMatrixValues0.add(innerValues);
                }

                assertThat(strMatrixValues0).hasSize(2);
                assertThat(strMatrixValues0.get(0)).containsExactly("a", "b");
                assertThat(strMatrixValues0.get(1)).containsExactly("c");

                // Row 1: string_matrix=[["x","y","z"]]
                PqRow row1 = rows.get(1);

                PqList strMatrix1 = row1.getValue(PqType.LIST, "string_matrix");
                List<List<String>> strMatrixValues1 = new ArrayList<>();
                for (PqList innerList : strMatrix1.getValues(PqType.LIST)) {
                    List<String> innerValues = new ArrayList<>();
                    for (String s : innerList.getValues(PqType.STRING)) {
                        innerValues.add(s);
                    }
                    strMatrixValues1.add(innerValues);
                }

                assertThat(strMatrixValues1).hasSize(1);
                assertThat(strMatrixValues1.get(0)).containsExactly("x", "y", "z");

                // Row 2: string_matrix=[[]] - empty inner list
                PqRow row2 = rows.get(2);

                PqList strMatrix2 = row2.getValue(PqType.LIST, "string_matrix");
                List<List<String>> strMatrixValues2 = new ArrayList<>();
                for (PqList innerList : strMatrix2.getValues(PqType.LIST)) {
                    List<String> innerValues = new ArrayList<>();
                    for (String s : innerList.getValues(PqType.STRING)) {
                        innerValues.add(s);
                    }
                    strMatrixValues2.add(innerValues);
                }

                assertThat(strMatrixValues2).hasSize(1);
                assertThat(strMatrixValues2.get(0)).isEmpty();

                // Row 3: string_matrix=[] - empty outer list
                PqRow row3 = rows.get(3);

                PqList strMatrix3 = row3.getValue(PqType.LIST, "string_matrix");
                assertThat(strMatrix3).isNotNull();
                assertThat(strMatrix3.isEmpty()).isTrue();

                // Row 4: string_matrix=null
                PqRow row4 = rows.get(4);
                assertThat(row4.getValue(PqType.LIST, "string_matrix")).isNull();
            }
        }
    }

    @Test
    void testDictionaryEncoding() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/dictionary_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<PqRow> rows = new ArrayList<>();
                for (PqRow row : rowReader) {
                    rows.add(row);
                }

                assertThat(rows).hasSize(5);

                // Verify id column (PLAIN encoded)
                assertThat(rows.get(0).getValue(PqType.INT64, "id")).isEqualTo(1L);
                assertThat(rows.get(1).getValue(PqType.INT64, "id")).isEqualTo(2L);
                assertThat(rows.get(2).getValue(PqType.INT64, "id")).isEqualTo(3L);
                assertThat(rows.get(3).getValue(PqType.INT64, "id")).isEqualTo(4L);
                assertThat(rows.get(4).getValue(PqType.INT64, "id")).isEqualTo(5L);

                // Verify category column (DICTIONARY encoded): ['A', 'B', 'A', 'C', 'B']
                assertThat(rows.get(0).getValue(PqType.STRING, "category")).isEqualTo("A");
                assertThat(rows.get(1).getValue(PqType.STRING, "category")).isEqualTo("B");
                assertThat(rows.get(2).getValue(PqType.STRING, "category")).isEqualTo("A");
                assertThat(rows.get(3).getValue(PqType.STRING, "category")).isEqualTo("C");
                assertThat(rows.get(4).getValue(PqType.STRING, "category")).isEqualTo("B");
            }
        }
    }

    @Test
    void testIteratorReuse() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // First iteration
            try (RowReader rowReader1 = fileReader.createRowReader()) {
                int count1 = 0;
                for (PqRow row : rowReader1) {
                    count1++;
                }
                assertThat(count1).isEqualTo(3);
            }

            // Second iteration (new reader)
            try (RowReader rowReader2 = fileReader.createRowReader()) {
                int count2 = 0;
                for (PqRow row : rowReader2) {
                    count2++;
                }
                assertThat(count2).isEqualTo(3);
            }
        }
    }
}
