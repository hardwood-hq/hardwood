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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.FieldAccessor;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Index-addressed accessors follow the order the projection requests its columns in, with the
/// columns one name expands to in schema order among themselves. `ColumnReaders` gives every
/// request its own positions, each with its own reader; row accessors give each schema node one
/// position, at its first mention.
class ProjectionOrderTest {

    /// `id INT64, value INT64`; rows `(1, 100), (2, 200), (3, 300)`.
    private static final Path FLAT = Paths.get("src/test/resources/plain_uncompressed.parquet");

    /// `id INT32, address STRUCT<street STRING, city STRING, zip INT32>`; row 0 is
    /// `(1, {123 Main St, New York, 10001})`, row 1 `(2, {456 Oak Ave, Los Angeles, 90001})`.
    private static final Path STRUCT = Paths.get("src/test/resources/nested_struct_test.parquet");

    /// `owner STRING, ownerPhoneNumbers LIST<STRING>, contacts LIST<STRUCT<name, phoneNumber>>`;
    /// row 0 is owned by `Julien Le Dem`, whose first contact is `Dmitriy Ryaboy, 555 987 6543`.
    private static final Path ADDRESS_BOOK = Paths.get("src/test/resources/address_book_test.parquet");

    /// `r STRUCT<key INT64, name STRING>, key INT64, amount INT64`; rows `({5, a}, 1005, 1)` and
    /// `({6, b}, 1006, 2)`.
    private static final Path SHARED_LEAF_NAME = Paths.get("src/test/resources/shared_leaf_name_test.parquet");

    // ==================== ColumnReaders ====================

    @Test
    void columnReadersFollowRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("value", "id"))) {

            assertThat(columnPaths(columns)).containsExactly("value", "id");
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getColumnReader(0).getLongs()).startsWith(100L, 200L, 300L);
            assertThat(columns.getColumnReader(1).getLongs()).startsWith(1L, 2L, 3L);
        }
    }

    @Test
    void columnReadersPlaceAnExpandedGroupAtItsRequestPosition() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ADDRESS_BOOK));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("contacts", "owner"))) {

            assertThat(columnPaths(columns)).containsExactly(
                    "contacts.list.element.name", "contacts.list.element.phoneNumber", "owner");
        }
    }

    @Test
    void columnReadersFollowRequestOrderWithinAStruct() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             ColumnReaders columns = reader.columnReaders(
                     ColumnProjection.columns("address.zip", "id", "address.city"))) {

            assertThat(columnPaths(columns)).containsExactly("address.zip", "id", "address.city");
        }
    }

    @Test
    void columnReadersExpandAGroupInSchemaOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("address", "id"))) {

            assertThat(columnPaths(columns)).containsExactly(
                    "address.street", "address.city", "address.zip", "id");
        }
    }

    @Test
    void columnReadersOfAllColumnsFollowSchemaOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {

            assertThat(columnPaths(columns)).containsExactly(
                    "id", "address.street", "address.city", "address.zip");
        }
    }

    @Test
    void filteredColumnReadersFollowRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("value", "id"))
                     .filter(FilterPredicate.gt("id", 1L))
                     .build()) {

            assertThat(columnPaths(columns)).containsExactly("value", "id");
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getRecordCount()).isEqualTo(2);
            assertThat(columns.getColumnReader(0).getLongs()).startsWith(200L, 300L);
            assertThat(columns.getColumnReader(1).getLongs()).startsWith(2L, 3L);
        }
    }

    // ==================== RowReader ====================

    @Test
    void flatRowReaderFollowsRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("value", "id")).build()) {

            assertThat(fieldNames(rows)).containsExactly("value", "id");
            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(100L);
            assertThat(rows.getLong(1)).isEqualTo(1L);
            assertThat(rows.getLong("id")).isEqualTo(1L);
        }
    }

    @Test
    void filteredFlatRowReaderFollowsRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("value", "id"))
                     .filter(FilterPredicate.gt("id", 1L))
                     .build()) {

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(200L);
            assertThat(rows.getLong(1)).isEqualTo(2L);
        }
    }

    @Test
    void nestedRowReaderFollowsRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ADDRESS_BOOK));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("contacts", "owner")).build()) {

            assertThat(fieldNames(rows)).containsExactly("contacts", "owner");
            rows.next();
            assertThat(rows.getList(0).structs().get(0).getString("name")).isEqualTo("Dmitriy Ryaboy");
            assertThat(rows.getString(1)).isEqualTo("Julien Le Dem");
        }
    }

    @Test
    void structChildrenFollowRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("address.zip", "id", "address.city")).build()) {

            assertThat(fieldNames(rows)).containsExactly("address", "id");
            rows.next();
            PqStruct address = rows.getStruct(0);
            assertThat(fieldNames(address)).containsExactly("zip", "city");
            assertThat(address.getInt(0)).isEqualTo(10001);
            assertThat(address.getString(1)).isEqualTo("New York");
            assertThat(rows.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void structChildrenInsideAListFollowRequestOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ADDRESS_BOOK));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns(
                             "contacts.list.element.phoneNumber", "contacts.list.element.name"))
                     .build()) {

            rows.next();
            PqStruct contact = rows.getList("contacts").structs().get(0);
            assertThat(fieldNames(contact)).containsExactly("phoneNumber", "name");
            assertThat(contact.getString(0)).isEqualTo("555 987 6543");
            assertThat(contact.getString(1)).isEqualTo("Dmitriy Ryaboy");
        }
    }

    @Test
    void expandedStructKeepsSchemaOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("address", "id")).build()) {

            assertThat(fieldNames(rows)).containsExactly("address", "id");
            rows.next();
            assertThat(fieldNames(rows.getStruct(0))).containsExactly("street", "city", "zip");
        }
    }

    /// The predicate's columns are read through their own view, indexed by the order the
    /// predicate names them in; naming them out of schema order must still evaluate each
    /// comparison against its own column.
    @Test
    void nestedFilterNamingColumnsOutOfSchemaOrder() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.and(
                             FilterPredicate.eq("address.city", "Los Angeles"),
                             FilterPredicate.eq("id", 2)))
                     .build()) {

            List<Integer> ids = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getInt(0));
            }
            assertThat(ids).containsExactly(2);
        }
    }

    @Test
    void filterOverlappingTheProjectionIsAccepted() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("address", "id"))
                     .filter(FilterPredicate.eq("address.city", "Los Angeles"))
                     .build()) {

            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getInt(1)).isEqualTo(2);
            assertThat(rows.getStruct(0).getString(1)).isEqualTo("Los Angeles");
        }
    }

    // ==================== Repeated and overlapping requests ====================

    @Test
    void columnReadersKeepARepeatedColumnAtEachPosition() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id", "value", "id"))) {

            assertThat(columnPaths(columns)).containsExactly("id", "value", "id");
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getColumnReader(0).getLongs()).startsWith(1L, 2L, 3L);
            assertThat(columns.getColumnReader(2).getLongs()).startsWith(1L, 2L, 3L);
            assertThat(columns.nextBatch()).isFalse();
        }
    }

    /// Each position is its own reader, so stepping every index once per turn moves the group
    /// by one batch even where two indices hold the same column.
    @Test
    void columnReadersWithARepeatedColumnStepReaderByReader() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id", "value", "id"))
                     .batchSize(1)
                     .build()) {

            List<String> rows = new ArrayList<>();
            while (true) {
                boolean advanced = true;
                for (int i = 0; i < columns.getColumnCount(); i++) {
                    advanced &= columns.getColumnReader(i).nextBatch();
                }
                if (!advanced) {
                    break;
                }
                rows.add(columns.getColumnReader(0).getLongs()[0] + "/"
                        + columns.getColumnReader(1).getLongs()[0] + "/"
                        + columns.getColumnReader(2).getLongs()[0]);
            }
            assertThat(rows).containsExactly("1/100/1", "2/200/2", "3/300/3");
        }
    }

    @Test
    void columnReadersExpandEachOverlappingRequest() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("address.zip", "address"))) {

            assertThat(columnPaths(columns)).containsExactly(
                    "address.zip", "address.street", "address.city", "address.zip");
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getColumnReader(3).getInts()).startsWith(10001, 90001);
            assertThat(columns.getColumnReader("address.zip").getInts()).startsWith(10001, 90001);
        }
    }

    @Test
    void rowReaderMergesARepeatedColumn() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("value", "id", "value")).build()) {

            assertThat(fieldNames(rows)).containsExactly("value", "id");
            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(100L);
            assertThat(rows.getLong(1)).isEqualTo(1L);
        }
    }

    @Test
    void rowReaderMergesOverlappingRequestsByFirstMention() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("address.zip", "id", "address")).build()) {

            assertThat(fieldNames(rows)).containsExactly("address", "id");
            rows.next();
            assertThat(fieldNames(rows.getStruct(0))).containsExactly("zip", "street", "city");
            assertThat(rows.getStruct(0).getInt(0)).isEqualTo(10001);
        }
    }

    // ==================== Name resolution ====================

    @Test
    void bareNameDoesNotMatchANestedLeaf() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(STRUCT))) {
            assertThatThrownBy(() -> reader.columnReaders(ColumnProjection.columns("zip")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column not found: zip");
        }
    }

    @Test
    void bareNameResolvesToTheTopLevelColumnSharingANestedLeafName() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SHARED_LEAF_NAME));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("r", "key")).build()) {

            assertThat(fieldNames(rows)).containsExactly("r", "key");
            rows.next();
            assertThat(rows.getStruct(0).getLong("key")).isEqualTo(5L);
            assertThat(rows.getLong(1)).isEqualTo(1005L);
        }
    }

    @Test
    void columnReadersTellATopLevelColumnFromANestedLeafOfTheSameName() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SHARED_LEAF_NAME));
             ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("key", "r.key"))) {

            assertThat(columnPaths(columns)).containsExactly("key", "r.key");
            assertThat(columns.nextBatch()).isTrue();
            assertThat(columns.getColumnReader(0).getLongs()).startsWith(1005L, 1006L);
            assertThat(columns.getColumnReader(1).getLongs()).startsWith(5L, 6L);
        }
    }

    @Test
    void filterOnATopLevelColumnSharingANestedLeafName() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SHARED_LEAF_NAME));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("r"))
                     .filter(FilterPredicate.eq("key", 1006L))
                     .build()) {

            List<String> names = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                names.add(rows.getStruct(0).getString("name"));
            }
            assertThat(names).containsExactly("b");
        }
    }

    @Test
    void filterOnBothColumnsSharingALeafName() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SHARED_LEAF_NAME));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("amount"))
                     .filter(FilterPredicate.and(FilterPredicate.eq("r.key", 5L), FilterPredicate.eq("key", 1005L)))
                     .build()) {

            List<Long> amounts = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                amounts.add(rows.getLong(0));
            }
            assertThat(amounts).containsExactly(1L);
        }
    }

    private static List<String> columnPaths(ColumnReaders columns) {
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < columns.getColumnCount(); i++) {
            paths.add(columns.getColumnReader(i).getColumnSchema().fieldPath().toString());
        }
        return paths;
    }

    private static List<String> fieldNames(FieldAccessor accessor) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < accessor.getFieldCount(); i++) {
            names.add(accessor.getFieldName(i));
        }
        return names;
    }
}
