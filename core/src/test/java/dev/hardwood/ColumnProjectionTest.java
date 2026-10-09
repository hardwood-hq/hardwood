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

import dev.hardwood.internal.reader.FlatRowReader;
import dev.hardwood.internal.reader.NestedRowReader;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.VariantType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for column projection support.
public class ColumnProjectionTest {

    // ==================== ColumnProjection Unit Tests ====================

    @Test
    void testColumnProjectionAll() {
        ColumnProjection projection = ColumnProjection.all();
        assertThat(projection.projectsAll()).isTrue();
        assertThat(projection.getProjectedColumnNames()).isNull();
    }

    @Test
    void testColumnProjectionColumns() {
        ColumnProjection projection = ColumnProjection.columns("id", "name", "address");
        assertThat(projection.projectsAll()).isFalse();
        assertThat(projection.getProjectedColumnNames())
                .containsExactlyInAnyOrder("id", "name", "address");
    }

    @Test
    void testColumnProjectionRejectsEmptyColumns() {
        assertThatThrownBy(ColumnProjection::columns)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("At least one column name must be specified");
    }

    @Test
    void testColumnProjectionRejectsNullColumnName() {
        assertThatThrownBy(() -> ColumnProjection.columns("id", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column name cannot be null or empty");
    }

    @Test
    void testColumnProjectionRejectsEmptyColumnName() {
        assertThatThrownBy(() -> ColumnProjection.columns("id", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column name cannot be null or empty");
    }

    // ==================== ProjectedSchema Unit Tests ====================

    @Test
    void testProjectedSchemaAllColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());

            assertThat(projected.projectsAll()).isTrue();
            assertThat(projected.getProjectedColumnCount()).isEqualTo(2);
            assertThat(projected.toOriginalIndex(0)).isEqualTo(0);
            assertThat(projected.toOriginalIndex(1)).isEqualTo(1);
            assertThat(projected.toProjectedIndex(0)).isEqualTo(0);
            assertThat(projected.toProjectedIndex(1)).isEqualTo(1);
        }
    }

    @Test
    void testProjectedSchemaSubset() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();
            // Select only id and name from the schema
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.columns("id", "name"));

            assertThat(projected.projectsAll()).isFalse();
            assertThat(projected.getProjectedColumnCount()).isEqualTo(2);
            assertThat(projected.getProjectedColumn(0).name()).isEqualTo("id");
            assertThat(projected.getProjectedColumn(1).name()).isEqualTo("name");
        }
    }

    @Test
    void testProjectedSchemaUnknownColumn() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();

            assertThatThrownBy(() -> ProjectedSchema.create(schema, ColumnProjection.columns("nonexistent")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column not found: nonexistent");
        }
    }

    /// Parquet permits any UTF-8 character in a name, `.` included, so a dot in a projection
    /// path need not separate two levels: `acme.address.city` reaches `city` below the group
    /// `acme.address` (#794).
    @Test
    void testDottedGroupNameIsReachableByItsPath() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("schema", 1),
                SchemaElement.group("acme.address", RepetitionType.OPTIONAL, 1),
                SchemaElement.primitive("city", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)));

        assertThat(ProjectedSchema.create(schema, ColumnProjection.columns("acme.address.city"))
                .getProjectedColumnCount()).isEqualTo(1);
        assertThat(ProjectedSchema.create(schema, ColumnProjection.columns("acme.address"))
                .getProjectedColumnCount()).isEqualTo(1);
        assertThatThrownBy(() -> ProjectedSchema.create(schema, ColumnProjection.columns("acme")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column not found: acme");
    }

    // ==================== Flat Schema Projection Tests ====================

    @Test
    void testFlatSchemaReadAllColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.all()).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(2);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getLong("value")).isEqualTo(100L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(2L);
            assertThat(rows.getLong("value")).isEqualTo(200L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(3L);
            assertThat(rows.getLong("value")).isEqualTo(300L);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaReadSingleColumn() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("id");

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getLong(0)).isEqualTo(1L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(2L);

            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(3L);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaAccessNonProjectedColumnThrows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {

            rows.next();

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getLong("value"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[plain_uncompressed.parquet] Column not in projection: value");
        }
    }

    @Test
    void testFlatSchemaProjectionWithNulls() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("name")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: name="alice"
            rows.next();
            assertThat(rows.isNull("name")).isFalse();
            assertThat(rows.getString("name")).isEqualTo("alice");

            // Row 1: name=null
            rows.next();
            assertThat(rows.isNull("name")).isTrue();
            assertThat(rows.getString("name")).isNull();

            // Row 2: name="charlie"
            rows.next();
            assertThat(rows.isNull("name")).isFalse();
            assertThat(rows.getString("name")).isEqualTo("charlie");

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testFlatSchemaProjectMultipleColumns() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id", "name", "balance")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(3);

            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(rows.getString("name")).isEqualTo("Alice");
            assertThat(rows.getDecimal("balance")).isEqualByComparingTo("1234.56");

            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(2);
            assertThat(rows.getString("name")).isEqualTo("Bob");
            assertThat(rows.getDecimal("balance")).isEqualByComparingTo("9876.54");
        }
    }

    // ==================== Nested Schema Projection Tests ====================

    @Test
    void testNestedSchemaProjectTopLevelField() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {

            // The unselected struct does not keep this scalar projection on the nested reader.
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("id");

            // Row 0
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);

            // Row 1
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(2);

            // Row 2
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(3);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedSchemaProjectStructField() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("address")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("address");

            // Row 0: address={street="123 Main St", city="New York", zip=10001}
            rows.next();
            PqStruct address0 = rows.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("street")).isEqualTo("123 Main St");
            assertThat(address0.getString("city")).isEqualTo("New York");
            assertThat(address0.getInt("zip")).isEqualTo(10001);

            // Row 1
            rows.next();
            PqStruct address1 = rows.getStruct("address");
            assertThat(address1).isNotNull();
            assertThat(address1.getString("city")).isEqualTo("Los Angeles");

            // Row 2: address=null
            rows.next();
            assertThat(rows.isNull("address")).isTrue();
            assertThat(rows.getStruct("address")).isNull();

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testNestedSchemaAccessNonProjectedThrows() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("address")).build()) {

            rows.next();

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field 'id' not in projection");
        }
    }

    @Test
    void testNestedSchemaProjectBothFields() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id", "address")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(2);

            // Row 0
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            PqStruct address0 = rows.getStruct("address");
            assertThat(address0).isNotNull();
            assertThat(address0.getString("city")).isEqualTo("New York");

            // Row 2
            rows.next();
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(3);
            assertThat(rows.getStruct("address")).isNull();
        }
    }

    @Test
    void mapValueSubFieldProjectionShouldReadKeysAndValue() throws Exception {
        // map_struct_value_test.parquet: id, people map<string, struct{name, age}>.
        // Row 0: {employee1:{Alice,30}, employee2:{Bob,25}}.
        // Row 1: {manager:{Charlie,45}}.
        // Row 2: empty map.
        //
        // Projecting only the value sub-field people.key_value.value.age should
        // read a well-formed map across all rows: a Parquet MAP's key column is
        // mandatory, so the resolver must keep it (parquet-java does the same).
        Path parquetFile = Paths.get("src/test/resources/map_struct_value_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("people.key_value.value.age")).build()) {

            // The leaf's top-level ancestor is the one field of the row
            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("people");

            List<String> keys = new ArrayList<>();
            List<Integer> ages = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                PqMap people = rows.getMap("people");
                for (PqMap.Entry entry : people.getEntries()) {
                    keys.add(entry.getStringKey());
                    ages.add(entry.getStructValue().getInt("age"));
                }
            }
            assertThat(keys).containsExactly("employee1", "employee2", "manager");
            assertThat(ages).containsExactly(30, 25, 45);
        }
    }

    @Test
    void physicalSkipKeepsMandatoryMapKeyForValueOnlyProjection() throws Exception {
        // Regression guard: a no-filter `skip` must complete containers exactly
        // like the skip-less read path, so a MAP's mandatory key leaf is kept when
        // only the value sub-field is projected. Same fixture as
        // mapValueSubFieldProjectionShouldReadKeysAndValue; skip(1) drops row 0
        // (employee1, employee2), leaving row 1 ({manager}) and the empty-map row 2.
        Path parquetFile = Paths.get("src/test/resources/map_struct_value_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("people.key_value.value.age"))
                     .skip(1).build()) {

            List<String> keys = new ArrayList<>();
            List<Integer> ages = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                PqMap people = rows.getMap("people");
                for (PqMap.Entry entry : people.getEntries()) {
                    keys.add(entry.getStringKey());
                    ages.add(entry.getStructValue().getInt("age"));
                }
            }
            assertThat(keys).containsExactly("manager");
            assertThat(ages).containsExactly(45);
        }
    }

    @Test
    void mapKeyOnlyProjectionDoesNotPullInValue() throws Exception {
        // Same map_struct_value_test.parquet fixture. Projecting only the key
        // sub-field people.key_value.value... is the mirror of the value-side
        // case: the key is structurally mandatory, but the value is not, so
        // container completion must force-include the key and nothing more — a
        // key-only key_value group is a valid map (the value column stays
        // unprojected and reads back as absent).
        Path parquetFile = Paths.get("src/test/resources/map_struct_value_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            FileSchema schema = reader.getFileSchema();

            // Resolver-level: the value leaves must not be force-included.
            ProjectedSchema projected = ProjectedSchema.create(
                    schema, ColumnProjection.columns("people.key_value.key"), true);
            assertThat(projected.getProjectedColumnCount()).isEqualTo(1);
            assertThat(projected.getProjectedColumn(0).name()).isEqualTo("key");

            // Row-level: the map still assembles and the keys read across all
            // rows, including the empty-map row, with the value absent.
            try (RowReader rows = reader.buildRowReader()
                    .projection(ColumnProjection.columns("people.key_value.key")).build()) {

                assertThat(rows.getFieldCount()).isEqualTo(1);
                assertThat(rows.getFieldName(0)).isEqualTo("people");

                List<String> keys = new ArrayList<>();
                while (rows.hasNext()) {
                    rows.next();
                    PqMap people = rows.getMap("people");
                    for (PqMap.Entry entry : people.getEntries()) {
                        keys.add(entry.getStringKey());
                    }
                }
                assertThat(keys).containsExactly("employee1", "employee2", "manager");
            }
        }
    }

    @Test
    void mapOfVariantSubFieldProjectionShouldReadBothSiblings() throws Exception {
        // variant_in_repeated_test.parquet: var_map is map<string, variant{metadata,value}>.
        // Row 0: {a -> BOOLEAN_TRUE, b -> INT32(7)}.
        // Row 1: {c -> "hi"}.
        //
        // Project a single leaf of the variant inside the map
        // (`var_map.key_value.value.value`). The resolver must pull in two
        // structural siblings in one pass: the variant's `metadata` leaf (read
        // atomically) and the MAP's `key` leaf. This exercises the post-order
        // traversal in enforceGroupInvariants on a compound shape.
        Path parquetFile = Paths.get("src/test/resources/variant_in_repeated_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("var_map.key_value.value.value")).build()) {

            rows.next();
            PqMap row0 = rows.getMap("var_map");
            List<PqMap.Entry> r0Entries = row0.getEntries();
            assertThat(r0Entries).hasSize(2);
            assertThat(r0Entries.get(0).getStringKey()).isEqualTo("a");
            assertThat(r0Entries.get(0).getVariantValue().type()).isEqualTo(VariantType.BOOLEAN_TRUE);
            assertThat(r0Entries.get(1).getStringKey()).isEqualTo("b");
            assertThat(r0Entries.get(1).getVariantValue().asInt()).isEqualTo(7);

            rows.next();
            PqMap row1 = rows.getMap("var_map");
            List<PqMap.Entry> r1Entries = row1.getEntries();
            assertThat(r1Entries).hasSize(1);
            assertThat(r1Entries.get(0).getStringKey()).isEqualTo("c");
            assertThat(r1Entries.get(0).getVariantValue().asString()).isEqualTo("hi");

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void variantSubFieldProjectionShouldReassemble() throws Exception {
        // variant_shredded_test.parquet: var is a shredded Variant
        // {metadata, value, typed_value:int64}. Four rows exercise the distinct
        // reassembly outcomes:
        //   Row 1: shredded INT64(42)        — typed_value set
        //   Row 2: unshredded BOOLEAN_TRUE   — value carries the Variant bytes,
        //                                      typed_value null (only readable
        //                                      if `value` is also projected)
        //   Row 3: Variant NULL              — both leaves null at non-null group
        //   Row 4: shredded INT64(10^12)
        //
        // Projecting a single Variant leaf (var.typed_value) must still yield a
        // correctly reassembled Variant across all rows — a Variant is read
        // atomically, so the resolver must pull in metadata and value as well.
        // Row 2 is the load-bearing case: if only typed_value were projected, the
        // unshredded boolean would be lost.
        Path parquetFile = Paths.get("src/test/resources/variant_shredded_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("var.typed_value")).build()) {

            rows.next();
            PqVariant v1 = rows.getVariant("var");
            assertThat(v1).isNotNull();
            assertThat(v1.type()).isEqualTo(VariantType.INT64);
            assertThat(v1.asLong()).isEqualTo(42L);

            rows.next();
            PqVariant v2 = rows.getVariant("var");
            assertThat(v2).isNotNull();
            assertThat(v2.type()).isEqualTo(VariantType.BOOLEAN_TRUE);
            assertThat(v2.asBoolean()).isTrue();

            rows.next();
            PqVariant v3 = rows.getVariant("var");
            assertThat(v3).isNotNull();
            assertThat(v3.type()).isEqualTo(VariantType.NULL);
            assertThat(v3.isNull()).isTrue();

            rows.next();
            PqVariant v4 = rows.getVariant("var");
            assertThat(v4).isNotNull();
            assertThat(v4.type()).isEqualTo(VariantType.INT64);
            assertThat(v4.asLong()).isEqualTo(1_000_000_000_000L);

            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testListFieldProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("tags")).build()) {

            assertThat(rows).isInstanceOf(NestedRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: tags=["a","b","c"]
            rows.next();
            PqList tags = rows.getList("tags");
            assertThat(tags).isNotNull();
            assertThat(tags.size()).isEqualTo(3);
            assertThat(tags.strings()).containsExactly("a", "b", "c");

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field 'id' not in projection");

            // Row 1: tags=[]
            rows.next();
            assertThat(rows.getList("tags").isEmpty()).isTrue();

            // Row 2: tags=null
            rows.next();
            assertThat(rows.getList("tags")).isNull();

            // Row 3: tags=["single"]
            rows.next();
            assertThat(rows.getList("tags").strings()).containsExactly("single");
            assertThat(rows.hasNext()).isFalse();
        }
    }

    /// Projecting every column of a file that mixes a scalar with lists stays on the nested reader.
    @Test
    void testListFileAllColumnsStaysNested() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.all()).build()) {

            assertThat(rows).isInstanceOf(NestedRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(3);
            rows.next();
            assertThat(rows.getInt("id")).isEqualTo(1);
            assertThat(rows.getList("tags").strings()).containsExactly("a", "b", "c");
            assertThat(rows.getList("scores").size()).isEqualTo(3);
        }
    }

    /// `id` is a top-level primitive; the lists beside it are not part of the projection.
    @Test
    void testListFileScalarProjectionReadsFlat() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("id");
            assertThat(readIds(rows)).containsExactly(1, 2, 3, 4);
        }
    }

    @Test
    void testListFileScalarProjectionFiltersFlat() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.gt("id", 2))
                     .build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(readIds(rows)).containsExactly(3, 4);
        }
    }

    /// Tail goes through the same row-reader factory as an ordinary read.
    @Test
    void testListFileScalarProjectionTailReadsFlat() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .tail(2)
                     .build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(readIds(rows)).containsExactly(3, 4);
        }
    }

    /// `labels` (STRING) and `ints` are nullable top-level primitives. `nums` is an unselected list.
    @Test
    void testNullableLogicalScalarsFromMixedFileReadFlat() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/batch_array_identity_test.parquet");
        String[] labels = { "alpha", null, null, "delta" };
        Integer[] ints = { 10, null, null, 40 };

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("labels", "ints")).build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(2);
            assertThat(rows.getFieldName(0)).isEqualTo("labels");
            assertThat(rows.getFieldName(1)).isEqualTo("ints");

            for (int row = 0; row < labels.length; row++) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();

                String label = labels[row];
                assertThat(rows.isNull("labels")).isEqualTo(label == null);
                assertThat(rows.isNull(0)).isEqualTo(label == null);
                assertThat(rows.getString("labels")).isEqualTo(label);
                assertThat(rows.getString(0)).isEqualTo(label);
                assertThat(rows.getValue("labels")).isEqualTo(label);
                assertThat(rows.getValue(0)).isEqualTo(label);

                Integer value = ints[row];
                assertThat(rows.isNull("ints")).isEqualTo(value == null);
                assertThat(rows.isNull(1)).isEqualTo(value == null);
                assertThat(rows.getValue("ints")).isEqualTo(value);
                assertThat(rows.getValue(1)).isEqualTo(value);
                if (value != null) {
                    assertThat(rows.getInt("ints")).isEqualTo(value.intValue());
                    assertThat(rows.getInt(1)).isEqualTo(value.intValue());
                }
            }
            assertThat(rows.hasNext()).isFalse();
            assertThatThrownBy(() -> rows.getString("nums"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[batch_array_identity_test.parquet] Column not in projection: nums");
        }
    }

    /// Filtering on another top-level primitive keeps the flat reader, and that column stays unexposed.
    @Test
    void testFlatFilterOnlyColumnStaysOnFlatRowReader() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/batch_array_identity_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("labels"))
                     .filter(FilterPredicate.gt("ints", 20))
                     .build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("labels");
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getString("labels")).isEqualTo("delta");
            assertThat(rows.getString(0)).isEqualTo("delta");
            assertThatThrownBy(() -> rows.getInt("ints"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[batch_array_identity_test.parquet] Column not in projection: ints");
            assertThatThrownBy(() -> rows.getInt(1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    /// A list the predicate references and the projection does not is still decoded, so the read stays nested.
    /// `tags` is null on id 3 and empty on id 2; the two predicates keep those rows apart, and `tags` stays unexposed.
    @Test
    void testNestedFilterOnlyColumnStaysOnNestedRowReader() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/list_basic_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile))) {
            try (RowReader nullTags = reader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.isNull("tags"))
                    .build()) {

                assertThat(nullTags).isInstanceOf(NestedRowReader.class);
                assertTagsUnexposed(nullTags);
                assertThat(readIds(nullTags)).containsExactly(3);
            }

            try (RowReader presentTags = reader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.isNotNull("tags"))
                    .build()) {

                assertThat(presentTags).isInstanceOf(NestedRowReader.class);
                assertTagsUnexposed(presentTags);
                assertThat(readIds(presentTags)).containsExactly(1, 2, 4);
            }
        }
    }

    @Test
    void testDeepNestedStructProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("account")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: Alice with full nested structure
            rows.next();
            PqStruct account0 = rows.getStruct("account");
            assertThat(account0).isNotNull();
            assertThat(account0.getString("id")).isEqualTo("ACC-001");

            PqStruct org0 = account0.getStruct("organization");
            assertThat(org0).isNotNull();
            assertThat(org0.getString("name")).isEqualTo("Acme Corp");

            PqStruct addr0 = org0.getStruct("address");
            assertThat(addr0).isNotNull();
            assertThat(addr0.getString("city")).isEqualTo("New York");

            // Accessing non-projected column should throw
            assertThatThrownBy(() -> rows.getInt("customer_id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Field 'customer_id' not in projection");
        }
    }

    @Test
    void testDeepNestedStructSubFieldProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("account.id")).build()) {

            // Repetition level zero is not enough: `account` is a group, so the nested reader stays.
            assertThat(rows).isInstanceOf(NestedRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);

            // Row 0: Alice — account.id = "ACC-001"
            rows.next();
            PqStruct account0 = rows.getStruct("account");
            assertThat(account0).isNotNull();
            assertThat(account0.getFieldCount()).isEqualTo(1);
            assertThat(account0.getFieldName(0)).isEqualTo("id");
            assertThat(account0.getString("id")).isEqualTo("ACC-001");

            // Row 1: Bob — account.id = "ACC-002"
            rows.next();
            PqStruct account1 = rows.getStruct("account");
            assertThat(account1).isNotNull();
            assertThat(account1.getFieldCount()).isEqualTo(1);
            assertThat(account1.getString("id")).isEqualTo("ACC-002");

            // Row 2: Charlie — account.id = "ACC-003", organization = null
            rows.next();
            PqStruct account2 = rows.getStruct("account");
            assertThat(account2).isNotNull();
            assertThat(account2.getString("id")).isEqualTo("ACC-003");

            // Row 3: Diana — account = null
            rows.next();
            assertThat(rows.isNull("account")).isTrue();
        }
    }

    // ==================== Index-Based Access Tests ====================

    @Test
    void testIndexBasedAccessWithProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("value")).build()) {

            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("value");

            rows.next();
            // Using projected index 0 to access "value" (originally index 1)
            assertThat(rows.getLong(0)).isEqualTo(100L);

            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(200L);

            rows.next();
            assertThat(rows.getLong(0)).isEqualTo(300L);
        }
    }

    @Test
    void testNestedIndexBasedAccessWithProjection() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/nested_struct_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquetFile));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("address")).build()) {

            assertThat(rows.getFieldCount()).isEqualTo(1);
            assertThat(rows.getFieldName(0)).isEqualTo("address");

            rows.next();
            // Accessing via projected index
            assertThat(rows.isNull(0)).isFalse();
        }
    }

    private static List<Integer> readIds(RowReader rows) throws Exception {
        List<Integer> ids = new ArrayList<>();
        while (rows.hasNext()) {
            rows.next();
            ids.add(rows.getInt("id"));
        }
        return ids;
    }

    private static void assertTagsUnexposed(RowReader rows) {
        assertThat(rows.getFieldCount()).isEqualTo(1);
        assertThat(rows.getFieldName(0)).isEqualTo("id");
        assertThatThrownBy(() -> rows.getList("tags"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Field 'tags' not in projection");
    }
}
