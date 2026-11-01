/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.BatchFilterCompiler;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.reader.row.FlatRowReader;
import dev.hardwood.internal.reader.row.NestedRowReader;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.StructBuilder;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads of projections whose top-level fields include non-repeated structs, which the flat
/// reader serves.
class FlatStructRowReaderTest {

    private static final int ROWS = 5_000;
    private static final Path NESTED_STRUCT = Paths.get("src/test/resources/nested_struct_test.parquet");
    private static final Path DEEP_NESTED_STRUCT = Paths.get("src/test/resources/deep_nested_struct_test.parquet");

    @TempDir
    static Path dir;

    /// `id`, an optional struct `s` holding an optional struct `inner`, a required struct `r` of
    /// the remaining physical types, a required struct `q` of one optional leaf, and a `tags`
    /// list, which puts a projection that includes it on the nested reader.
    private static Path structs;

    @BeforeAll
    static void writeFixture() throws IOException {
        structs = dir.resolve("flat_structs.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .struct("s", RepetitionType.OPTIONAL, s -> s
                        .addColumn("a", PhysicalType.INT32, RepetitionType.OPTIONAL)
                        .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType())
                        .struct("inner", RepetitionType.OPTIONAL, inner -> inner
                                .addColumn("x", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                                .addColumn("day", PhysicalType.INT32, RepetitionType.OPTIONAL, new LogicalType.DateType())))
                .struct("r", RepetitionType.REQUIRED, r -> r
                        .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                        .addColumn("flag", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL)
                        .addColumn("f", PhysicalType.FLOAT, RepetitionType.OPTIONAL)
                        .addColumn("price", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 8,
                                LogicalType.decimal(18, 2))
                        .addColumn("key", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 16,
                                LogicalType.uuid())
                        .addColumn("blob", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL))
                .struct("q", RepetitionType.REQUIRED, q -> q
                        .addColumn("o", PhysicalType.INT32, RepetitionType.OPTIONAL))
                .list("tags", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(1_200).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(structs), schema, config)) {
            for (int i = 0; i < ROWS; i++) {
                final int id = i;
                writer.rowWriter().writeRow(row -> {
                    row.setLong("id", id);
                    if (id % 6 == 0) {
                        row.setNull("s");
                    }
                    else {
                        row.setStruct("s", s -> fillS(s, id));
                    }
                    row.setStruct("r", r -> fillR(r, id));
                    row.setStruct("q", q -> {
                        if (id % 5 == 0) {
                            q.setNull("o");
                        }
                        else {
                            q.setInt("o", id);
                        }
                    });
                    row.setList("tags", tags -> tags.addLong(id));
                });
            }
        }
    }

    /// Rows cycle through a struct whose leaves are all null, a null inner struct beside present
    /// leaves, an inner struct whose leaves are null, and every leaf present.
    private static void fillS(StructBuilder s, int id) {
        switch (id % 6) {
            case 1 -> s.setNull("a").setString("name", "n" + (id % 7)).setNull("inner");
            case 2 -> s.setInt("a", id).setString("name", "n" + (id % 7))
                    .setStruct("inner", inner -> inner.setNull("x").setNull("day"));
            case 3 -> s.setInt("a", id).setNull("name")
                    .setStruct("inner", inner -> inner.setDouble("x", id * 0.5).setDate("day", day(id)));
            case 4 -> s.setInt("a", id).setString("name", "n" + (id % 7))
                    .setStruct("inner", inner -> inner.setDouble("x", id * 0.25).setNull("day"));
            default -> s.setNull("a").setNull("name").setNull("inner");
        }
    }

    /// Every fifth row leaves the optional leaves of `r` null.
    private static void fillR(StructBuilder r, int id) {
        r.setLong("b", -id);
        if (id % 5 == 0) {
            r.setNull("flag").setNull("f").setNull("price").setNull("key").setNull("blob");
        }
        else {
            r.setBoolean("flag", id % 2 == 0)
                    .setFloat("f", id * 0.5f)
                    .setDecimal("price", BigDecimal.valueOf(id, 2))
                    .setUuid("key", new UUID(id, -id))
                    .setBinary("blob", new byte[] { (byte) id, (byte) (id >>> 8) });
        }
    }

    private static LocalDate day(int id) {
        return LocalDate.of(2020, 1, 1).plusDays(id);
    }

    @Test
    void readsTheSameValuesAsTheNestedReader() throws Exception {
        assertThat(readFlatAndNested(null)).hasSize(ROWS);
    }

    /// Filters on leaves below a struct, and a filter on a top-level leaf, which prunes row groups
    /// and leaves one undecided, so that the struct batches the flat reader serves are the ones
    /// the filter keeps rows of.
    static Stream<FilterPredicate> filters() {
        return Stream.of(
                FilterPredicate.gt("id", 4_000L),
                FilterPredicate.gt("s.inner.x", 1_000.0),
                FilterPredicate.isNull("s.a"),
                FilterPredicate.isNotNull("s.inner.day"),
                FilterPredicate.eq("s.name", "n3"),
                FilterPredicate.and(FilterPredicate.gt("id", 2_000L), FilterPredicate.isNull("s.inner.x")),
                FilterPredicate.or(FilterPredicate.lt("r.b", -4_500L), FilterPredicate.eq("s.a", 7)),
                FilterPredicate.isNull("q.o"));
    }

    @ParameterizedTest
    @MethodSource("filters")
    void filtersOnStructLeavesAsTheNestedReaderDoes(FilterPredicate filter) throws Exception {
        assertThat(readFlatAndNested(filter)).isNotEmpty();
    }

    /// A null test on a group takes the per-row matcher on the flat reader: `q` is required, so
    /// `q IS NULL` holds on no row, though its leaf `o` is null on every fifth.
    @Test
    void nullTestOnARequiredStructIsAConstant() throws Exception {
        assertThat(readFlatAndNested(FilterPredicate.isNull("q"))).isEmpty();
        assertThat(readFlatAndNested(FilterPredicate.isNotNull("q"))).hasSize(ROWS);
    }

    /// `s.a` and `id` each appear in both arms of the `OR`, which no batch matcher can hold, so
    /// the flat reader evaluates the leaf below `s` through its per-row matcher.
    @Test
    void filtersOnAStructLeafWithThePerRowMatcherAsTheNestedReaderDoes() throws Exception {
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.and(FilterPredicate.gt("s.a", 4_900), FilterPredicate.lt("id", 4_950L)),
                FilterPredicate.and(FilterPredicate.lt("s.a", 50), FilterPredicate.gt("id", 10L)));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs))) {
            FileSchema schema = reader.getFileSchema();
            ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());
            assertThat(BatchFilterCompiler.tryCompile(FilterPredicateResolver.resolve(filter, schema), schema,
                    projected::toProjectedIndex))
                    .as("filter %s must take the per-row matcher", filter)
                    .isNull();
        }
        // `s.a` is set, to the row's id, where `id % 6` is 2, 3 or 4
        IntPredicate aSet = id -> id % 6 >= 2 && id % 6 <= 4;
        List<Long> expected = expectedIds(id -> aSet.test(id)
                && ((id > 4_900 && id < 4_950) || (id < 50 && id > 10)));
        assertThat(readFlatAndNested(filter)).hasSize(expected.size());
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader rows = filtered(reader, filter, "id")) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).isEqualTo(expected);
    }

    /// Reads `id`, `s` and `r` under `filter` through the flat reader, and through the nested
    /// reader by adding `tags` to the projection, and returns the rows once both agree.
    private static List<String> readFlatAndNested(FilterPredicate filter) throws IOException {
        List<String> flat = new ArrayList<>();
        List<String> nested = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader flatRows = filtered(reader, filter, "id", "s", "r");
             RowReader nestedRows = filtered(reader, filter, "id", "s", "r", "tags")) {
            assertThat(flatRows).isInstanceOf(FlatRowReader.class);
            assertThat(nestedRows).isInstanceOf(NestedRowReader.class);
            while (flatRows.hasNext()) {
                flatRows.next();
                flat.add(render(flatRows));
            }
            while (nestedRows.hasNext()) {
                nestedRows.next();
                nested.add(render(nestedRows));
            }
        }
        assertThat(flat).isEqualTo(nested);
        return flat;
    }

    private static RowReader filtered(ParquetFileReader reader, FilterPredicate filter, String... columns)
            throws IOException {
        ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader().projection(ColumnProjection.columns(columns));
        return (filter != null ? builder.filter(filter) : builder).build();
    }

    /// The first three fields of the row, by index and by name, with the nullness of each.
    private static String render(RowReader rows) {
        StringBuilder out = new StringBuilder();
        for (int field = 0; field < 3; field++) {
            out.append(rows.getFieldName(field)).append(rows.isNull(field) ? " null " : " ")
                    .append(rows.getValue(field)).append(' ').append(rows.getRawValue(field)).append("; ");
        }
        PqStruct s = rows.getStruct("s");
        if (s != null) {
            PqStruct inner = s.getStruct("inner");
            out.append(s.isNull("a") ? "a=null" : "a=" + s.getInt("a"))
                    .append(" name=").append(s.getString("name"))
                    .append(inner == null ? " inner=null" : " day=" + inner.getDate("day") + " x=" + inner.getValue("x"));
        }
        PqStruct r = rows.getStruct(2);
        out.append(" b=").append(r.getLong("b"));
        if (!r.isNull("flag")) {
            out.append(" flag=").append(r.getBoolean("flag")).append(" f=").append(r.getFloat("f"))
                    .append(" price=").append(r.getDecimal("price")).append(" key=").append(r.getUuid("key"))
                    .append(" blob=").append(Arrays.toString(r.getBinary("blob")));
        }
        return out.toString();
    }

    /// `s` projected through `inner` alone holds no primitive of its own, so the one leaf below
    /// `inner` decides both whether `s` is null and whether `inner` is.
    @Test
    void readsAStructOfStructsAsTheNestedReaderDoes() throws Exception {
        List<String> flat = new ArrayList<>();
        List<String> nested = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader flatRows = reader.buildRowReader().projection(ColumnProjection.columns("s.inner.x")).build();
             RowReader nestedRows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("s.inner.x", "tags")).build()) {
            assertThat(flatRows).isInstanceOf(FlatRowReader.class);
            assertThat(nestedRows).isInstanceOf(NestedRowReader.class);
            while (flatRows.hasNext()) {
                flatRows.next();
                flat.add(renderInner(flatRows));
            }
            while (nestedRows.hasNext()) {
                nestedRows.next();
                nested.add(renderInner(nestedRows));
            }
        }
        assertThat(flat).hasSize(ROWS).isEqualTo(nested);
        // id 0: `s` null; id 1: `inner` null; id 2: `x` null below a present `inner`
        assertThat(flat.subList(0, 3)).containsExactly("s=null", "inner=null", "x=null");
    }

    private static String renderInner(RowReader rows) {
        PqStruct s = rows.getStruct("s");
        if (s == null) {
            return "s=null";
        }
        PqStruct inner = s.getStruct("inner");
        if (inner == null) {
            return "inner=null";
        }
        return inner.isNull("x") ? "x=null" : "x=" + inner.getDouble("x");
    }

    @Test
    void tellsANullStructFromAStructOfNullLeaves() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("s")).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);

            rows.next(); // id 0: `s` is null
            assertThat(rows.isNull("s")).isTrue();
            assertThat(rows.isNull(0)).isTrue();
            assertThat(rows.getStruct("s")).isNull();
            assertThat(rows.getValue(0)).isNull();

            rows.next(); // id 1: `s` holds a null `a` and a null `inner`
            PqStruct s = rows.getStruct("s");
            assertThat(rows.isNull("s")).isFalse();
            assertThat(s.isNull("a")).isTrue();
            assertThat(s.getString("name")).isEqualTo("n1");
            assertThat(s.isNull("inner")).isTrue();
            assertThat(s.getStruct("inner")).isNull();

            rows.next(); // id 2: `inner` is present, its leaves null
            PqStruct inner = rows.getStruct("s").getStruct("inner");
            assertThat(inner).isNotNull();
            assertThat(inner.isNull("x")).isTrue();
            assertThat(inner.getDate("day")).isNull();

            for (int id = 3; id < 6; id++) {
                rows.next();
            }
            // id 5: every leaf of `s` is null, `s` itself is not
            assertThat(rows.isNull("s")).isFalse();
            assertThat(rows.getStruct("s").toString()).isEqualTo("PqStruct{a=null, name=null, inner=null}");
        }
    }

    /// A struct held past `next()` keeps reading its own row once the reader has moved on to
    /// later batches, whose fills would otherwise reuse the arrays it reads.
    @Test
    void structsHeldAcrossBatchesReadTheirOwnRow() throws Exception {
        List<PqStruct> held = new ArrayList<>();
        List<String> whenRead = new ArrayList<>();
        // Without a pool the read decodes a batch as the reader asks for it, so each column
        // refills the holders the reader has handed back.
        try (HardwoodContext context = HardwoodContext.create(0);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs), context);
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("s", "r")).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            while (rows.hasNext()) {
                rows.next();
                PqStruct s = rows.getStruct("s");
                PqStruct r = rows.getStruct("r");
                held.add(s);
                held.add(r);
                whenRead.add(String.valueOf(s));
                whenRead.add(String.valueOf(r));
            }
        }
        List<String> afterwards = new ArrayList<>();
        for (PqStruct struct : held) {
            afterwards.add(String.valueOf(struct));
        }
        assertThat(afterwards).hasSize(2 * ROWS).isEqualTo(whenRead);
    }

    @Test
    void addressesTopLevelFieldsByTheirPositionInTheProjection() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("address", "id")).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            assertThat(rows.getFieldCount()).isEqualTo(2);
            assertThat(rows.getFieldName(0)).isEqualTo("address");
            assertThat(rows.getFieldName(1)).isEqualTo("id");

            rows.next();
            // `address` has three leaves; `id` is the second field nonetheless
            assertThat(rows.getInt(1)).isEqualTo(rows.getInt("id"));
            assertThat(rows.getStruct(0).getString("city")).isEqualTo(rows.getStruct("address").getString("city"));
        }
    }

    @Test
    void readsAStructNestedThreeDeep() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(DEEP_NESTED_STRUCT));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("account")).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);

            rows.next(); // Alice
            assertThat(rows.getStruct("account").getStruct("organization").getStruct("address").getString("city"))
                    .isEqualTo("New York");
            rows.next(); // Bob
            rows.next(); // Charlie: `organization` is null
            assertThat(rows.getStruct("account").getString("id")).isEqualTo("ACC-003");
            assertThat(rows.getStruct("account").getStruct("organization")).isNull();
            rows.next(); // Diana: `account` is null
            assertThat(rows.getStruct("account")).isNull();
        }
    }

    @Test
    void accessorOfAnotherKindFails() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id", "address")).build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            rows.next();

            assertThatThrownBy(() -> rows.getStruct("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'id' is not a struct");
            assertThatThrownBy(() -> rows.getList("address"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'address' is not a list");
            assertThatThrownBy(() -> rows.getMap(1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'address' is not a map");
            assertThatThrownBy(() -> rows.getVariant("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'id' is not a variant");
            assertThatThrownBy(() -> rows.getString("address"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'address' is a struct, which cannot be read as a string; read it with getStruct");
            assertThatThrownBy(() -> rows.getFile("address"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'address' is not annotated as FILE");
            assertThatThrownBy(() -> rows.getStruct(2))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("[nested_struct_test.parquet] Field index 2 is out of bounds for a projection of 2 fields");
            assertThatThrownBy(() -> rows.getInt("address"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Field 'address' is a struct, which cannot be read as an int; read it with getStruct");
            assertThatThrownBy(() -> rows.getLong("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_struct_test.parquet] Column 'id' is INT32, which cannot be read as a long; read it with getInt");
        }
    }

    @Test
    void filterOnAStructLeafStaysOnTheFlatReader() throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.gt("s.inner.x", 2_000.0))
                     .build()) {
            assertThat(rows).isInstanceOf(FlatRowReader.class);
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).isEqualTo(expectedIds(id -> (id % 6 == 3 && id * 0.5 > 2_000.0)
                || (id % 6 == 4 && id * 0.25 > 2_000.0)));
    }

    @Test
    void filterTestingAStructForNullIsReadByTheNestedReader() throws Exception {
        List<Long> ids = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(structs));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id", "s"))
                     .filter(FilterPredicate.isNull("s.inner"))
                     .build()) {
            // The predicate view serves the leaves below a struct, but not the struct.
            assertThat(rows).isInstanceOf(NestedRowReader.class);
            while (rows.hasNext()) {
                rows.next();
                ids.add(rows.getLong("id"));
            }
        }
        assertThat(ids).isEqualTo(expectedIds(id -> id % 6 == 0 || id % 6 == 1 || id % 6 == 5));
    }

    private static List<Long> expectedIds(IntPredicate kept) {
        List<Long> ids = new ArrayList<>();
        for (int id = 0; id < ROWS; id++) {
            if (kept.test(id)) {
                ids.add((long) id);
            }
        }
        return ids;
    }
}
