/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConvertCommandTest implements ConvertCommandContract {

    private final String VARIANT_FILE = getClass().getResource("/variant_test.parquet").getPath();

    private final String VARIANT_SHREDDED_FILE = getClass().getResource("/variant_shredded_test.parquet").getPath();

    private final String VARIANT_ATTRIBUTES_FILE = getClass().getResource("/variant_attributes_example.parquet").getPath();

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String deepNestedFile() {
        return getClass().getResource("/deep_nested_struct_test.parquet").getPath();
    }

    @Override
    public String listFile() {
        return getClass().getResource("/list_basic_test.parquet").getPath();
    }

    @Override
    public String multiRowGroupIntFile() {
        return getClass().getResource("/filter_pushdown_int.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Override
    public String fidelityFile() {
        return getClass().getResource("/convert_fidelity_test.parquet").getPath();
    }

    @Test
    void jsonRendersUnsignedIntegersAsNumbers() {
        Cli.Result result = Cli.launch("convert", "-f", getClass().getResource("/unsigned_int_test.parquet").getPath(),
                "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("{\"id\":1,\"uint32_val\":0,\"uint64_val\":0}")
                .contains("{\"id\":3,\"uint32_val\":4294967295,\"uint64_val\":18446744073709551615}");
    }

    @Test
    void jsonKeepsRepeatedPrimitiveAsString() {
        Cli.Result result = Cli.launch("convert", "-f",
                getClass().getResource("/unannotated_repeated_primitive_test.parquet").getPath(),
                "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("\"foo\":\"[42, 7]\"");
    }

    @Test
    void jsonKeepsAnnotatedLogicalTypesAsStrings() {
        Cli.Result result = Cli.launch("convert", "-f", getClass().getResource("/logical_types_test.parquet").getPath(),
                "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("\"name\":\"Alice\"")
                .contains("\"birth_date\":\"1990-01-15\"")
                .contains("\"created_at_millis\":\"")
                .contains("\"wake_time_micros\":\"")
                .contains("\"balance\":\"1234.56\"")
                .contains("\"account_id\":\"12345678-1234-5678-1234-567812345678\"")
                .contains("\"profile_json\":\"");
    }

    @Test
    void jsonRendersIntAnnotatedColumnsAsNumbers() {
        Cli.Result result = Cli.launch("convert", "-f", getClass().getResource("/logical_types_test.parquet").getPath(),
                "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("\"tiny_int\":10")
                .contains("\"small_int\":1000")
                .contains("\"big_int\":10000000000")
                .contains("\"tiny_uint\":255")
                .contains("\"big_uint\":9223372036854775807");
    }

    @Test
    void jsonKeepsInt96IntervalAndFloat16AsStrings() {
        Cli.Result int96 = Cli.launch("convert", "-f",
                getClass().getResource("/int96_timestamp_test.parquet").getPath(), "--format", "json");
        Cli.Result interval = Cli.launch("convert", "-f",
                getClass().getResource("/interval_logical_type_test.parquet").getPath(), "--format", "json");
        Cli.Result float16 = Cli.launch("convert", "-f",
                getClass().getResource("/float16_logical_type_test.parquet").getPath(), "--format", "json");

        assertThat(int96.exitCode()).isZero();
        assertThat(int96.output()).contains("\"ts\":\"");
        assertThat(interval.exitCode()).isZero();
        assertThat(interval.output()).contains("\"duration\":\"").contains("\"duration\":null");
        assertThat(float16.exitCode()).isZero();
        assertThat(float16.output()).contains("\"half\":\"").contains("\"half\":null");
    }

    @Test
    void variantNullValueIsDistinctFromANullVariantColumn() {
        String file = getClass().getResource("/convert_variant_null_test.parquet").getPath();

        Cli.Result csv = Cli.launch("convert", "-f", file, "--format", "csv", "--null-string", "\\N");
        Cli.Result json = Cli.launch("convert", "-f", file, "--format", "json");

        assertThat(csv.exitCode()).isZero();
        assertThat(csv.output()).isEqualTo("""
                id,var
                1,42
                2,\\N
                3,null""");
        assertThat(json.exitCode()).isZero();
        assertThat(json.output()).isEqualTo("""
                [
                  {"id":1,"var":42},
                  {"id":2,"var":null},
                  {"id":3,"var":null}
                ]""");
    }

    @Test
    void csvFlattenRejectsStructFieldThatIsNotAStruct() {
        SchemaNode.PrimitiveNode child = new SchemaNode.PrimitiveNode("id", PhysicalType.INT32,
                RepetitionType.OPTIONAL, null, 0, 2, 0);
        SchemaNode.GroupNode account = new SchemaNode.GroupNode("account", RepetitionType.OPTIONAL, null, null,
                List.of(child), 1, 0);
        List<String> values = new ArrayList<>();

        assertThatThrownBy(() -> ConvertCommand.flattenValues("not a struct", account, "account",
                ColumnProjection.all(), values, ""))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Field 'account' is a struct in the schema, but the reader returned a "
                                 + "java.lang.String")
                        ;
        assertThat(values).isEmpty();
    }


    private String nestedBinaryFile() {
        return getClass().getResource("/nested_binary_test.parquet").getPath();
    }

    /// A CSV cell has to carry the payload, not describe it — a byte count
    /// cannot be read back. The nested positions reach the renderer through
    /// the list / struct / map branches rather than the top-level leaf one.
    @Test
    void csvExportsNestedBinaryAsHex() {
        Cli.Result result = Cli.launch("convert", "-f", nestedBinaryFile(), "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("0x010100000000000000005366c0f71622f0fa1955c0")
                .contains("0x0101000000f71622f0fa1955c000000000005366c0")
                .doesNotContain("<21 bytes>");
    }

    /// Same contract for JSON, which renders a list / struct / map whole
    /// rather than flattening it into one column per leaf as CSV does.
    @Test
    void jsonExportsNestedBinaryAsHex() {
        Cli.Result result = Cli.launch("convert", "-f", nestedBinaryFile(), "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("0x010100000000000000005366c0f71622f0fa1955c0")
                .contains("0x0101000000f71622f0fa1955c000000000005366c0")
                .doesNotContain("<21 bytes>");
    }

    @Test
    void outputToFile(@TempDir Path tempDir) throws IOException {
        Path out = tempDir.resolve("output.csv");

        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-o", out.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(Files.readString(out))
                .startsWith("id,value")
                .contains("1,100");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("convert", "-f", "gs://bucket/data.parquet", "--format", "csv");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }

    @Test
    void requiresFormatFlag() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile());

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    void csvEmitsVariantAsSingleColumnWithDecodedValues() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_FILE, "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,var
                1,true
                2,false
                3,42
                4,\"\"\"hi\"\"\"""");
    }

    @Test
    void csvEmitsShreddedVariantAsSingleColumn() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_SHREDDED_FILE, "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,var
                1,42
                2,true
                3,null
                4,1000000000000""");
    }

    @Test
    void csvEmitsVariantObjectAsJsonStringInOneCell() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_ATTRIBUTES_FILE, "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,name,value
                1,age,42
                1,email,\"\"\"ada@example.com\"\"\"
                1,preferences,\"{\"\"opt_in\"\": true, \"\"theme\"\": \"\"dark\"\"}\"""");
    }

    @Test
    void jsonEmitsVariantAsNativeJsonSubtree() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_FILE, "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"var":true},
                  {"id":2,"var":false},
                  {"id":3,"var":42},
                  {"id":4,"var":"hi"}
                ]""");
    }

    @Test
    void jsonEmitsShreddedVariantAsNativeJsonScalars() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_SHREDDED_FILE, "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"var":42},
                  {"id":2,"var":true},
                  {"id":3,"var":null},
                  {"id":4,"var":1000000000000}
                ]""");
    }

    @Test
    void jsonEmitsVariantObjectAsInlineJson() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_ATTRIBUTES_FILE, "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"name":"age","value":42},
                  {"id":1,"name":"email","value":"ada@example.com"},
                  {"id":1,"name":"preferences","value":{"opt_in": true, "theme": "dark"}}
                ]""");
    }
}
