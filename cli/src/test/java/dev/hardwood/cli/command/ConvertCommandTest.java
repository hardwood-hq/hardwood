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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

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
                  {"id":"1","var":true},
                  {"id":"2","var":false},
                  {"id":"3","var":42},
                  {"id":"4","var":"hi"}
                ]""");
    }

    @Test
    void jsonEmitsShreddedVariantAsNativeJsonScalars() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_SHREDDED_FILE, "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":"1","var":42},
                  {"id":"2","var":true},
                  {"id":"3","var":null},
                  {"id":"4","var":1000000000000}
                ]""");
    }

    @Test
    void jsonEmitsVariantObjectAsInlineJson() {
        Cli.Result result = Cli.launch("convert", "-f", VARIANT_ATTRIBUTES_FILE, "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":"1","name":"age","value":42},
                  {"id":"1","name":"email","value":"ada@example.com"},
                  {"id":"1","name":"preferences","value":{"opt_in": true, "theme": "dark"}}
                ]""");
    }
}
