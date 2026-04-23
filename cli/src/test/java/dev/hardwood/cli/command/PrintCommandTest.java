/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PrintCommandTest implements PrintCommandContract {

    private final String VARIANT_FILE = getClass().getResource("/variant_test.parquet").getPath();

    private final String VARIANT_SHREDDED_FILE = getClass().getResource("/variant_shredded_test.parquet").getPath();

    private final String VARIANT_ATTRIBUTES_FILE = getClass().getResource("/variant_attributes_example.parquet").getPath();

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String byteArrayFile() {
        return getClass().getResource("/delta_byte_array_test.parquet").getPath();
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
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Override
    public String unsignedIntFile() {
        return getClass().getResource("/unsigned_int_test.parquet").getPath();
    }

    @Override
    public String multiRowGroupIntFile() {
        return getClass().getResource("/filter_pushdown_int.parquet").getPath();
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("print", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).isEqualTo("Remote URIs are not implemented yet.");
    }

    @Test
    void rendersUnshreddedVariantValuesAsDecodedScalars() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+-------+
                | id | var   |
                +----+-------+
                | 1  | true  |
                | 2  | false |
                | 3  | 42    |
                | 4  | "hi"  |
                +----+-------+""");
    }

    @Test
    void rendersShreddedVariantValuesAsDecodedScalars() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_SHREDDED_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+---------------+
                | id | var           |
                +----+---------------+
                | 1  | 42            |
                | 2  | true          |
                | 3  | null          |
                | 4  | 1000000000000 |
                +----+---------------+""");
    }

    @Test
    void rendersVariantObjectAsJsonLikeText() {
        Cli.Result result = Cli.launch("print", "-f", VARIANT_ATTRIBUTES_FILE, "-mw", "120");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                +----+-------------+-----------------------------------+
                | id | name        | value                             |
                +----+-------------+-----------------------------------+
                | 1  | age         | 42                                |
                | 1  | email       | "ada@example.com"                 |
                | 1  | preferences | {"opt_in": true, "theme": "dark"} |
                +----+-------------+-----------------------------------+""");
    }
}
