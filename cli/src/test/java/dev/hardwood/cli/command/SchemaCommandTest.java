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

class SchemaCommandTest implements SchemaCommandContract {

    private final String NESTED_FILE = this.getClass().getResource("/nested_struct_test.parquet").getPath();

    private final String VARIANT_FILE = this.getClass().getResource("/variant_test.parquet").getPath();

    private final String VARIANT_SHREDDED_FILE = this.getClass().getResource("/variant_shredded_test.parquet").getPath();

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void displaysAvroSchemaForNestedFile() {
        Cli.Result result = Cli.launch("schema", "-f", NESTED_FILE, "--format", "AVRO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("\"type\": \"record\"");
    }

    @Test
    void displaysProtoSchemaForNestedFile() {
        Cli.Result result = Cli.launch("schema", "-f", NESTED_FILE, "--format", "PROTO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output())
                .contains("syntax = \"proto3\"")
                .contains("message");
    }

    @Test
    void rejectsRemoteUri() {
        Cli.Result result = Cli.launch("schema", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("not implemented yet");
    }

    @Test
    void displaysVariantAnnotation() {
        Cli.Result result = Cli.launch("schema", "-f", VARIANT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                message schema {
                  required int32 id;
                  optional group var (VARIANT(1)) {
                    required byte_array metadata;
                    required byte_array value;
                  }
                }""");
    }

    @Test
    void displaysShreddedVariantAnnotationWithTypedValueChild() {
        Cli.Result result = Cli.launch("schema", "-f", VARIANT_SHREDDED_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                message schema {
                  required int32 id;
                  optional group var (VARIANT(1)) {
                    required byte_array metadata;
                    optional byte_array value;
                    optional int64 typed_value;
                  }
                }""");
    }
}
