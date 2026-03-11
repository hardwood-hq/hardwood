/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class SchemaCommandTest {

    private final String FLAT_FILE = this.getClass().getResource("/plain_uncompressed.parquet").getPath();
    private final String NESTED_FILE = this.getClass().getResource("/nested_struct_test.parquet").getPath();

    @Test
    void displaysNativeSchemaByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", FLAT_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("message")
                .contains("id")
                .contains("value");
    }

    @Test
    void displaysAvroSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", FLAT_FILE, "--format", "AVRO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("\"type\": \"record\"")
                .contains("\"fields\"")
                .contains("\"name\": \"id\"")
                .contains("\"name\": \"value\"");
    }

    @Test
    void displaysProtoSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", FLAT_FILE, "--format", "PROTO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("syntax = \"proto3\"")
                .contains("message")
                .contains("id")
                .contains("value");
    }

    @Test
    void displaysAvroSchemaForNestedFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", NESTED_FILE, "--format", "AVRO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).contains("\"type\": \"record\"");
    }

    @Test
    void displaysProtoSchemaForNestedFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", NESTED_FILE, "--format", "PROTO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("syntax = \"proto3\"")
                .contains("message");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }
}
