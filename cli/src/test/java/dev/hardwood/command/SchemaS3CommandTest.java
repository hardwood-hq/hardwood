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
class SchemaS3CommandTest extends AbstractS3CommandTest {

    @Test
    void displaysNativeSchemaByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("message")
                .contains("id")
                .contains("value");
    }

    @Test
    void displaysAvroSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", S3_FILE, "--format", "AVRO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("\"type\": \"record\"")
                .contains("\"fields\"")
                .contains("\"name\": \"id\"")
                .contains("\"name\": \"value\"");
    }

    @Test
    void displaysProtoSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", S3_FILE, "--format", "PROTO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("syntax = \"proto3\"")
                .contains("message")
                .contains("id")
                .contains("value");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
