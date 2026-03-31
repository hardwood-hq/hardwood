/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `schema` command.
interface SchemaCommandContract {

    String plainFile();

    String nonexistentFile();

    @Test
    default void displaysNativeSchemaByDefault(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", plainFile());

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                message schema {
                  required int64 id;
                  required int64 value;
                }""");
    }

    @Test
    default void displaysAvroSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", plainFile(), "--format", "AVRO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                {
                  "type": "record",
                  "name": "Schema",
                  "fields": [
                    { "name": "id", "type": "long" },
                    { "name": "value", "type": "long" }
                  ]
                }""");
    }

    @Test
    default void displaysProtoSchema(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", plainFile(), "--format", "PROTO");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                syntax = "proto3";

                message Schema {
                  int64 id = 1;
                  int64 value = 2;
                }""");
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("schema", "-f", nonexistentFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
