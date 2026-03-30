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
class TailCommandTest implements TailCommandContract {

    private final String LOGICAL_TYPES_FILE = this.getClass().getResource("/logical_types_test.parquet").getPath();

    @Override
    public String plainFile() {
        return getClass().getResource("/plain_uncompressed.parquet").getPath();
    }

    @Override
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void displaysStringColumnsAsText(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", LOGICAL_TYPES_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Alice")
                .contains("Bob")
                .contains("Charlie");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("tail", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }
}
