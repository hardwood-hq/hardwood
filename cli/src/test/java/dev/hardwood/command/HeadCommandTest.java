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
class HeadCommandTest implements HeadCommandContract {

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
        String logicalTypesFile = getClass().getResource("/logical_types_test.parquet").getPath();
        LaunchResult result = launcher.launch("head", "-f", logicalTypesFile);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Alice")
                .contains("Bob")
                .contains("Charlie");
    }

    @Test
    void displaysNestedStructStringFieldsAsText(QuarkusMainLauncher launcher) {
        String deepNestedFile = getClass().getResource("/deep_nested_struct_test.parquet").getPath();
        LaunchResult result = launcher.launch("head", "-f", deepNestedFile);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("ACC-001")
                .contains("ACC-002")
                .contains("ACC-003");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("head", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
    }
}
