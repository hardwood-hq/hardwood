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
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class PrintCommandTest implements PrintCommandContract {

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
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("print", "-f", "gs://bucket/data.parquet");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).isEqualTo("Remote URIs are not implemented yet.");
    }
}
