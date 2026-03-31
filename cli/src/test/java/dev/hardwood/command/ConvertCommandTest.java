/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class ConvertCommandTest implements ConvertCommandContract {

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
    public String nonexistentFile() {
        return "nonexistent.parquet";
    }

    @Test
    void outputToFile(@TempDir Path tempDir, QuarkusMainLauncher launcher) throws IOException {
        Path out = tempDir.resolve("output.csv");

        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv", "-o", out.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(Files.readString(out))
                .startsWith("id,value")
                .contains("1,100");
    }

    @Test
    void rejectsRemoteUri(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", "gs://bucket/data.parquet", "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.getErrorOutput()).contains("not implemented yet");
    }

    @Test
    void requiresFormatFlag(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile());

        assertThat(result.exitCode()).isNotZero();
    }
}
