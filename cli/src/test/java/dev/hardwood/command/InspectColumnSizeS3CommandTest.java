/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusMainTest
class InspectColumnSizeS3CommandTest extends AbstractS3CommandTest {

    @Test
    void displaysRankedColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("Rank")
                .contains("Column")
                .contains("Compressed")
                .contains("Uncompressed")
                .contains("Ratio");
    }

    @Test
    void listsAllColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput())
                .contains("id")
                .contains("value");
    }

    @Test
    void rank1IsLargestCompressedColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", S3_FILE);

        assertThat(result.exitCode()).isZero();
        List<String> dataRows = result.getOutput().lines()
                .filter(line -> line.startsWith("|") && !line.contains("Rank"))
                .toList();
        assertThat(dataRows).isNotEmpty();
        assertThat(dataRows.getFirst().split("\\|")[1].strip()).isEqualTo("1");
    }

    @Test
    void failsOnNonexistentS3File(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("inspect", "column-size", "-f", S3_NONEXISTENT_FILE);

        assertThat(result.exitCode()).isNotZero();
    }
}
