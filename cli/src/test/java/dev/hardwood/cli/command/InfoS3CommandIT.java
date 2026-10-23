/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class InfoS3CommandIT extends AbstractS3CommandIT implements InfoCommandContract {

    @Override
    public String plainFile() {
        return S3_FILE;
    }

    @Override
    public String nonexistentFile() {
        return S3_NONEXISTENT_FILE;
    }

    @Override
    public String kvMetadataFile() {
        return S3_KV_METADATA_FILE;
    }

    /// `--log-file` on a non-interactive command records each S3 fetch, which is what it is
    /// for: seeing what a read over the network did. stderr stays free of them.
    @Test
    void logFileRecordsEachFetch(@TempDir Path tempDir) throws IOException {
        Path logFile = tempDir.resolve("info.log");

        Cli.Result result = Cli.launch("info", "-f", S3_FILE, "--log-file", logFile.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.errorOutput()).isEmpty();
        assertThat(Files.readAllLines(logFile)).anySatisfy(line -> assertThat(line)
                .contains(" FINE [dev.hardwood.s3.S3Fetcher] ")
                .contains(" fetch #1 "));
    }
}
