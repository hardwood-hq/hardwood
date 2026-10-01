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

/// A record Hardwood logs while a command runs reaches the command's stderr as one
/// `LEVEL: message` line.
class CliLoggingTest {

    @Test
    void aWarningIsOneLineOnStderr() {
        Cli.Result result = Cli.launch("schema", "-f",
                getClass().getResource("/annotated_repeated_group_test.parquet").getPath());

        assertThat(result.exitCode()).isZero();
        assertThat(result.errorOutput()).isEqualTo("WARNING: Ignoring 6 annotation(s) on repeated groups outside"
                + " a LIST or MAP group; those groups are read as though unannotated: foo_mkv (MAP_KEY_VALUE);"
                + " foo_list (LIST); foo_list_lt (LIST); foo_map (MAP); foo_map_ct (MAP); s.bar (LIST)");
    }
}
