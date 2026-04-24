/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Smoke test for the `hardwood dive` subcommand: verifies that it wires up end
/// to end by rendering one frame into a memory buffer and exiting 0. Does not
/// exercise interactive keyboard input — that's layer-1 state tests.
class DiveCommandTest {

    @Test
    void smokeRenderExitsZero() {
        Path fixture = Path.of(getClass().getResource("/compat_plain_int64.parquet").getPath());

        Cli.Result result = Cli.launch("dive", "-f", fixture.toString(), "--smoke-render");

        assertThat(result.exitCode())
                .withFailMessage("smoke-render failed: stdout=%s | stderr=%s", result.output(), result.errorOutput())
                .isZero();
    }

    @Test
    void rejectsMissingFileFlag() {
        Cli.Result result = Cli.launch("dive");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).containsIgnoringCase("file");
    }
}
