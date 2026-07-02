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

class HardwoodCommandTest {

    @Test
    void helpFlagPrintsUsage() {
        Cli.Result result = Cli.launch("--help");
        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("hardwood");
    }

    @Test
    void versionFlagPrintsVersion() {
        Cli.Result result = Cli.launch("--version");
        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("hardwood");
    }
}
