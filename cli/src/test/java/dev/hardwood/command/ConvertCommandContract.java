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

import static org.assertj.core.api.Assertions.assertThat;

/// Shared test contract for the `convert` command.
interface ConvertCommandContract {

    String plainFile();

    String deepNestedFile();

    String listFile();

    String nonexistentFile();

    @Test
    default void csvOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id,value
                1,100
                2,200
                3,300""");
    }

    @Test
    default void jsonOutput(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                [
                  {"id":"1","value":"100"},
                  {"id":"2","value":"200"},
                  {"id":"3","value":"300"}
                ]""");
    }

    @Test
    default void csvColumnsFilter(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv", "--columns", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id
                1
                2
                3""");
    }

    @Test
    default void csvWithNestedStructColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", deepNestedFile(), "--to", "csv", "--columns", "customer_id,name");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                customer_id,name
                1,Alice
                2,Bob
                3,Charlie
                4,Diana""");
    }

    @Test
    default void csvFlattensNestedStructs(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", deepNestedFile(), "--to", "csv", "--columns", "name,account");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                name,account.id,account.organization.name,account.organization.address.street,account.organization.address.city,account.organization.address.zip
                Alice,ACC-001,Acme Corp,123 Main St,New York,10001
                Bob,ACC-002,TechStart,null,null,null
                Charlie,ACC-003,null,null,null,null
                Diana,null,null,null,null,null""");
    }

    @Test
    default void csvWithListColumns(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", listFile(), "--to", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.getOutput()).isEqualTo("""
                id,tags,scores
                1,"[a, b, c]","[10, 20, 30]"
                2,[],[100]
                3,null,"[1, 2]"
                4,[single],null""");
    }

    @Test
    default void rejectsUnknownColumn(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", plainFile(), "--to", "csv", "--columns", "unknown");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void failsOnNonexistentFile(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("convert", "-f", nonexistentFile(), "--to", "csv");

        assertThat(result.exitCode()).isNotZero();
    }
}
