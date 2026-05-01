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

/// Shared test contract for the `convert` command.
interface ConvertCommandContract {

    String plainFile();

    String deepNestedFile();

    String listFile();

    String nonexistentFile();

    @Test
    default void csvOutput() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                1,100
                2,200
                3,300""");
    }

    @Test
    default void jsonOutput() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":"1","value":"100"},
                  {"id":"2","value":"200"},
                  {"id":"3","value":"300"}
                ]""");
    }

    @Test
    default void csvColumnsFilter() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "--columns", "id");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id
                1
                2
                3""");
    }

    @Test
    default void csvWithNestedStructColumns() {
        Cli.Result result = Cli.launch("convert", "-f", deepNestedFile(), "--format", "csv", "--columns", "customer_id,name");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                customer_id,name
                1,Alice
                2,Bob
                3,Charlie
                4,Diana""");
    }

    @Test
    default void csvFlattensNestedStructs() {
        Cli.Result result = Cli.launch("convert", "-f", deepNestedFile(), "--format", "csv", "--columns", "name,account");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                name,account.id,account.organization.name,account.organization.address.street,account.organization.address.city,account.organization.address.zip
                Alice,ACC-001,Acme Corp,123 Main St,New York,10001
                Bob,ACC-002,TechStart,null,null,null
                Charlie,ACC-003,null,null,null,null
                Diana,null,null,null,null,null""");
    }

    @Test
    default void csvWithListColumns() {
        Cli.Result result = Cli.launch("convert", "-f", listFile(), "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,tags,scores
                1,"[a, b, c]","[10, 20, 30]"
                2,[],[100]
                3,null,"[1, 2]"
                4,[single],null""");
    }

    @Test
    default void rejectsUnknownColumn() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "--columns", "unknown");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void failsOnNonexistentFile() {
        Cli.Result result = Cli.launch("convert", "-f", nonexistentFile(), "--format", "csv");

        assertThat(result.exitCode()).isNotZero();
    }

    @Test
    default void explicitOneShowsFirstRow(){
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                1,100""");
    }

    @Test
    default void explicitNegativeOneShowsLastRow(){
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "-1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                3,300""");
    }

    @Test
    default void explicitAllShowsEveryRow(){
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "ALL");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                1,100
                2,200
                3,300""");
    }

}
