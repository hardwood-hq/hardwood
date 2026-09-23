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

    String multiRowGroupIntFile();

    String nonexistentFile();

    String fidelityFile();

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
                  {"id":1,"value":100},
                  {"id":2,"value":200},
                  {"id":3,"value":300}
                ]""");
    }

    @Test
    default void jsonPreservesScalarTypesAndNulls() {
        Cli.Result result = Cli.launch("convert", "-f", fidelityFile(), "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"flag":true,"small":10,"large":100,"single":1.5,"double":2.5,"text":"null"},
                  {"id":2,"flag":false,"small":null,"large":-3,"single":"NaN","double":"Infinity","text":null},
                  {"id":3,"flag":null,"small":2147483647,"large":null,"single":"Infinity","double":"NaN","text":"literal"},
                  {"id":4,"flag":true,"small":-1,"large":-5,"single":"-Infinity","double":-2.5,"text":null}
                ]""");
    }

    @Test
    default void csvUsesConfiguredNullStringAndQuotesIt() {
        Cli.Result result = Cli.launch("convert", "-f", fidelityFile(), "--format", "csv",
                "--null-string", "NULL,VALUE");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,flag,small,large,single,double,text
                1,true,10,100,1.5,2.5,null
                2,false,"NULL,VALUE",-3,NaN,Infinity,"NULL,VALUE"
                3,"NULL,VALUE",2147483647,"NULL,VALUE",Infinity,NaN,literal
                4,true,-1,-5,-Infinity,-2.5,"NULL,VALUE\"""");
    }

    @Test
    default void csvUsesEmptyFieldForNullByDefault() {
        Cli.Result result = Cli.launch("convert", "-f", fidelityFile(), "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,flag,small,large,single,double,text
                1,true,10,100,1.5,2.5,null
                2,false,,-3,NaN,Infinity,
                3,,2147483647,,Infinity,NaN,literal
                4,true,-1,-5,-Infinity,-2.5,""");
    }

    @Test
    default void csvNullStringAppliesToFlattenedStructLeaves() {
        Cli.Result result = Cli.launch("convert", "-f", deepNestedFile(), "--format", "csv",
                "--null-string", "\\N");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                customer_id,name,account.id,account.organization.name,account.organization.address.street,account.organization.address.city,account.organization.address.zip
                1,Alice,ACC-001,Acme Corp,123 Main St,New York,10001
                2,Bob,ACC-002,TechStart,\\N,\\N,\\N
                3,Charlie,ACC-003,\\N,\\N,\\N,\\N
                4,Diana,\\N,\\N,\\N,\\N,\\N""");
    }

    @Test
    default void csvProjectingANestedLeafEmitsOnlyThatColumn() {
        Cli.Result result = Cli.launch("convert", "-f", deepNestedFile(), "--format", "csv",
                "--columns", "account.organization.address.zip", "--null-string", "\\N");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                account.organization.address.zip
                10001
                \\N
                \\N
                \\N""");
    }

    @Test
    default void nullStringIsRejectedForJson() {
        Cli.Result result = Cli.launch("convert", "-f", fidelityFile(), "--format", "json",
                "--null-string", "NULL");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("--null-string applies to CSV output only");
    }

    @Test
    default void jsonUsesBareNullForNullNestedFields() {
        Cli.Result structResult = Cli.launch("convert", "-f", deepNestedFile(), "--format", "json");
        Cli.Result listResult = Cli.launch("convert", "-f", listFile(), "--format", "json");

        assertThat(structResult.exitCode()).isZero();
        assertThat(structResult.output()).contains("\"account\":null");
        assertThat(listResult.exitCode()).isZero();
        assertThat(listResult.output()).contains("\"tags\":null").contains("\"scores\":null");
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
                Bob,ACC-002,TechStart,,,
                Charlie,ACC-003,,,,
                Diana,,,,,""");
    }

    @Test
    default void csvWithListColumns() {
        Cli.Result result = Cli.launch("convert", "-f", listFile(), "--format", "csv");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,tags,scores
                1,"[""a"", ""b"", ""c""]","[10, 20, 30]"
                2,[],[100]
                3,,"[1, 2]"
                4,"[""single""]",""");
    }

    @Test
    default void jsonWritesListsAsNativeArrays() {
        Cli.Result result = Cli.launch("convert", "-f", listFile(), "--format", "json");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"tags":["a", "b", "c"],"scores":[10, 20, 30]},
                  {"id":2,"tags":[],"scores":[100]},
                  {"id":3,"tags":null,"scores":[1, 2]},
                  {"id":4,"tags":["single"],"scores":null}
                ]""");
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
    default void head() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                1,100""");
    }

    @Test
    default void tail() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "-1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                3,300""");
    }

    @Test
    default void tailOnMultipleRowGroups() {
        // filter_pushdown_int.parquet has three row groups of 100 rows each.
        // The tail must reflect the last rows of the file regardless of the
        // row-group layout; this also exercises the code path that skips
        // row groups outside the tail.
        Cli.Result result = Cli.launch("convert", "-f", multiRowGroupIntFile(), "--format", "csv", "-n", "-3");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value,label
                298,298,rg3_298
                299,299,rg3_299
                300,300,rg3_300""");
    }

    @Test
    default void headJsonOutput() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "json", "-n", "1");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                [
                  {"id":1,"value":100}
                ]""");
    }

    @Test
    default void explicitAllConvertsEveryRow() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "ALL");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                1,100
                2,200
                3,300""");
    }

    @Test
    default void skipStartsAtTheGivenRow() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "--skip", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id,value
                3,300""");
    }

    @Test
    default void rowGroupConvertsThatGroupOnly() {
        // filter_pushdown_int.parquet holds three row groups of 100 rows each.
        Cli.Result result = Cli.launch("convert", "-f", multiRowGroupIntFile(), "--format", "csv",
                "-c", "id", "--row-group", "2", "-n", "2");

        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).isEqualTo("""
                id
                201
                202""");
    }

    @Test
    default void skipAndRowGroupAreRefusedTogether() {
        Cli.Result result = Cli.launch("convert", "-f", multiRowGroupIntFile(), "--format", "csv",
                "--skip", "1", "--row-group", "1");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("--skip and --row-group cannot be combined");
    }

    @Test
    default void rejectsNonIntegerRowLimit() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "abc");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("Invalid value for option '-n'");
    }

    @Test
    default void rejectsZeroRowLimit() {
        Cli.Result result = Cli.launch("convert", "-f", plainFile(), "--format", "csv", "-n", "0");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.errorOutput()).contains("Invalid value for option '-n'");
    }

}
