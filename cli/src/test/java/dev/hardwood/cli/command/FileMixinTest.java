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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// How an `s3://` command reports settings it cannot use, before it sends any request.
/// [S3SettingsTest] covers the resolution itself.
class FileMixinTest {

    private static final String[] PROPERTIES = {
            "aws.region", "aws.profile", "aws.configFile", "aws.sharedCredentialsFile",
            "aws.endpointUrl", "aws.endpointUrlS3", "aws.ignoreConfiguredEndpointUrls" };

    private final Map<String, String> savedProperties = new HashMap<>();

    @TempDir
    Path dir;

    @BeforeEach
    void isolateFromTheHostsAwsSettings() {
        // A command reads this process's environment, which a test cannot change
        assumeTrue(System.getenv().keySet().stream().noneMatch(name -> name.startsWith("AWS_")),
                "AWS_ environment variables are set");
        for (String property : PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                savedProperties.put(property, value);
            }
            System.clearProperty(property);
        }
        System.setProperty("aws.configFile", dir.resolve("config").toString());
        System.setProperty("aws.sharedCredentialsFile", dir.resolve("credentials").toString());
    }

    @AfterEach
    void restoreProperties() {
        for (String property : PROPERTIES) {
            System.clearProperty(property);
        }
        savedProperties.forEach(System::setProperty);
    }

    @Test
    void unparseableConfigFileFailsTheCommandWithOneLine() throws IOException {
        Files.writeString(dir.resolve("config"), "region = us-west-2\n[default]\n");

        Cli.Result result = Cli.launch("info", "-f", "s3://bucket/data.parquet");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.errorOutput()).isEqualTo("Unable to read the AWS profile files " + dir.resolve("config")
                + " and " + dir.resolve("credentials") + ": Expected a profile definition on line 1");
    }

    @Test
    void missingRegionNamesTheSelectedProfileAndConfigFile() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-east-1\n[profile work]\n");
        System.setProperty("aws.profile", "work");

        Cli.Result result = Cli.launch("info", "-f", "s3://bucket/data.parquet");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.errorOutput()).isEqualTo(
                "Unable to determine AWS region. Set AWS_REGION, or region for profile 'work' in " + dir.resolve("config")
                        + ". For an S3-compatible service, set AWS_ENDPOINT_URL or endpoint_url in that profile instead.");
    }

    @Test
    void invalidEndpointFailsTheCommandNamingItsSource() {
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.endpointUrl", "localhost:9000");

        Cli.Result result = Cli.launch("info", "-f", "s3://bucket/data.parquet");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.errorOutput())
                .isEqualTo("system property aws.endpointUrl must be an http or https URL with a valid host name, "
                        + "was 'localhost:9000'");
    }
}
