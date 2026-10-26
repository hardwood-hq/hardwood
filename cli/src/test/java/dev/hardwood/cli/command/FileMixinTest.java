/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Resolves regions against isolated profile files, without S3 or instance metadata.
class FileMixinTest {

    private static final String[] PROPERTIES = {
            "aws.region", "aws.profile", "aws.configFile", "aws.sharedCredentialsFile" };

    private final Map<String, String> savedProperties = new HashMap<>();

    @TempDir
    Path dir;

    @BeforeEach
    void configureProfileFiles() {
        for (String property : PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                savedProperties.put(property, value);
            }
        }
        assumeTrue(System.getenv().keySet().stream().noneMatch(name -> name.startsWith("AWS_")),
                "AWS_ environment variables are set");
        for (String property : PROPERTIES) {
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
    void regionFromCustomConfigFile() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");

        assertThat(FileMixin.resolveRegion()).isEqualTo("us-west-2");
    }

    @Test
    void profileFromSystemProperty() throws IOException {
        Files.writeString(dir.resolve("config"),
                "[default]\nregion = us-east-1\n[profile work]\nregion = eu-west-1\n");
        System.setProperty("aws.profile", "work");

        assertThat(FileMixin.resolveRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void systemPropertyOverridesProfileRegion() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");
        System.setProperty("aws.region", "ap-south-1");

        assertThat(FileMixin.resolveRegion()).isEqualTo("ap-south-1");
    }

    @Test
    void missingProfileDoesNotUseDefaultRegion() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");
        System.setProperty("aws.profile", "missing");

        assertThat(FileMixin.resolveRegion()).isNull();
    }

    @Test
    void missingFilesHaveNoRegion() {
        assertThat(FileMixin.resolveRegion()).isNull();
    }

    @Test
    void emptyFilesHaveNoRegion() throws IOException {
        Files.writeString(dir.resolve("config"), "");
        Files.writeString(dir.resolve("credentials"), "");

        assertThat(FileMixin.resolveRegion()).isNull();
    }

    @Test
    void regionKeyMustMatchExactly() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion_name = us-west-2\n");

        assertThat(FileMixin.resolveRegion()).isNull();
    }

    @Test
    void profileFromEnvironment() throws Exception {
        Files.writeString(dir.resolve("config"),
                "[default]\nregion = us-east-1\n[profile work]\nregion = eu-west-1\n");

        assertThat(regionFromEnvironment(Map.of("AWS_PROFILE", "work"))).isEqualTo("eu-west-1");
    }

    @ParameterizedTest
    @ValueSource(strings = { "AWS_REGION", "AWS_DEFAULT_REGION" })
    void environmentRegionOverridesProfile(String variable) throws Exception {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");

        assertThat(regionFromEnvironment(Map.of(variable, "ap-south-1"))).isEqualTo("ap-south-1");
    }

    @Test
    void configFileFromEnvironment() throws Exception {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");

        assertThat(regionFromEnvironment(Map.of())).isEqualTo("us-west-2");
    }

    private String regionFromEnvironment(Map<String, String> settings) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), RegionProcess.class.getName());
        String path = builder.environment().get("PATH");
        builder.environment().clear();
        if (path != null) {
            builder.environment().put("PATH", path);
        }
        builder.environment().put("AWS_CONFIG_FILE", dir.resolve("config").toString());
        builder.environment().put("AWS_SHARED_CREDENTIALS_FILE", dir.resolve("credentials").toString());
        builder.environment().putAll(settings);

        Process process = builder.start();
        boolean finished = process.waitFor(60, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        assertThat(finished).withFailMessage("Process timed out after 60s").isTrue();
        assertThat(process.exitValue()).isZero();
        return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
    }

    public static class RegionProcess {

        public static void main(String[] args) {
            System.out.println(FileMixin.resolveRegion());
        }
    }

}
