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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import software.amazon.awssdk.profiles.ProfileFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Resolves region and endpoint against profile files in a temporary directory, read through
/// [ProfileFile#defaultProfileFile] as in a command, with system properties and environment
/// variables supplied by a map. The nested `ThroughTheProcess` tests check the wiring to this
/// process's real system properties and environment in a child JVM.
class S3SettingsTest {

    /// The SDK settings naming the profile files, pointed at the temporary directory.
    private static final String[] FILE_PROPERTIES = { "aws.configFile", "aws.sharedCredentialsFile" };

    private final Map<String, String> savedProperties = new HashMap<>();
    private final Map<String, String> properties = new HashMap<>();
    private final Map<String, String> variables = new HashMap<>();

    @TempDir
    Path dir;

    @BeforeEach
    void pointTheSdkAtTheTemporaryProfileFiles() {
        for (String property : FILE_PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                savedProperties.put(property, value);
            }
        }
        System.setProperty("aws.configFile", config().toString());
        System.setProperty("aws.sharedCredentialsFile", credentials().toString());
    }

    @AfterEach
    void restoreProperties() {
        for (String property : FILE_PROPERTIES) {
            System.clearProperty(property);
        }
        savedProperties.forEach(System::setProperty);
    }

    private Path config() {
        return dir.resolve("config");
    }

    private Path credentials() {
        return dir.resolve("credentials");
    }

    private void writeConfig(String content) throws IOException {
        Files.writeString(config(), content);
    }

    private void writeCredentials(String content) throws IOException {
        Files.writeString(credentials(), content);
    }

    private S3Settings settings() {
        S3Settings.Environment environment = new S3Settings.Environment() {
            @Override
            public String property(String name) {
                return properties.get(name);
            }

            @Override
            public String variable(String name) {
                return variables.get(name);
            }
        };
        return new S3Settings(environment, ProfileFile::defaultProfileFile);
    }

    private S3Settings.Resolved resolve() throws S3Settings.InvalidSettingException {
        return settings().resolveSettings();
    }

    private S3Settings.Resolved resolveForCommand() throws S3Settings.InvalidSettingException {
        return settings().resolveForCommand();
    }

    private String region() throws S3Settings.InvalidSettingException {
        return resolve().region();
    }

    private String endpoint() throws S3Settings.InvalidSettingException {
        return resolve().endpoint();
    }

    @Nested
    class Region {

        /// The four region sources, highest first: `aws.region`, `AWS_REGION`, `AWS_DEFAULT_REGION`,
        /// the profile. Every source from `highest` down is set; the one at `highest` wins.
        @ParameterizedTest
        @ValueSource(ints = { 0, 1, 2, 3 })
        void eachSourceWinsOverEveryLowerOne(int highest) throws Exception {
            writeConfig("[default]\nregion = region-3\n");
            setRegionSourcesFrom(highest, "region-");

            assertThat(region()).isEqualTo("region-" + highest);
        }

        /// A blank value at every source above the profile counts as unset.
        @ParameterizedTest
        @ValueSource(strings = { "", " ", "\t" })
        void blankValuesFallThroughToTheProfile(String blank) throws Exception {
            writeConfig("[default]\nregion = eu-west-1\n");
            properties.put("aws.region", blank);
            variables.put("AWS_REGION", blank);
            variables.put("AWS_DEFAULT_REGION", blank);

            assertThat(region()).isEqualTo("eu-west-1");
        }

        @Test
        void blankProfileRegionIsNoRegion() throws Exception {
            writeConfig("[default]\nregion =\n");

            assertThat(region()).isNull();
        }

        @Test
        void valuesAreTrimmed() throws Exception {
            variables.put("AWS_REGION", "  us-east-2 ");

            assertThat(region()).isEqualTo("us-east-2");
        }

        @Test
        void noSourceIsNoRegion() throws Exception {
            assertThat(region()).isNull();
        }

        @Test
        void emptyFilesHaveNoRegion() throws Exception {
            writeConfig("");
            writeCredentials("");

            assertThat(region()).isNull();
        }

        @Test
        void regionKeyMustMatchExactly() throws Exception {
            writeConfig("[default]\nregion_name = us-west-2\nregional = us-west-1\nREGION = us-east-1\n");

            assertThat(region()).isNull();
        }

        @Test
        void profileDefaultHeaderInTheConfigFile() throws Exception {
            writeConfig("[profile default]\nregion = us-west-2\n");

            assertThat(region()).isEqualTo("us-west-2");
        }

        @Test
        void inlineCommentsAreNotPartOfTheValue() throws Exception {
            writeConfig("[default]\nregion = us-west-2 ; set by ops\n");

            assertThat(region()).isEqualTo("us-west-2");
        }

        @Test
        void regionFromTheCredentialsFile() throws Exception {
            writeCredentials("[default]\nregion = eu-central-1\n");

            assertThat(region()).isEqualTo("eu-central-1");
        }

        @Test
        void credentialsFileWinsOverConfigFile() throws Exception {
            writeConfig("[default]\nregion = us-west-2\n");
            writeCredentials("[default]\nregion = eu-central-1\n");

            assertThat(region()).isEqualTo("eu-central-1");
        }
    }

    @Nested
    class Profile {

        private static final String PROFILES = """
                [default]
                region = default-region
                [profile from-property]
                region = property-region
                [profile from-variable]
                region = variable-region
                """;

        @Test
        void propertyWinsOverVariable() throws Exception {
            writeConfig(PROFILES);
            properties.put("aws.profile", "from-property");
            variables.put("AWS_PROFILE", "from-variable");

            assertThat(region()).isEqualTo("property-region");
        }

        @Test
        void variableWinsOverDefault() throws Exception {
            writeConfig(PROFILES);
            variables.put("AWS_PROFILE", "from-variable");

            assertThat(region()).isEqualTo("variable-region");
        }

        @Test
        void defaultWithoutEither() throws Exception {
            writeConfig(PROFILES);

            assertThat(region()).isEqualTo("default-region");
        }

        @Test
        void namesAreTrimmed() throws Exception {
            writeConfig(PROFILES);
            variables.put("AWS_PROFILE", " from-variable ");

            assertThat(region()).isEqualTo("variable-region");
        }

        @Test
        void missingProfileDoesNotFallBackToDefault() throws Exception {
            writeConfig(PROFILES);
            properties.put("aws.profile", "missing");

            assertThat(region()).isNull();
        }

        /// As for the credential chain, a blank name selects no profile rather than `default`.
        @Test
        void blankNameSelectsNoProfile() throws Exception {
            writeConfig(PROFILES);
            properties.put("aws.profile", "");

            assertThat(region()).isNull();
        }

        @Test
        void configProfileNeedsTheProfilePrefix() throws Exception {
            writeConfig("[work]\nregion = eu-west-1\n");
            properties.put("aws.profile", "work");

            assertThat(region()).isNull();
        }

        @Test
        void selectedProfileSuppliesRegionAndEndpointTogether() throws Exception {
            writeConfig("""
                    [default]
                    region = us-east-1
                    [profile r2]
                    region = auto
                    endpoint_url = https://account.r2.cloudflarestorage.com
                    """);
            variables.put("AWS_PROFILE", "r2");

            assertThat(resolve()).isEqualTo(new S3Settings.Resolved("auto", "https://account.r2.cloudflarestorage.com"));
        }
    }

    @Nested
    class Endpoint {

        /// The seven endpoint sources, highest first: `aws.endpointUrlS3`, `aws.endpointUrl`,
        /// `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, the `services` section's `s3.endpoint_url`,
        /// the profile's `s3.endpoint_url`, the profile's `endpoint_url`. Every source from
        /// `highest` down is set; the one at `highest` wins.
        @ParameterizedTest
        @ValueSource(ints = { 0, 1, 2, 3, 4, 5, 6 })
        void eachSourceWinsOverEveryLowerOne(int highest) throws Exception {
            writeConfig(endpointProfile(highest));
            setEndpointVariablesFrom(highest, "https://source-%d.example");

            assertThat(endpoint()).isEqualTo("https://source-" + highest + ".example");
        }

        /// A blank value at every source above the profile's `endpoint_url` counts as unset.
        @Test
        void blankValuesFallThroughToTheProfile() throws Exception {
            writeConfig("""
                    [default]
                    services = local
                    s3 =
                      endpoint_url =
                    endpoint_url = https://profile.example
                    [services local]
                    s3 =
                      endpoint_url =
                    """);
            setEndpointVariablesFrom(0, " ");

            assertThat(endpoint()).isEqualTo("https://profile.example");
        }

        @Test
        void noSourceIsNoEndpoint() throws Exception {
            writeConfig("[default]\nregion = us-east-1\n");

            assertThat(endpoint()).isNull();
        }

        @Test
        void servicesSectionOfAnotherServiceIsNotUsed() throws Exception {
            writeConfig("""
                    [default]
                    services = local
                    endpoint_url = https://profile.example
                    [services local]
                    dynamodb =
                      endpoint_url = https://dynamodb.example
                    """);

            assertThat(endpoint()).isEqualTo("https://profile.example");
        }

        @Test
        void servicesSectionTheProfileDoesNotNameIsNotUsed() throws Exception {
            writeConfig("""
                    [default]
                    endpoint_url = https://profile.example
                    [services local]
                    s3 =
                      endpoint_url = https://services.example
                    """);

            assertThat(endpoint()).isEqualTo("https://profile.example");
        }

        @Test
        void missingServicesSectionFallsThroughToTheProfile() throws Exception {
            writeConfig("""
                    [default]
                    services = missing
                    endpoint_url = https://profile.example
                    """);

            assertThat(endpoint()).isEqualTo("https://profile.example");
        }

        @Test
        void endpointOfAnotherProfileIsNotUsed() throws Exception {
            writeConfig("""
                    [default]
                    endpoint_url = https://default.example
                    [profile work]
                    region = eu-west-1
                    """);
            properties.put("aws.profile", "work");

            assertThat(endpoint()).isNull();
        }

        @Test
        void portAndPathAreKept() throws Exception {
            variables.put("AWS_ENDPOINT_URL", "http://127.0.0.1:9000/base");

            assertThat(endpoint()).isEqualTo("http://127.0.0.1:9000/base");
        }

        @ParameterizedTest
        @ValueSource(strings = { "localhost:9000", "ftp://files.example", "https://", "not a url", "/just/a/path",
                "http://host:abc", "http://minio_1:9000" })
        void nonHttpUrlIsRejectedNamingItsSource(String value) {
            variables.put("AWS_ENDPOINT_URL_S3", value);

            assertThatThrownBy(S3SettingsTest.this::endpoint)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("environment variable AWS_ENDPOINT_URL_S3 must be an http or https URL with a valid host name, was '"
                            + value + "'");
        }

        @Test
        void ipv6HostIsAccepted() throws Exception {
            variables.put("AWS_ENDPOINT_URL", "http://[::1]:9000");

            assertThat(endpoint()).isEqualTo("http://[::1]:9000");
        }

        @Test
        void nonHttpUrlInTheProfileIsRejectedNamingItsSource() throws Exception {
            writeConfig("[profile work]\nendpoint_url = minio:9000\n");
            properties.put("aws.profile", "work");

            assertThatThrownBy(S3SettingsTest.this::endpoint)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("property endpoint_url of profile 'work' must be an http or https URL with a valid host name, "
                            + "was 'minio:9000'");
        }

        @Test
        void nonHttpUrlInTheServicesSectionIsRejectedNamingItsSource() throws Exception {
            writeConfig("""
                    [default]
                    services = local
                    [services local]
                    s3 =
                      endpoint_url = minio:9000
                    """);

            assertThatThrownBy(S3SettingsTest.this::endpoint)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("property s3.endpoint_url of services section 'local' must be an http or https URL "
                            + "with a valid host name, was 'minio:9000'");
        }
    }

    /// `ignore_configured_endpoint_urls`, which turns off every endpoint source when `true`.
    @Nested
    class IgnoreConfiguredEndpoints {

        /// Every endpoint source set, with the switch from `switchSource` added.
        private void setEveryEndpointSource(String switchSource, String value) throws IOException {
            String config = endpointProfile(4);
            if (switchSource.equals("profile")) {
                config = config.replace("[default]\n", "[default]\nignore_configured_endpoint_urls = " + value + "\n");
            }
            writeConfig(config);
            setEndpointVariablesFrom(0, "https://source-%d.example");
            if (switchSource.equals("property")) {
                properties.put("aws.ignoreConfiguredEndpointUrls", value);
            }
            else if (switchSource.equals("variable")) {
                variables.put("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", value);
            }
        }

        @ParameterizedTest
        @ValueSource(strings = { "property", "variable", "profile" })
        void trueTurnsOffEverySource(String switchSource) throws Exception {
            setEveryEndpointSource(switchSource, "TRUE");

            assertThat(endpoint()).isNull();
        }

        @ParameterizedTest
        @ValueSource(strings = { "property", "variable", "profile" })
        void falseKeepsTheSources(String switchSource) throws Exception {
            setEveryEndpointSource(switchSource, "false");

            assertThat(endpoint()).isEqualTo("https://source-0.example");
        }

        @Test
        void propertyWinsOverVariable() throws Exception {
            setEveryEndpointSource("variable", "true");
            properties.put("aws.ignoreConfiguredEndpointUrls", "false");

            assertThat(endpoint()).isEqualTo("https://source-0.example");
        }

        @Test
        void variableWinsOverProfile() throws Exception {
            setEveryEndpointSource("profile", "true");
            variables.put("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false");

            assertThat(endpoint()).isEqualTo("https://source-0.example");
        }

        @Test
        void blankValueCountsAsUnset() throws Exception {
            setEveryEndpointSource("profile", "true");
            properties.put("aws.ignoreConfiguredEndpointUrls", " ");

            assertThat(endpoint()).isNull();
        }

        @Test
        void otherValuesAreRejectedNamingTheirSource() throws Exception {
            setEveryEndpointSource("variable", "yes");

            assertThatThrownBy(S3SettingsTest.this::endpoint)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("environment variable AWS_IGNORE_CONFIGURED_ENDPOINT_URLS must be true or false, was 'yes'");
        }

        @Test
        void otherValuesInTheProfileAreRejectedNamingTheirSource() throws Exception {
            setEveryEndpointSource("profile", "1");

            assertThatThrownBy(S3SettingsTest.this::endpoint)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("property ignore_configured_endpoint_urls of profile 'default' must be true or false, was '1'");
        }

        /// With the endpoints ignored, a command needs a region.
        @Test
        void ignoredEndpointWithoutRegionFailsTheCommand() throws Exception {
            setEveryEndpointSource("property", "true");

            assertThatThrownBy(S3SettingsTest.this::resolveForCommand)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to determine AWS region. Set AWS_REGION, or region for profile 'default' in " + config()
                            + ". For an S3-compatible service, set AWS_ENDPOINT_URL or endpoint_url in that profile instead.");
        }
    }

    /// A command needs a region or an endpoint.
    @Nested
    class ForACommand {

        @Test
        void noRegionAndNoEndpointNamesTheDefaultProfileAndTheConfigFile() throws Exception {
            writeConfig("[default]\nendpoint_url =\n");

            assertThatThrownBy(S3SettingsTest.this::resolveForCommand)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to determine AWS region. Set AWS_REGION, or region for profile 'default' in " + config()
                            + ". For an S3-compatible service, set AWS_ENDPOINT_URL or endpoint_url in that profile instead.");
        }

        @Test
        void noRegionAndNoEndpointNamesTheSelectedProfile() throws Exception {
            writeConfig("[default]\nregion = us-east-1\n[profile work]\n");
            variables.put("AWS_PROFILE", "work");

            assertThatThrownBy(S3SettingsTest.this::resolveForCommand)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to determine AWS region. Set AWS_REGION, or region for profile 'work' in " + config()
                            + ". For an S3-compatible service, set AWS_ENDPOINT_URL or endpoint_url in that profile instead.");
        }

        @Test
        void endpointWithoutRegion() throws Exception {
            writeConfig("[default]\nendpoint_url = https://profile.example\n");

            assertThat(resolveForCommand()).isEqualTo(new S3Settings.Resolved(null, "https://profile.example"));
        }

        @Test
        void regionWithoutEndpoint() throws Exception {
            writeConfig("[default]\nregion = us-west-2\n");

            assertThat(resolveForCommand()).isEqualTo(new S3Settings.Resolved("us-west-2", null));
        }
    }

    @Nested
    class UnreadableProfileFiles {

        @ParameterizedTest
        @ValueSource(strings = {
                "region = us-west-2\n[default]\n",
                "[default\nregion = us-west-2\n",
                "[default]\nregion us-west-2\n",
                "﻿[default]\nregion = us-west-2\n" })
        void unparseableConfigFileIsReportedNamingTheFiles(String content) throws Exception {
            writeConfig(content);
            String sdkMessage = sdkParseError();

            assertThatThrownBy(S3SettingsTest.this::resolve)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to read the AWS profile files " + config() + " and " + credentials() + ": " + sdkMessage);
        }

        @Test
        void unparseableCredentialsFileIsReported() throws Exception {
            writeCredentials("aws_access_key_id = AKIA\n");
            String sdkMessage = sdkParseError();

            assertThatThrownBy(S3SettingsTest.this::resolve)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to read the AWS profile files " + config() + " and " + credentials() + ": " + sdkMessage);
        }

        /// With the region, `ignore_configured_endpoint_urls` and the endpoint set by system
        /// properties and environment variables, the profile files are not read.
        @Test
        void settingsDecidedBeforeTheFilesDoNotReadThem() throws Exception {
            writeConfig("region = us-west-2\n[default]\n");
            variables.put("AWS_REGION", "us-east-1");
            variables.put("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false");
            variables.put("AWS_ENDPOINT_URL", "https://env.example");

            assertThat(resolve()).isEqualTo(new S3Settings.Resolved("us-east-1", "https://env.example"));
        }

        /// Without `ignore_configured_endpoint_urls` set outside the files, it is looked up in
        /// them, as in the SDK, even when an endpoint variable is set.
        @Test
        void ignoreSwitchLookupInAnUnparseableFileIsReported() throws Exception {
            writeConfig("region = us-west-2\n[default]\n");
            variables.put("AWS_REGION", "us-east-1");
            variables.put("AWS_ENDPOINT_URL", "https://env.example");
            String sdkMessage = sdkParseError();

            assertThatThrownBy(S3SettingsTest.this::resolve)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to read the AWS profile files " + config() + " and " + credentials() + ": " + sdkMessage);
        }

        /// Without an endpoint set outside the files, the endpoint is looked up in them.
        @Test
        void endpointLookupInAnUnparseableFileIsReported() throws Exception {
            writeConfig("region = us-west-2\n[default]\n");
            variables.put("AWS_REGION", "us-east-1");
            String sdkMessage = sdkParseError();

            assertThatThrownBy(S3SettingsTest.this::resolve)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to read the AWS profile files " + config() + " and " + credentials() + ": " + sdkMessage);
        }

        /// Without a region set outside the files, the region is looked up in them.
        @Test
        void regionLookupInAnUnparseableFileIsReported() throws Exception {
            writeConfig("region = us-west-2\n[default]\n");
            variables.put("AWS_ENDPOINT_URL", "https://env.example");
            String sdkMessage = sdkParseError();

            assertThatThrownBy(S3SettingsTest.this::resolve)
                    .isInstanceOf(S3Settings.InvalidSettingException.class)
                    .hasMessage("Unable to read the AWS profile files " + config() + " and " + credentials() + ": " + sdkMessage);
        }

        @Test
        void directoryInPlaceOfTheConfigFileHasNoSettings() throws Exception {
            Files.createDirectory(config());

            assertThat(resolve()).isEqualTo(new S3Settings.Resolved(null, null));
        }

        /// The SDK's own message for the current profile files, which the CLI passes on.
        private String sdkParseError() {
            try {
                ProfileFile.defaultProfileFile();
            }
            catch (IllegalArgumentException e) {
                return e.getMessage();
            }
            throw new AssertionError("The SDK parsed the profile files");
        }
    }

    /// The real system properties and environment of a child JVM, which the SDK reads for the
    /// profile files' locations and which the CLI reads for every other setting.
    @Nested
    class ThroughTheProcess {

        @Test
        void environmentSelectsFilesProfileRegionAndEndpoint() throws Exception {
            writeConfig("""
                    [default]
                    region = us-east-1
                    [profile work]
                    region = eu-west-1
                    endpoint_url = https://work.example
                    """);

            assertThat(resolveInChild(List.of(), Map.of(
                    "AWS_CONFIG_FILE", config().toString(),
                    "AWS_SHARED_CREDENTIALS_FILE", credentials().toString(),
                    "AWS_PROFILE", "work")))
                    .isEqualTo("eu-west-1 https://work.example");
        }

        @Test
        void systemPropertiesWinOverEnvironment() throws Exception {
            Path other = dir.resolve("other-config");
            Files.writeString(other, "[profile env]\nregion = env-region\n[profile prop]\nregion = prop-region\n");
            writeConfig("[profile env]\nregion = wrong-file\n[profile prop]\nregion = wrong-file\n");

            assertThat(resolveInChild(
                    List.of("-Daws.configFile=" + other, "-Daws.profile=prop", "-Daws.endpointUrl=https://prop.example"),
                    Map.of("AWS_CONFIG_FILE", config().toString(),
                            "AWS_SHARED_CREDENTIALS_FILE", credentials().toString(),
                            "AWS_PROFILE", "env",
                            "AWS_ENDPOINT_URL_S3", "https://env.example")))
                    .isEqualTo("prop-region https://prop.example");
        }

        @Test
        void serviceSpecificVariableAndDefaultRegion() throws Exception {
            assertThat(resolveInChild(List.of(), Map.of(
                    "AWS_CONFIG_FILE", config().toString(),
                    "AWS_SHARED_CREDENTIALS_FILE", credentials().toString(),
                    "AWS_DEFAULT_REGION", "ap-south-1",
                    "AWS_ENDPOINT_URL", "https://global.example",
                    "AWS_ENDPOINT_URL_S3", "https://s3.example")))
                    .isEqualTo("ap-south-1 https://s3.example");
        }

        /// Runs [ResolveProcess] with only `PATH` and `variables` in its environment.
        private String resolveInChild(List<String> systemProperties, Map<String, String> variables) throws Exception {
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            command.addAll(systemProperties);
            command.add("-cp");
            command.add(System.getProperty("java.class.path"));
            command.add(ResolveProcess.class.getName());
            ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
            String path = builder.environment().get("PATH");
            builder.environment().clear();
            if (path != null) {
                builder.environment().put("PATH", path);
            }
            builder.environment().putAll(variables);

            Process process = builder.start();
            boolean finished = process.waitFor(60, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
            }
            assertThat(finished).withFailMessage("Process timed out after 60s").isTrue();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
            assertThat(process.exitValue()).withFailMessage(output).isZero();
            return output;
        }
    }

    /// Prints the region and endpoint this process resolves, separated by a space.
    public static class ResolveProcess {

        public static void main(String[] args) throws S3Settings.InvalidSettingException {
            S3Settings.Resolved settings = S3Settings.resolve();
            System.out.println(settings.region() + " " + settings.endpoint());
        }
    }

    /// Sets the region sources from index `highest` down to the system property and environment
    /// variables, each to `prefix` + its index; the profile, index 3, is written by the caller.
    private void setRegionSourcesFrom(int highest, String prefix) {
        String[] names = { "aws.region", "AWS_REGION", "AWS_DEFAULT_REGION" };
        for (int i = highest; i < names.length; i++) {
            if (i == 0) {
                properties.put(names[i], prefix + i);
            }
            else {
                variables.put(names[i], prefix + i);
            }
        }
    }

    /// Sets the endpoint system properties and environment variables from index `highest` down
    /// (indices 0 to 3), each to `format` applied to its index.
    private void setEndpointVariablesFrom(int highest, String format) {
        String[] properties = { "aws.endpointUrlS3", "aws.endpointUrl" };
        String[] variables = { "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL" };
        for (int i = highest; i < 2; i++) {
            this.properties.put(properties[i], format.formatted(i));
        }
        for (int i = Math.max(highest, 2); i < 4; i++) {
            this.variables.put(variables[i - 2], format.formatted(i));
        }
    }

    /// A config file holding the endpoint sources from index `highest` down among the profile ones
    /// (4: `services` section, 5: profile `s3.endpoint_url`, 6: profile `endpoint_url`), each set to
    /// `https://source-<index>.example`.
    private static String endpointProfile(int highest) {
        StringBuilder config = new StringBuilder("[default]\n");
        if (highest <= 4) {
            config.append("services = local\n");
        }
        if (highest <= 5) {
            config.append("s3 =\n  endpoint_url = https://source-5.example\n");
        }
        config.append("endpoint_url = https://source-6.example\n");
        config.append("[services local]\ns3 =\n  endpoint_url = https://source-4.example\n");
        return config.toString();
    }
}
