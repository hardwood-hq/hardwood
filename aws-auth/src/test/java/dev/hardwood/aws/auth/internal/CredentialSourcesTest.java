/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.aws.auth.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import software.amazon.awssdk.profiles.ProfileFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Runs against system properties and profile files in a temp directory. The SDK falls back
/// to environment variables for unset properties, so the tests are skipped when the test JVM
/// has any `AWS_` variable.
class CredentialSourcesTest {

    private static final String[] PROPERTIES = {
            "aws.accessKeyId", "aws.secretAccessKey", "aws.sessionToken", "aws.webIdentityTokenFile",
            "aws.containerCredentialsPath", "aws.containerCredentialsFullUri", "aws.profile", "aws.configFile",
            "aws.sharedCredentialsFile" };

    @TempDir
    Path dir;

    @BeforeEach
    void pointProfileFilesAtTempDir() {
        assumeTrue(System.getenv().keySet().stream().noneMatch(name -> name.startsWith("AWS_")),
                "AWS_ environment variables are set");
        System.setProperty("aws.configFile", dir.resolve("config").toString());
        System.setProperty("aws.sharedCredentialsFile", dir.resolve("credentials").toString());
    }

    @AfterEach
    void clearProperties() {
        for (String property : PROPERTIES) {
            System.clearProperty(property);
        }
    }

    @Test
    void nothingConfigured() {
        assertThat(CredentialSources.anyConfigured()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "aws.accessKeyId", "aws.secretAccessKey", "aws.sessionToken", "aws.webIdentityTokenFile",
            "aws.containerCredentialsPath", "aws.containerCredentialsFullUri", "aws.profile" })
    void settingConfigured(String property) {
        System.setProperty(property, "value");

        assertThat(CredentialSources.anyConfigured()).isTrue();
    }

    @Test
    void defaultProfileWithRegionOnly() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nregion = us-west-2\n");

        assertThat(CredentialSources.anyConfigured()).isFalse();
    }

    @Test
    void defaultProfileWithKeys() throws IOException {
        Files.writeString(dir.resolve("credentials"),
                "[default]\naws_access_key_id = AKID\naws_secret_access_key = secret\n");

        assertThat(CredentialSources.anyConfigured()).isTrue();
    }

    @Test
    void otherProfileWithKeysOnly() throws IOException {
        Files.writeString(dir.resolve("credentials"),
                "[work]\naws_access_key_id = AKID\naws_secret_access_key = secret\n");

        assertThat(CredentialSources.anyConfigured()).isFalse();
    }

    @Test
    void unparseableProfileFileCountsAsConfigured() throws IOException {
        Files.writeString(dir.resolve("config"), "[default]\nnot a property\n");

        assertThatThrownBy(ProfileFile::defaultProfileFile)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected an '=' sign defining a property on line 2");
        assertThat(CredentialSources.anyConfigured()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = { "aws_secret_access_key = secret", "aws_session_token = token", "role_arn = arn:aws:iam::1:role/r",
            "credential_process = /bin/creds", "login_session = arn:aws:iam::1:user/u",
            "sso_start_url = https://example.awsapps.com/start", "sso_region = us-east-1",
            "sso_account_id = 123456789012", "sso_role_name = reader", "sso_session = my-sso" })
    void profileCredentialProperty(String line) {
        ProfileFile profileFile = ProfileFile.builder()
                .content("[default]\n" + line + "\n")
                .type(ProfileFile.Type.CONFIGURATION)
                .build();

        assertThat(CredentialSources.profileHasCredentials(profileFile, "default")).isTrue();
    }
}
