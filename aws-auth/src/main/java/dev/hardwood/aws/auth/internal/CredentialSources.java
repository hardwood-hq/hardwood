/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.aws.auth.internal;

import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.profiles.Profile;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.profiles.ProfileProperty;

/// Tells whether a local source of the AWS SDK's default credential chain is configured.
///
/// The chain reports every failure as the same exception, whether no source is configured
/// or a configured one is broken. This check tells the two apart before the chain runs, so
/// that a caller can fall back to unsigned requests when nothing is configured while a
/// broken configuration still fails with the chain's error.
///
/// Mirrors the sources of `DefaultCredentialsProvider` other than instance metadata, which
/// is not configured locally: system properties and environment variables, a web identity
/// token file, the container credentials endpoint, and the selected profile. A source the
/// SDK adds in a later version is not seen here until it is added to this class.
public final class CredentialSources {

    /// Profile properties from which the SDK derives credentials for a profile.
    private static final List<String> PROFILE_CREDENTIAL_PROPERTIES = List.of(
            ProfileProperty.AWS_ACCESS_KEY_ID,
            ProfileProperty.AWS_SECRET_ACCESS_KEY,
            ProfileProperty.AWS_SESSION_TOKEN,
            ProfileProperty.ROLE_ARN,
            ProfileProperty.CREDENTIAL_PROCESS,
            ProfileProperty.LOGIN_SESSION,
            ProfileProperty.SSO_START_URL,
            ProfileProperty.SSO_REGION,
            ProfileProperty.SSO_ACCOUNT_ID,
            ProfileProperty.SSO_ROLE_NAME,
            "sso_session");

    private static final List<SdkSystemSetting> CREDENTIAL_SETTINGS = List.of(
            SdkSystemSetting.AWS_ACCESS_KEY_ID,
            SdkSystemSetting.AWS_SECRET_ACCESS_KEY,
            SdkSystemSetting.AWS_SESSION_TOKEN,
            SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE,
            SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI,
            SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI);

    private CredentialSources() {
    }

    /// Returns whether any local credential source is configured, reading the default profile
    /// files. A profile file that cannot be parsed counts as configured, so the chain raises
    /// the parse error.
    public static boolean anyConfigured() {
        if (anySettingPresent() || ProfileFileSystemSetting.AWS_PROFILE.getNonDefaultStringValue().isPresent()) {
            return true;
        }
        ProfileFile profileFile;
        try {
            profileFile = ProfileFile.defaultProfileFile();
        }
        catch (RuntimeException e) {
            return true;
        }
        return profileHasCredentials(profileFile, ProfileFileSystemSetting.AWS_PROFILE.defaultValue());
    }

    /// Returns whether the named profile carries any property the SDK derives credentials from.
    static boolean profileHasCredentials(ProfileFile profileFile, String profileName) {
        Optional<Profile> profile = profileFile.profile(profileName);
        if (profile.isEmpty()) {
            return false;
        }
        for (String property : PROFILE_CREDENTIAL_PROPERTIES) {
            if (profile.get().property(property).isPresent()) {
                return true;
            }
        }
        return false;
    }

    private static boolean anySettingPresent() {
        for (SdkSystemSetting setting : CREDENTIAL_SETTINGS) {
            if (setting.getNonDefaultStringValue().isPresent()) {
                return true;
            }
        }
        return false;
    }
}
