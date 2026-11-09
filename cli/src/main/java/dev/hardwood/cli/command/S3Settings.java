/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Supplier;

import software.amazon.awssdk.profiles.Profile;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileLocation;
import software.amazon.awssdk.profiles.ProfileProperty;

/// Resolves the region and endpoint of the CLI's S3 commands from system properties, environment
/// variables and the selected profile of the AWS config and credentials files, without the
/// instance metadata service.
///
/// The region is the first of `aws.region`, `AWS_REGION`, `AWS_DEFAULT_REGION` and the profile's
/// `region`. The endpoint follows the AWS SDK's order for a service's configured endpoint, S3 being
/// the service: `aws.endpointUrlS3`, `aws.endpointUrl`, `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`,
/// `s3.endpoint_url` of the `services` section the profile names, the profile's own
/// `s3.endpoint_url`, and the profile's `endpoint_url`. As in the SDK, `ignore_configured_endpoint_urls`
/// set to `true` turns all of them off; it is the first of `aws.ignoreConfiguredEndpointUrls`,
/// `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` and the profile's `ignore_configured_endpoint_urls`.
///
/// A blank value counts as unset and the order moves on. The profile is selected as the SDK's
/// credential chain selects it, so that region, endpoint and credentials come from the same
/// profile: `aws.profile`, then `AWS_PROFILE`, then `default`.
final class S3Settings {

    private static final String SERVICE_KEY = "s3";
    private static final String SERVICE_ENDPOINT_URL = SERVICE_KEY + "." + ProfileProperty.ENDPOINT_URL;
    private static final String SERVICES_SECTION = "services";

    /// Where system properties and environment variables are read from.
    interface Environment {

        /// The system property `name`, or `null` when it is not set.
        String property(String name);

        /// The environment variable `name`, or `null` when it is not set.
        String variable(String name);
    }

    /// This process's system properties and environment.
    static final Environment SYSTEM = new Environment() {
        @Override
        public String property(String name) {
            return System.getProperty(name);
        }

        @Override
        public String variable(String name) {
            return System.getenv(name);
        }
    };

    /// The region and endpoint, each `null` when no source sets it.
    record Resolved(String region, String endpoint) {
    }

    /// A setting holds a value the CLI cannot use, or the profile files cannot be parsed. The
    /// message names the setting or the files.
    static final class InvalidSettingException extends Exception {

        InvalidSettingException(String message) {
            super(message);
        }
    }

    /// A value and the setting it came from, for error messages.
    private record Setting(String source, String value) {
    }

    private final Environment environment;
    private final Supplier<ProfileFile> profileFiles;
    private ProfileFile profileFile;

    S3Settings(Environment environment, Supplier<ProfileFile> profileFiles) {
        this.environment = environment;
        this.profileFiles = profileFiles;
    }

    /// Resolves the settings a command needs from this process's system properties, environment
    /// and default profile files. See [#resolveForCommand].
    static Resolved resolve() throws InvalidSettingException {
        return new S3Settings(SYSTEM, ProfileFile::defaultProfileFile).resolveForCommand();
    }

    /// The settings, failing when neither a region nor an endpoint is set: without an endpoint, the
    /// region decides which AWS endpoint the requests go to. The message names the profile and the
    /// config file the region was looked for in.
    Resolved resolveForCommand() throws InvalidSettingException {
        Resolved resolved = resolveSettings();
        if (resolved.region() == null && resolved.endpoint() == null) {
            throw new InvalidSettingException("Unable to determine AWS region. Set AWS_REGION, or region for profile '"
                    + profileName() + "' in " + ProfileFileLocation.configurationFilePath()
                    + ". For an S3-compatible service, set AWS_ENDPOINT_URL or endpoint_url in that profile instead.");
        }
        return resolved;
    }

    Resolved resolveSettings() throws InvalidSettingException {
        return new Resolved(region(), endpoint());
    }

    private String region() throws InvalidSettingException {
        Setting region = first(
                property("aws.region"),
                variable("AWS_REGION"),
                variable("AWS_DEFAULT_REGION"));
        if (region == null) {
            region = profileSetting(ProfileProperty.REGION);
        }
        return region == null ? null : region.value();
    }

    private String endpoint() throws InvalidSettingException {
        if (ignoreConfiguredEndpoints()) {
            return null;
        }
        Setting endpoint = first(
                property("aws.endpointUrlS3"),
                property("aws.endpointUrl"),
                variable("AWS_ENDPOINT_URL_S3"),
                variable("AWS_ENDPOINT_URL"));
        if (endpoint == null) {
            endpoint = servicesSectionEndpoint();
        }
        if (endpoint == null) {
            endpoint = profileSetting(SERVICE_ENDPOINT_URL);
        }
        if (endpoint == null) {
            endpoint = profileSetting(ProfileProperty.ENDPOINT_URL);
        }
        return endpoint == null ? null : requireHttpUrl(endpoint);
    }

    /// Whether `ignore_configured_endpoint_urls` turns off every endpoint source.
    private boolean ignoreConfiguredEndpoints() throws InvalidSettingException {
        Setting ignore = first(
                property("aws.ignoreConfiguredEndpointUrls"),
                variable("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"));
        if (ignore == null) {
            ignore = profileSetting(ProfileProperty.IGNORE_CONFIGURED_ENDPOINT_URLS);
        }
        return ignore != null && requireBoolean(ignore);
    }

    private static boolean requireBoolean(Setting setting) throws InvalidSettingException {
        if ("true".equalsIgnoreCase(setting.value())) {
            return true;
        }
        if ("false".equalsIgnoreCase(setting.value())) {
            return false;
        }
        throw new InvalidSettingException(
                setting.source() + " must be true or false, was '" + setting.value() + "'");
    }

    /// `s3.endpoint_url` of the `services` section the selected profile names.
    private Setting servicesSectionEndpoint() throws InvalidSettingException {
        Setting sectionName = profileSetting(SERVICES_SECTION);
        if (sectionName == null) {
            return null;
        }
        Optional<Profile> section = profileFile().getSection(SERVICES_SECTION, sectionName.value());
        return setting("property " + SERVICE_ENDPOINT_URL + " of services section '" + sectionName.value() + "'",
                section.flatMap(s -> s.property(SERVICE_ENDPOINT_URL)).orElse(null));
    }

    private static String requireHttpUrl(Setting endpoint) throws InvalidSettingException {
        URI uri;
        try {
            uri = new URI(endpoint.value());
        }
        catch (URISyntaxException e) {
            uri = null;
        }
        // A registry-based authority such as `minio_1:9000` parses, but has no host to send requests to
        if (uri == null || uri.getHost() == null
                || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new InvalidSettingException(endpoint.source()
                    + " must be an http or https URL with a valid host name, was '" + endpoint.value() + "'");
        }
        return endpoint.value();
    }

    /// The selected profile's property `name`.
    private Setting profileSetting(String name) throws InvalidSettingException {
        String profileName = profileName();
        Optional<Profile> profile = profileFile().profile(profileName);
        return setting("property " + name + " of profile '" + profileName + "'",
                profile.flatMap(p -> p.property(name)).orElse(null));
    }

    /// The profile `ProfileFileSystemSetting.AWS_PROFILE` selects: the trimmed `aws.profile`, else
    /// the trimmed `AWS_PROFILE`, else `default`. A blank name selects no profile, as it does for
    /// the credential chain.
    private String profileName() {
        String name = environment.property("aws.profile");
        if (name == null) {
            name = environment.variable("AWS_PROFILE");
        }
        return name == null ? "default" : name.trim();
    }

    /// The profile files, parsed on first use, so that settings decided before them do not read them.
    private ProfileFile profileFile() throws InvalidSettingException {
        if (profileFile == null) {
            try {
                profileFile = profileFiles.get();
            }
            catch (RuntimeException e) {
                throw new InvalidSettingException("Unable to read the AWS profile files "
                        + ProfileFileLocation.configurationFilePath() + " and "
                        + ProfileFileLocation.credentialsFilePath() + ": " + e.getMessage());
            }
        }
        return profileFile;
    }

    private Setting property(String name) {
        return setting("system property " + name, environment.property(name));
    }

    private Setting variable(String name) {
        return setting("environment variable " + name, environment.variable(name));
    }

    /// The trimmed `value` from `source`, or `null` when it is unset or blank.
    private static Setting setting(String source, String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return new Setting(source, value.strip());
    }

    private static Setting first(Setting... settings) {
        for (Setting setting : settings) {
            if (setting != null) {
                return setting;
            }
        }
        return null;
    }
}
