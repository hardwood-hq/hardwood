/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/// Identifies the Hardwood build the running code came from.
///
/// The values are baked into `dev/hardwood/build-info.properties` at build time: the Maven
/// project version, the short commit hash captured by the parent POM's `capture-git-info`
/// execution, and the working-tree cleanliness captured by core's `capture-git-dirty` execution.
/// A build that runs outside a git checkout, or a classpath the resource did not survive,
/// reports [#UNKNOWN] rather than failing — the identifier is descriptive, and every consumer
/// of it renders the placeholder as-is so an unidentifiable build is visible rather than
/// silently mistaken for a known one.
public final class BuildInfo {

    /// The value reported for a component the build could not determine.
    public static final String UNKNOWN = "unknown";

    private static final String RESOURCE = "/dev/hardwood/build-info.properties";

    private static final Properties PROPERTIES = load();

    private static final String VERSION = resolve("project.version");
    private static final String REVISION = resolve("project.revision");
    private static final boolean DIRTY = Boolean.parseBoolean(resolve("project.revision.dirty"));

    private BuildInfo() {
    }

    /// The Maven project version, e.g. `1.1.0-SNAPSHOT`, or [#UNKNOWN].
    public static String version() {
        return VERSION;
    }

    /// The short commit hash the build ran from, e.g. `a093aab`, or [#UNKNOWN].
    public static String revision() {
        return REVISION;
    }

    /// Whether the working tree had tracked or untracked changes at build time. False when
    /// the build could not determine it.
    public static boolean dirty() {
        return DIRTY;
    }

    /// The commit hash with a `-dirty` suffix when the working tree was not clean, e.g.
    /// `a093aab-dirty`. This is the form embedded in artifacts that identify their own build.
    public static String revisionWithDirtyMark() {
        return DIRTY ? REVISION + "-dirty" : REVISION;
    }

    private static Properties load() {
        Properties properties = new Properties();
        try (InputStream in = BuildInfo.class.getResourceAsStream(RESOURCE)) {
            if (in != null) {
                properties.load(in);
            }
        }
        catch (IOException e) {
            // A build identifier is descriptive only; an unreadable resource degrades to UNKNOWN.
        }
        return properties;
    }

    /// Reads one property, mapping both a missing value and an unsubstituted `${...}`
    /// placeholder — what an unfiltered copy of the resource leaves behind — to [#UNKNOWN].
    private static String resolve(String key) {
        String value = PROPERTIES.getProperty(key);
        if (value == null || value.isBlank() || value.startsWith("${")) {
            return UNKNOWN;
        }
        return value;
    }
}
