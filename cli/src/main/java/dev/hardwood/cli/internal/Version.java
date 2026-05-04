/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

/// Resolves the human-readable version string for the CLI and TUI.
public final class Version {

    /// Value of `project.revision.dirty` produced by `buildnumber-maven-plugin`'s
    /// `doCheck` when the working tree has uncommitted changes (the alternative is `ok`).
    private static final String DIRTY_MARKER = "tainted";

    private static final String VERSION = initVersion();

    private Version() {
    }

    /// Returns the version in the form `<project-version> (<short-sha>[-dirty])`,
    /// e.g. `1.0.0-SNAPSHOT (a093aab-dirty)`. Values come from the filtered
    /// `application.properties` populated at build time by `buildnumber-maven-plugin`.

    private static String initVersion() {
        Config config = ConfigProvider.getConfig();
        String applicationVersion = config.getValue("project.version", String.class);
        String applicationRevision = config.getValue("project.revision", String.class);
        String dirtyStatus = config.getValue("project.revision.dirty", String.class);

        String dirtyMark = DIRTY_MARKER.equalsIgnoreCase(dirtyStatus) ? "-dirty" : "";
        return Fmt.fmt("%s (%s%s)", applicationVersion, applicationRevision, dirtyMark);
    }

    public static String getVersion() {
        return VERSION;
    }
}
