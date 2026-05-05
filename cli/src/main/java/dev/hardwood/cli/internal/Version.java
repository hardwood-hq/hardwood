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

    private static final String VERSION = initVersion();

    private Version() {
    }

    /// Returns the version in the form `<project-version> (<short-sha>[-dirty])`,
    /// e.g. `1.0.0-SNAPSHOT (a093aab-dirty)`. The `-dirty` suffix is appended when
    /// the working tree has any tracked or untracked changes at build time. The
    /// short SHA falls back to `unknown` when the build does not run from a git
    /// checkout. Values come from the filtered `application.properties` populated
    /// by the `capture-git-info` antrun step.
    private static String initVersion() {
        Config config = ConfigProvider.getConfig();
        String applicationVersion = config.getValue("project.version", String.class);
        String applicationRevision = config.getValue("project.revision", String.class);
        boolean dirty = config.getValue("project.revision.dirty", Boolean.class);

        String dirtyMark = dirty ? "-dirty" : "";
        return Fmt.fmt("%s (%s%s)", applicationVersion, applicationRevision, dirtyMark);
    }

    public static String getVersion() {
        return VERSION;
    }
}
