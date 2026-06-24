/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
        Properties props = new Properties();
        try (InputStream in = Version.class.getResourceAsStream("/application.properties")) {
            if (in != null) {
                props.load(in);
            }
        }
        catch (IOException e) {
            // Ignore — fall through to defaults
        }
        String applicationVersion = props.getProperty("project.version", "unknown");
        String applicationRevision = props.getProperty("project.revision", "unknown");
        boolean dirty = Boolean.parseBoolean(props.getProperty("project.revision.dirty", "false"));

        String dirtyMark = dirty ? "-dirty" : "";
        return Fmt.fmt("%s (%s%s)", applicationVersion, applicationRevision, dirtyMark);
    }

    public static String getVersion() {
        return VERSION;
    }
}
