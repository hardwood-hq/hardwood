/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import org.eclipse.microprofile.config.ConfigProvider;

public class Version {

    private static final String VERSION = initVersion();

    private static String initVersion() {
        String applicationVersion = ConfigProvider.getConfig().getValue("project.version", String.class);
        String applicationRevision = ConfigProvider.getConfig().getValue("project.revision", String.class);
        String dirtyStatus = ConfigProvider.getConfig().getValue("project.revision.dirty", String.class);

        String dirtyMark = "tainted".equalsIgnoreCase(dirtyStatus) ? "-dirty" : "";
        return Fmt.fmt("%s (%s%s)", applicationVersion, applicationRevision, dirtyMark);
    }

    private Version() {
    }

    public static String getVersion() {
        return VERSION;
    }

}
