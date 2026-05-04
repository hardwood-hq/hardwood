package dev.hardwood.cli.internal;

import org.eclipse.microprofile.config.ConfigProvider;

public class Version {

    private Version() {
    }

    public static String getVersion() {
        String applicationVersion = ConfigProvider.getConfig().getValue("project.version", String.class);
        String applicationRevision = ConfigProvider.getConfig().getValue("project.revision", String.class);
        String dirtyStatus = ConfigProvider.getConfig().getValue("project.revision.dirty", String.class);

        String dirtyMark = "tainted".equalsIgnoreCase(dirtyStatus) ? "-dirty" : "";
        return Fmt.fmt("%s (%s%s)", applicationVersion, applicationRevision, dirtyMark);
    }

}
