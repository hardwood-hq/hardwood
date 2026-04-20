/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.picocli.runtime.PicocliCommandLineFactory;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.enterprise.inject.Produces;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;

@TopCommand()
@CommandLine.Command(name = "hardwood", mixinStandardHelpOptions = true, versionProvider = VersionProviderWithConfigProvider.class, subcommands = {
        HelpCommand.class,
        InfoCommand.class,
        SchemaCommand.class,
        ConvertCommand.class,
        FooterCommand.class,
        InspectCommand.class,
        PrintCommand.class
}, description = "A command-line interface for hardwood"

)

public class HardwoodCommand {
    @Produces
    CommandLine getCommandLineInstance(PicocliCommandLineFactory factory) {
        return factory.create().setCaseInsensitiveEnumValuesAllowed(true);
    }
}

class VersionProviderWithConfigProvider implements IVersionProvider {

    @Override
    public String[] getVersion() {
        String applicationName = "hardwood";
        String applicationVersion = ConfigProvider.getConfig().getValue("project.version", String.class);
        return new String[]{ String.format("%s %s", applicationName, applicationVersion) };
    }
}
