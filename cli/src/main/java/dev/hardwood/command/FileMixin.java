/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.nio.file.Path;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

public class FileMixin {

    private static final String[] REMOTE_PREFIXES = { "s3://", "s3a://", "s3n://", "gs://", "gcs://", "hdfs://" };

    @CommandLine.Option(names = { "-f", "--file" }, required = true, paramLabel = "FILE", description = "Path to the Parquet file.")
    String file;

    @Spec
    CommandSpec spec;

    boolean isRemoteUri() {
        for (String prefix : REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                spec.commandLine().getErr().println("Remote URIs are not implemented yet.");
                return true;
            }
        }
        return false;
    }

    Path toPath() {
        return Path.of(file);
    }
}