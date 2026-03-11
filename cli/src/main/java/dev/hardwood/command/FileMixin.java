/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.nio.file.Path;

import dev.hardwood.InputFile;
import dev.hardwood.s3.S3InputFile;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

public class FileMixin {

    private static final String[] REMOTE_PREFIXES = { "s3://", "s3a://", "s3n://" };
    private static final String[] UNSUPPORTED_REMOTE_PREFIXES = { "gs://", "gcs://", "hdfs://" };

    @CommandLine.Option(names = { "-f", "--file" }, required = true, paramLabel = "FILE", description = "Path to the Parquet file.")
    String file;

    @Spec
    CommandSpec spec;

    boolean isRemoteUri() {
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                spec.commandLine().getErr().println("Remote paths are not implemented yet for this command.");
                return true;
            }
        }
        return false;
    }

    InputFile toInputFile() {
        for (String prefix : REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                return parseS3Uri(prefix);
            }
        }
        for (String prefix : UNSUPPORTED_REMOTE_PREFIXES) {
            if (file.startsWith(prefix)) {
                spec.commandLine().getErr().println("Remote URIs are not implemented yet.");
                return null;
            }
        }
        return InputFile.of(Path.of(file));
    }

    private InputFile parseS3Uri(String prefix) {
        String withoutPrefix = file.substring(prefix.length());
        int slash = withoutPrefix.indexOf('/');
        if (slash < 0 || slash == withoutPrefix.length() - 1) {
            spec.commandLine().getErr().println("Invalid S3 URI (missing key): " + file);
            return null;
        }
        String bucket = withoutPrefix.substring(0, slash);
        String key = withoutPrefix.substring(slash + 1);
        return S3InputFile.of(bucket, key);
    }

    Path toPath() {
        return Path.of(file);
    }
}