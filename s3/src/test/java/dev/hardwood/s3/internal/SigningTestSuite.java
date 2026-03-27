/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

/// Clones the AWS SigV4 test suite from [awslabs/aws-c-auth](https://github.com/awslabs/aws-c-auth)
/// into `target/aws-c-auth` and provides the path to the v4 test vectors.
final class SigningTestSuite {

    private static final String REPO_URL = "https://github.com/awslabs/aws-c-auth.git";
    private static final Path TARGET_DIR = Path.of("target/aws-c-auth");
    private static final Path TEST_SUITE = TARGET_DIR.resolve("tests/aws-signing-test-suite/v4");

    static Path ensureCloned() throws IOException {
        if (Files.exists(TEST_SUITE) && Files.isDirectory(TEST_SUITE)) {
            return TEST_SUITE;
        }

        System.out.println("Cloning aws-c-auth repository (shallow)...");

        // Clean up any partial clone
        if (Files.exists(TARGET_DIR)) {
            deleteRecursively(TARGET_DIR);
        }

        try {
            Git.cloneRepository()
                    .setURI(REPO_URL)
                    .setDirectory(TARGET_DIR.toFile())
                    .setDepth(1)
                    .call()
                    .close();

            System.out.println("Successfully cloned signing test suite to: " + TEST_SUITE.toAbsolutePath());
            return TEST_SUITE;
        }
        catch (GitAPIException e) {
            throw new IOException("Failed to clone aws-c-auth repository: " + e.getMessage(), e);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
