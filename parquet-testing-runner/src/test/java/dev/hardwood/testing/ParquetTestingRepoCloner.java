/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

/**
 * Utility to clone the parquet-testing repository for comparison tests.
 */
public class ParquetTestingRepoCloner {

    private static final String REPO_URL = "https://github.com/apache/parquet-testing.git";
    private static final Path TARGET_DIR = Path.of("target/parquet-testing");

    /**
     * Ensure the parquet-testing repository is cloned to target/parquet-testing.
     * If already present, returns the existing path without re-cloning.
     *
     * @return path to the cloned repository
     * @throws IOException if cloning fails
     */
    public static Path ensureCloned() throws IOException {
        if (Files.exists(TARGET_DIR) && Files.isDirectory(TARGET_DIR)) {
            // Already cloned
            return TARGET_DIR;
        }

        System.out.println("Cloning parquet-testing repository (shallow clone)...");

        try {
            Git.cloneRepository()
                    .setURI(REPO_URL)
                    .setDirectory(TARGET_DIR.toFile())
                    .setDepth(1) // Shallow clone for speed
                    .call()
                    .close();

            System.out.println("Successfully cloned to: " + TARGET_DIR.toAbsolutePath());
            return TARGET_DIR;
        }
        catch (GitAPIException e) {
            throw new IOException("Failed to clone parquet-testing repository: " + e.getMessage(), e);
        }
    }

    /**
     * Get the path to a test file within the parquet-testing repository.
     *
     * @param relativePath path relative to repo root (e.g., "data/alltypes_dictionary.parquet")
     * @return absolute path to the file
     * @throws IOException if repo is not cloned or file doesn't exist
     */
    public static Path getTestFile(String relativePath) throws IOException {
        Path repoPath = ensureCloned();
        Path filePath = repoPath.resolve(relativePath);

        if (!Files.exists(filePath)) {
            throw new IOException("Test file not found: " + filePath);
        }

        return filePath;
    }
}
