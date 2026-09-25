/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// Copies of a fixture with one footer-addressed structure made unparseable,
/// for the commands that read such structures at the bytes directly.
final class DamagedFiles {

    private DamagedFiles() {
    }

    /// Writes `damaged.parquet` into `tempDir`: a copy of `source` with 24
    /// bytes at `offset` overwritten by `0xff`, which the Thrift parser
    /// rejects as field type 15.
    static Path damage(Path source, long offset, Path tempDir) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        for (int i = 0; i < 24; i++) {
            bytes[Math.toIntExact(offset) + i] = (byte) 0xff;
        }
        Path damaged = tempDir.resolve("damaged.parquet");
        Files.write(damaged, bytes);
        return damaged;
    }
}
