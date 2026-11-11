/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexWriter;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;

/// Copies of a fixture with one footer-addressed structure made unparseable or
/// malformed, for the commands and `dive` screens that read such structures at
/// the bytes directly.
public final class DamagedFiles {

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

    /// Writes `damaged.parquet` into `tempDir`: a copy of `source` whose OffsetIndex at `offset`
    /// is replaced by one listing no page, which is malformed for a chunk with values. The bytes
    /// after its stop field are left in place and never read.
    static Path emptyOffsetIndex(Path source, long offset, Path tempDir) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        // Field 1, a list, of no structs, then the stop field
        byte[] empty = {0x19, 0x0C, 0x00};
        System.arraycopy(empty, 0, bytes, Math.toIntExact(offset), empty.length);
        Path damaged = tempDir.resolve("damaged.parquet");
        Files.write(damaged, bytes);
        return damaged;
    }

    /// Writes `damaged.parquet` into `tempDir`: a copy of `source` whose OffsetIndex at `offset`,
    /// of `length` bytes, has its first page start at row 1, which a read refuses: the first page
    /// starts at row 0. The rewritten index takes the bytes of the original.
    public static Path offsetIndexFromRowOne(Path source, long offset, int length, Path tempDir)
            throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        int at = Math.toIntExact(offset);
        OffsetIndex index = OffsetIndexReader.read(new ThriftCompactReader(ByteBuffer.wrap(bytes, at, length).slice()));
        List<PageLocation> pages = new ArrayList<>(index.pageLocations());
        PageLocation first = pages.getFirst();
        pages.set(0, new PageLocation(first.offset(), first.compressedPageSize(), 1));
        ThriftCompactWriter writer = new ThriftCompactWriter();
        OffsetIndexWriter.write(writer, new OffsetIndex(pages, index.unencodedByteArrayDataBytes()));
        byte[] rewritten = writer.toByteArray();
        if (rewritten.length != length) {
            throw new IllegalStateException("Rewritten OffsetIndex of " + rewritten.length
                    + " bytes does not take the original's " + length);
        }
        System.arraycopy(rewritten, 0, bytes, at, length);
        Path damaged = tempDir.resolve("damaged.parquet");
        Files.write(damaged, bytes);
        return damaged;
    }
}
