/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.writer.DataPageVersion;

/// Writes one column chunk's data pages to its sink in page order, each body compressed on the
/// chunk's thread or, while a flush thread is idle, handed to [PageCompression].
///
/// A `DATA_PAGE` compresses its whole body, levels included. A `DATA_PAGE_V2` stores its level
/// streams as encoded and compresses only the value section after them; its CRC covers both, as
/// stored.
///
/// A page's header carries its stored size and CRC, and its place in the file follows the pages
/// before it, so a page handed off is written once its compression is done and every page before
/// it is written. Behind a page still being compressed elsewhere, the chunk's thread goes on
/// compressing its next pages itself and holds them until their turn. The bytes are the same
/// whichever thread compressed which body.
final class DataPageWriter {

    private static final byte[] NO_LEVELS = new byte[0];

    private final ChunkSink sink;
    private final CompressionCodec codec;
    private final Compressor compressor;
    private final DataPageVersion version;
    /// Where bodies are handed off; `null` where every body is compressed here.
    private final PageCompression handoff;
    private final long firstPageOffset;
    /// Pages not yet written, in page order: those handed off, and those compressed here behind a
    /// page a worker still holds.
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final List<PageLocation> locations = new ArrayList<>();
    private long storedBytes;
    private long uncompressedBytes;

    /// What one data page's header states besides its sizes and CRC, and where the page's rows
    /// start.
    ///
    /// @param entries the page's level entries, nulls included
    /// @param nulls the entries carrying no value
    /// @param rows the records the page holds
    /// @param repetitionLevelsLength the byte length of the page's repetition level stream, which
    ///        opens its body
    /// @param definitionLevelsLength the byte length of the definition level stream after it
    /// @param encoding the encoding of the page's values
    /// @param firstRow the index within the row group of the page's first row
    record Shape(int entries, int nulls, int rows, int repetitionLevelsLength, int definitionLevelsLength,
                 Encoding encoding, long firstRow) {

        int levelsLength() {
            return repetitionLevelsLength + definitionLevelsLength;
        }
    }

    private record Pending(PageCompression.Page page, Shape shape, int uncompressedLength) {
    }

    /// @param sink takes the chunk's bytes
    /// @param codec the codec `compressor` applies; `UNCOMPRESSED` bodies are stored as built
    /// @param compressor compresses each body
    /// @param version the header and layout of every page
    /// @param handoff where bodies are handed off, or `null` to compress every body here
    /// @param firstPageOffset the chunk-relative offset of the first data page
    DataPageWriter(ChunkSink sink, CompressionCodec codec, Compressor compressor, DataPageVersion version,
                   PageCompression handoff, long firstPageOffset) {
        this.sink = sink;
        this.codec = codec;
        this.compressor = compressor;
        this.version = version;
        this.handoff = codec == CompressionCodec.UNCOMPRESSED ? null : handoff;
        this.firstPageOffset = firstPageOffset;
    }

    /// Writes the page whose body `body` holds, or hands the body off and writes the page once it
    /// is compressed. `body` may be reused once this returns.
    ///
    /// @param body the page's level streams, then its value section
    /// @param shape what the page's header states besides its sizes and CRC
    void write(ByteArrayBuilder body, Shape shape) throws IOException {
        int length = body.length();
        if (codec == CompressionCodec.UNCOMPRESSED) {
            // Stored as it stands, written straight out of the body buffer, which the next page
            // overwrites.
            CRC32 crc = new CRC32();
            crc.update(body.array(), 0, length);
            frame(shape, length, NO_LEVELS, 0, false, body.array(), length, (int) crc.getValue(), true);
            return;
        }
        // The bytes stored ahead of the compressed region: a V2 page's level streams.
        int prefix = version == DataPageVersion.V2 ? shape.levelsLength() : 0;
        if (handoff != null && handoff.tryReserve()) {
            byte[] levels = prefix > 0 ? Arrays.copyOf(body.array(), prefix) : NO_LEVELS;
            PageCompression.Page page = handoff.offer(compressor, levels,
                    Arrays.copyOfRange(body.array(), prefix, length), length - prefix);
            pending.add(new Pending(page, shape, length));
            writeDone();
            return;
        }
        if (!pending.isEmpty()) {
            // Compressed here without waiting for the pages handed off before it, which a worker
            // polling for bodies takes up a moment after they are queued, and written after them.
            byte[] levels = prefix > 0 ? Arrays.copyOf(body.array(), prefix) : NO_LEVELS;
            pending.add(new Pending(handoff.compressHere(compressor, levels, body.array(), prefix, length - prefix),
                    shape, length));
            writeDone();
            return;
        }
        byte[] stored = compressor.compress(body.array(), prefix, length - prefix);
        CRC32 crc = new CRC32();
        crc.update(body.array(), 0, prefix);
        crc.update(stored, 0, stored.length);
        // The levels are read out of the body buffer, which the next page overwrites; a
        // compressor's output is not.
        frame(shape, length, body.array(), prefix, true, stored, stored.length, (int) crc.getValue(), false);
    }

    /// Writes every page handed off and not yet written.
    void finish() throws IOException {
        writePending();
    }

    /// Takes back every body handed off and still unclaimed, and waits for those another thread
    /// claimed, so no thread still compresses one of this chunk's bodies once a failure leaves.
    void abandon() {
        for (Pending p : pending) {
            if (!p.page().claim()) {
                p.page().await();
            }
        }
        pending.clear();
    }

    /// The chunk's data pages as stored: headers and bodies.
    long storedBytes() {
        return storedBytes;
    }

    /// The chunk's data pages with every body at its size before compression.
    long uncompressedBytes() {
        return uncompressedBytes;
    }

    List<PageLocation> locations() {
        return locations;
    }

    /// Writes the pages at the head of [#pending] whose compression is done.
    private void writeDone() throws IOException {
        while (!pending.isEmpty() && pending.peek().page().isDone()) {
            writeHandedOff(pending.poll());
        }
    }

    /// Writes every pending page, compressing those still unclaimed here.
    private void writePending() throws IOException {
        while (!pending.isEmpty()) {
            Pending next = pending.peek();
            if (next.page().claim()) {
                next.page().run();
            }
            else {
                next.page().await();
            }
            writeHandedOff(pending.poll());
        }
    }

    private void writeHandedOff(Pending p) throws IOException {
        PageCompression.Page page = p.page();
        Throwable failure = page.failure();
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("Compressing a page failed", failure);
        }
        byte[] levels = page.levels();
        frame(p.shape(), p.uncompressedLength(), levels, levels.length, false, page.stored(), page.stored().length,
                page.crc(), false);
    }

    /// Writes the header of a page, then the `levelsLength` bytes of `levels` stored ahead of its
    /// compressed region, then its `storedLength` bytes of `stored`.
    private void frame(Shape shape, int uncompressedLength, byte[] levels, int levelsLength, boolean levelsReused,
                       byte[] stored, int storedLength, int crc, boolean reused) throws IOException {
        int pageLength = levelsLength + storedLength;
        ThriftCompactWriter header = new ThriftCompactWriter();
        if (version == DataPageVersion.V2) {
            PageHeaderWriter.writeDataPageV2(header, shape.entries(), shape.nulls(), shape.rows(), uncompressedLength,
                    pageLength, crc, shape.encoding(), shape.definitionLevelsLength(),
                    shape.repetitionLevelsLength(), codec != CompressionCodec.UNCOMPRESSED);
        }
        else {
            PageHeaderWriter.writeDataPageV1(header, shape.entries(), uncompressedLength, pageLength, crc,
                    shape.encoding());
        }
        byte[] headerBytes = header.toByteArray();
        sink.write(headerBytes, 0, headerBytes.length, false);
        if (levelsLength > 0) {
            sink.write(levels, 0, levelsLength, levelsReused);
        }
        sink.write(stored, 0, storedLength, reused);
        long pageBytes = (long) headerBytes.length + pageLength;
        locations.add(new PageLocation(firstPageOffset + storedBytes, Math.toIntExact(pageBytes), shape.firstRow()));
        storedBytes += pageBytes;
        uncompressedBytes += headerBytes.length + uncompressedLength;
    }
}
