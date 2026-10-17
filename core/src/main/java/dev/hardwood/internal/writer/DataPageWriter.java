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

/// Writes one column chunk's data pages to its sink in page order, each body compressed on the
/// chunk's thread or, while a flush thread is idle, handed to [PageCompression].
///
/// A page's header carries its stored size and CRC, and its place in the file follows the pages
/// before it, so a page handed off is written once its compression is done and every page before
/// it is written. Behind a page still being compressed elsewhere, the chunk's thread goes on
/// compressing its next pages itself and holds them until their turn. The bytes are the same
/// whichever thread compressed which body.
final class DataPageWriter {

    private final ChunkSink sink;
    private final CompressionCodec codec;
    private final Compressor compressor;
    /// Where bodies are handed off; `null` where every body is compressed here.
    private final PageCompression handoff;
    private final long firstPageOffset;
    /// Pages not yet written, in page order: those handed off, and those compressed here behind a
    /// page a worker still holds.
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final List<PageLocation> locations = new ArrayList<>();
    private long storedBytes;
    private long uncompressedBytes;

    private record Pending(PageCompression.Page page, int entries, int uncompressedLength, Encoding encoding,
                           long firstRow) {
    }

    /// @param sink takes the chunk's bytes
    /// @param codec the codec `compressor` applies; `UNCOMPRESSED` bodies are stored as built
    /// @param compressor compresses each body
    /// @param handoff where bodies are handed off, or `null` to compress every body here
    /// @param firstPageOffset the chunk-relative offset of the first data page
    DataPageWriter(ChunkSink sink, CompressionCodec codec, Compressor compressor, PageCompression handoff,
                   long firstPageOffset) {
        this.sink = sink;
        this.codec = codec;
        this.compressor = compressor;
        this.handoff = codec == CompressionCodec.UNCOMPRESSED ? null : handoff;
        this.firstPageOffset = firstPageOffset;
    }

    /// Writes the page whose body `body` holds, or hands the body off and writes the page once it
    /// is compressed. `body` may be reused once this returns.
    ///
    /// @param entries the page's level entries
    /// @param encoding the encoding of the page's values
    /// @param firstRow the index within the row group of the page's first row
    void write(ByteArrayBuilder body, int entries, Encoding encoding, long firstRow) throws IOException {
        int length = body.length();
        if (handoff != null && handoff.tryReserve()) {
            PageCompression.Page page = handoff.offer(compressor, Arrays.copyOf(body.array(), length), length);
            pending.add(new Pending(page, entries, length, encoding, firstRow));
            writeDone();
            return;
        }
        if (!pending.isEmpty()) {
            // Compressed here without waiting for the pages handed off before it, which a worker
            // polling for bodies takes up a moment after they are queued, and written after them.
            pending.add(new Pending(handoff.compressHere(compressor, body.array(), length), entries, length, encoding,
                    firstRow));
            writeDone();
            return;
        }
        // UNCOMPRESSED stores the body as it stands, so the page is written straight out of the
        // body buffer; every other codec produces its own array.
        byte[] stored = codec == CompressionCodec.UNCOMPRESSED ? body.array() : compressor.compress(body.array(), 0, length);
        int storedLength = codec == CompressionCodec.UNCOMPRESSED ? length : stored.length;
        CRC32 crc = new CRC32();
        crc.update(stored, 0, storedLength);
        // The body buffer is overwritten by the next page; a compressor's output is not.
        frame(entries, length, stored, storedLength, (int) crc.getValue(), encoding, firstRow,
                stored == body.array());
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
        frame(p.entries(), p.uncompressedLength(), page.stored(), page.stored().length, page.crc(), p.encoding(),
                p.firstRow(), false);
    }

    private void frame(int entries, int uncompressedLength, byte[] stored, int storedLength, int crc,
                       Encoding encoding, long firstRow, boolean reused) throws IOException {
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, entries, uncompressedLength, storedLength, crc, encoding);
        byte[] headerBytes = header.toByteArray();
        sink.write(headerBytes, 0, headerBytes.length, false);
        sink.write(stored, 0, storedLength, reused);
        long pageBytes = (long) headerBytes.length + storedLength;
        locations.add(new PageLocation(firstPageOffset + storedBytes, Math.toIntExact(pageBytes), firstRow));
        storedBytes += pageBytes;
        uncompressedBytes += headerBytes.length + uncompressedLength;
    }
}
