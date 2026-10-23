/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;

/// A file of bytes in memory that behaves as the S3 backend does towards the reader: its open is
/// one request that fetches the last `tailSize` bytes, or `suppliedTailSize` when the footers are
/// supplied, and holds them, a read inside the tail issues no request, and every other read is one
/// request. The reader treats it as remote, so it reads ahead.
class TailFile implements InputFile, TailHolder {

    /// One request: the bytes `[offset, offset + length)`, and the [FetchReason] in effect.
    record Request(long offset, int length, String reason) {
    }

    private final ByteBuffer bytes;
    private final String name;
    private final int tailSize;
    private final int suppliedTailSize;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger releases = new AtomicInteger();
    private volatile ByteBuffer tail;
    private long tailOffset;
    private long length = -1;
    private volatile boolean openedForSuppliedFooter;

    TailFile(byte[] bytes, String name, int tailSize, int suppliedTailSize) {
        this.bytes = ByteBuffer.wrap(bytes);
        this.name = name;
        this.tailSize = tailSize;
        this.suppliedTailSize = suppliedTailSize;
    }

    /// Every request so far, the open's first.
    List<Request> requests() {
        return List.copyOf(requests);
    }

    /// The bytes the open fetched.
    int openBytes() {
        return requests.getFirst().length();
    }

    boolean openedForSuppliedFooter() {
        return openedForSuppliedFooter;
    }

    /// How often the tail was released while held.
    int releases() {
        return releases.get();
    }

    @Override
    public void open() throws IOException {
        open(false);
    }

    @Override
    public void open(boolean footerSupplied) throws IOException {
        if (length >= 0) {
            return;
        }
        length = bytes.remaining();
        int fetched = Math.toIntExact(Math.min(tailReservation(footerSupplied), length));
        tailOffset = length - fetched;
        requests.add(new Request(tailOffset, fetched, "open-tail"));
        ByteBuffer held = ByteBuffer.allocateDirect(fetched);
        held.put(bytes.slice(Math.toIntExact(tailOffset), fetched));
        held.flip();
        openedForSuppliedFooter = footerSupplied;
        tail = held;
    }

    @Override
    public long tailReservation(boolean footerSupplied) {
        return footerSupplied ? Math.min(tailSize, suppliedTailSize) : tailSize;
    }

    /// The bytes the tail holds; `0` before the open and once released.
    long tailBytes() {
        ByteBuffer held = tail;
        return held == null ? 0 : held.capacity();
    }

    @Override
    public void releaseTail() {
        if (tail != null) {
            releases.incrementAndGet();
        }
        tail = null;
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        ReadRanges.checkBounds(name, offset, length, this.length);
        ByteBuffer held = tail;
        if (held != null && offset >= tailOffset && offset + length <= tailOffset + held.capacity()) {
            return held.slice(Math.toIntExact(offset - tailOffset), length);
        }
        requests.add(new Request(offset, length, FetchReason.current()));
        return bytes.slice(Math.toIntExact(offset), length);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        releaseTail();
    }
}
