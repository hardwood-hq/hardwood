/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.row.PqFile;

/// The [PqFile] the reader returns: the reference's small fields, read when the value is built,
/// and a position in the batch that holds its inline bytes, which [#inline()] copies out on each
/// call. A batch's arrays are not refilled once a later batch is read, so the value can be held
/// after the reader moves on; it keeps that batch's binary values reachable while it is.
final class PqFileView implements PqFile {

    private final String uri;
    private final long offset;
    private final long size;
    private final String contentType;
    private final String checksum;
    /// The batch values holding the inline bytes, their views already built, or `null` for none.
    private final BinaryBatchValues inlineValues;
    private final int inlineIndex;

    /// @param offset the offset, [PqFileImpl#NOT_SET] when unset
    /// @param size the size, [PqFileImpl#NOT_SET] when unset
    PqFileView(String uri, long offset, long size, String contentType, String checksum,
               BinaryBatchValues inlineValues, int inlineIndex) {
        this.uri = uri;
        this.offset = offset;
        this.size = size;
        this.contentType = contentType;
        this.checksum = checksum;
        this.inlineValues = inlineValues;
        this.inlineIndex = inlineIndex;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public boolean hasOffset() {
        return offset != PqFileImpl.NOT_SET;
    }

    @Override
    public long offset() {
        return offset != PqFileImpl.NOT_SET ? offset : 0;
    }

    @Override
    public boolean hasSize() {
        return size != PqFileImpl.NOT_SET;
    }

    @Override
    public long size() {
        if (size == PqFileImpl.NOT_SET) {
            throw new IllegalStateException("This FILE reference stores no size");
        }
        return size;
    }

    @Override
    public String contentType() {
        return contentType;
    }

    @Override
    public String checksum() {
        return checksum;
    }

    @Override
    public byte[] inline() {
        return inlineValues != null ? inlineValues.byteArrayAt(inlineIndex) : null;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PqFile other && (other instanceof PqFileView || other instanceof PqFileImpl)
                && PqFileImpl.equal(this, other);
    }

    @Override
    public int hashCode() {
        return PqFileImpl.hash(uri, offset, size, contentType, checksum, inlineHash());
    }

    /// `Arrays.hashCode` of the inline bytes, computed where they sit rather than over a copy.
    private int inlineHash() {
        if (inlineValues == null) {
            return 0;
        }
        byte[] bytes = inlineValues.buffer(inlineIndex);
        int hash = 1;
        for (int i = inlineValues.starts()[inlineIndex], end = inlineValues.ends()[inlineIndex]; i < end; i++) {
            hash = 31 * hash + bytes[i];
        }
        return hash;
    }

    @Override
    public String toString() {
        return PqFileImpl.render(this, inlineValues != null
                ? inlineValues.ends()[inlineIndex] - inlineValues.starts()[inlineIndex]
                : -1);
    }
}
