/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.Objects;

import dev.hardwood.row.PqFile;

/// The immutable [PqFile] the factories and the reader return. Byte arrays are copied on the way
/// in and on the way out.
public final class PqFileImpl implements PqFile {

    /// The `offset` or `size` of a reference that stores none.
    static final long NOT_SET = -1;

    private final String uri;
    private final long offset;
    private final long size;
    private final String contentType;
    private final String checksum;
    private final byte[] inline;

    private PqFileImpl(String uri, long offset, long size, String contentType, String checksum, byte[] inline) {
        this.uri = uri;
        this.offset = offset;
        this.size = size;
        this.contentType = contentType;
        this.checksum = checksum;
        this.inline = inline;
    }

    public static PqFileImpl inline(byte[] bytes) {
        return new PqFileImpl(null, NOT_SET, NOT_SET, null, null, copyOf(bytes, "bytes"));
    }

    public static PqFileImpl external(String uri) {
        return new PqFileImpl(requireText(uri, "uri"), NOT_SET, NOT_SET, null, null, null);
    }

    public static PqFileImpl external(String uri, long size) {
        return new PqFileImpl(requireText(uri, "uri"), NOT_SET, requireNonNegative(size, "size"), null, null, null);
    }

    public static PqFileImpl external(String uri, long offset, long size) {
        return new PqFileImpl(requireText(uri, "uri"), requireNonNegative(offset, "offset"),
                requireNonNegative(size, "size"), null, null, null);
    }

    /// `file` itself when it is a [PqFileImpl], otherwise a copy of the fields its accessors
    /// return. Whether they form a valid reference is checked where the value is written; a
    /// negative offset or size is refused here, since it would read back as unset.
    ///
    /// @throws IllegalArgumentException if `file` stores a negative offset or size
    public static PqFileImpl copyOf(PqFile file) {
        if (file instanceof PqFileImpl impl) {
            return impl;
        }
        return new PqFileImpl(file.uri(), file.hasOffset() ? requireNonNegative(file.offset(), "offset") : NOT_SET,
                file.hasSize() ? requireNonNegative(file.size(), "size") : NOT_SET, file.contentType(),
                file.checksum(), file.inline());
    }

    @Override
    public PqFile withInline(byte[] bytes) {
        requireUnset(inline, "inline");
        return new PqFileImpl(uri, offset, size, contentType, checksum, copyOf(bytes, "bytes"));
    }

    @Override
    public PqFile withContentType(String mediaType) {
        requireUnset(contentType, "content_type");
        return new PqFileImpl(uri, offset, size, requireText(mediaType, "mediaType"), checksum, inline);
    }

    @Override
    public PqFile withChecksum(String token) {
        requireUnset(checksum, "checksum");
        return new PqFileImpl(uri, offset, size, contentType, requireText(token, "token"), inline);
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public boolean hasOffset() {
        return offset != NOT_SET;
    }

    @Override
    public long offset() {
        return offset != NOT_SET ? offset : 0;
    }

    @Override
    public boolean hasSize() {
        return size != NOT_SET;
    }

    @Override
    public long size() {
        if (size == NOT_SET) {
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
        return inline != null ? inline.clone() : null;
    }

    /// The stored inline bytes themselves, or `null`, for the writer, which keeps a reference to
    /// them until the record is written. The value never modifies them, and the caller must not.
    public byte[] sharedInline() {
        return inline;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PqFileImpl other) {
            return offset == other.offset
                    && size == other.size
                    && Objects.equals(uri, other.uri)
                    && Objects.equals(contentType, other.contentType)
                    && Objects.equals(checksum, other.checksum)
                    && Arrays.equals(inline, other.inline);
        }
        return o instanceof PqFileView other && equal(this, other);
    }

    @Override
    public int hashCode() {
        return hash(uri, offset, size, contentType, checksum, Arrays.hashCode(inline));
    }

    @Override
    public String toString() {
        return render(this, inline != null ? inline.length : -1);
    }

    /// Whether two references are equal: every accessor returns equal values, the inline bytes
    /// compared by content. The library's own values, [PqFileImpl] and [PqFileView], compare this
    /// way with each other and are never equal to another implementation, which specifies no
    /// `equals` or `hashCode` to agree with.
    static boolean equal(PqFile a, PqFile b) {
        return a.hasOffset() == b.hasOffset() && a.offset() == b.offset()
                && a.hasSize() == b.hasSize() && (!a.hasSize() || a.size() == b.size())
                && Objects.equals(a.uri(), b.uri())
                && Objects.equals(a.contentType(), b.contentType())
                && Objects.equals(a.checksum(), b.checksum())
                && Arrays.equals(a.inline(), b.inline());
    }

    /// The hash code of a reference whose fields are these, `offset` and `size` being
    /// [#NOT_SET] when unset and `inlineHash` the `Arrays.hashCode` of the inline bytes, so that
    /// equal references hash alike whichever class holds them.
    static int hash(String uri, long offset, long size, String contentType, String checksum, int inlineHash) {
        return 31 * Objects.hash(uri, offset, size, contentType, checksum) + inlineHash;
    }

    /// Renders the fields `file` sets, the inline bytes by their length, `-1` for none.
    static String render(PqFile file, int inlineLength) {
        StringBuilder sb = new StringBuilder("PqFile{");
        String separator = "";
        if (file.uri() != null) {
            sb.append("uri=").append(file.uri());
            separator = ", ";
        }
        if (file.hasOffset()) {
            sb.append(separator).append("offset=").append(file.offset());
            separator = ", ";
        }
        if (file.hasSize()) {
            sb.append(separator).append("size=").append(file.size());
            separator = ", ";
        }
        if (file.contentType() != null) {
            sb.append(separator).append("contentType=").append(file.contentType());
            separator = ", ";
        }
        if (file.checksum() != null) {
            sb.append(separator).append("checksum=").append(file.checksum());
            separator = ", ";
        }
        if (inlineLength >= 0) {
            sb.append(separator).append("inline=").append(inlineLength).append(" bytes");
        }
        return sb.append('}').toString();
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative: " + value);
        }
        return value;
    }

    private static byte[] copyOf(byte[] bytes, String name) {
        return Objects.requireNonNull(bytes, name).clone();
    }

    private static void requireUnset(Object field, String name) {
        if (field != null) {
            throw new IllegalStateException("This FILE reference already has " + name);
        }
    }
}
