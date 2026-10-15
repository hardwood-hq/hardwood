/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import dev.hardwood.internal.reader.PqFileImpl;

/// Value of a Parquet column annotated with the `FILE` logical type: a reference to a range of
/// bytes, stored inline in the value or in an external object named by a URI.
///
/// The factories build only references the format defines:
///
/// | Factory | Refers to |
/// |---|---|
/// | [#inline(byte\[\])] | the given bytes |
/// | [#external(String)] | the whole object at `uri` |
/// | [#external(String, long)] | bytes `[0, size)` of the object at `uri` |
/// | [#external(String, long, long)] | bytes `[offset, offset + size)` of the object at `uri` |
///
/// [#withInline], [#withContentType] and [#withChecksum] return a copy with one more field set.
///
/// ```java
/// PqFile photo = PqFile.external("s3://bucket/bundle.tar", 4096, 51200)
///         .withContentType("image/png");
/// ```
///
/// A `FILE` value is not resolved: fetching the object at [#uri()] and verifying [#checksum()]
/// are left to the caller.
///
/// The values the factories and the reader return are immutable: the factories copy the arrays
/// they are given, and [#inline()] returns a copy on each call. Two such values are equal when
/// their accessors return equal values, the inline bytes compared by content; neither is equal
/// to an instance of another implementation. A value the reader returns can be held after the
/// reader moves on; while it is reachable, the inline bytes of every value in the batch it was
/// read from stay in memory. A writer checks every value it is given against the format's rules,
/// whatever implements it.
public interface PqFile {

    /// A reference to bytes stored in the value itself.
    ///
    /// @param bytes the referenced bytes, possibly empty
    /// @throws NullPointerException if `bytes` is `null`
    static PqFile inline(byte[] bytes) {
        return PqFileImpl.inline(bytes);
    }

    /// A reference to the whole object at `uri`.
    ///
    /// @param uri an RFC 3986 URI-reference, absolute or relative, stored as given
    /// @throws NullPointerException if `uri` is `null`
    /// @throws IllegalArgumentException if `uri` is empty
    static PqFile external(String uri) {
        return PqFileImpl.external(uri);
    }

    /// A reference to the first `size` bytes of the object at `uri`.
    ///
    /// @param uri an RFC 3986 URI-reference, absolute or relative, stored as given
    /// @param size the number of bytes referenced, `0` for none
    /// @throws NullPointerException if `uri` is `null`
    /// @throws IllegalArgumentException if `uri` is empty or `size` is negative
    static PqFile external(String uri, long size) {
        return PqFileImpl.external(uri, size);
    }

    /// A reference to `size` bytes of the object at `uri`, starting at `offset`.
    ///
    /// @param uri an RFC 3986 URI-reference, absolute or relative, stored as given
    /// @param offset the position of the first byte referenced
    /// @param size the number of bytes referenced, `0` for none
    /// @throws NullPointerException if `uri` is `null`
    /// @throws IllegalArgumentException if `uri` is empty, or `offset` or `size` is negative
    static PqFile external(String uri, long offset, long size) {
        return PqFileImpl.external(uri, offset, size);
    }

    /// Returns a copy of this reference that also stores `bytes` inline. Combined with
    /// [#external] it records where inline bytes came from; the two are expected to denote the
    /// same bytes, which is not checked.
    ///
    /// @throws NullPointerException if `bytes` is `null`
    /// @throws IllegalStateException if this reference already stores bytes inline
    default PqFile withInline(byte[] bytes) {
        return PqFileImpl.copyOf(this).withInline(bytes);
    }

    /// Returns a copy of this reference with the media type of the referenced bytes.
    ///
    /// @param mediaType an RFC 2046 media type, such as `image/png`
    /// @throws NullPointerException if `mediaType` is `null`
    /// @throws IllegalArgumentException if `mediaType` is empty
    /// @throws IllegalStateException if this reference already has a content type
    default PqFile withContentType(String mediaType) {
        return PqFileImpl.copyOf(this).withContentType(mediaType);
    }

    /// Returns a copy of this reference with an integrity token for the referenced bytes, of
    /// the form `<algorithm>:<digest>`, such as `SHA-256:…` or an object store's `ETAG:…`. The
    /// token is stored as given and not verified against any bytes. The writer refuses a token
    /// not of that form, and one whose digest for `MD5`, `CRC32`, `CRC32C` or `SHA-256` is not
    /// lowercase hex of that algorithm's length.
    ///
    /// @throws NullPointerException if `token` is `null`
    /// @throws IllegalArgumentException if `token` is empty
    /// @throws IllegalStateException if this reference already has a checksum
    default PqFile withChecksum(String token) {
        return PqFileImpl.copyOf(this).withChecksum(token);
    }

    /// The URI of the referenced object, or `null` if the reference has none.
    String uri();

    /// Whether the reference stores an offset into the object at [#uri()].
    boolean hasOffset();

    /// The position of the first referenced byte in the object at [#uri()]; `0` when the
    /// reference stores no offset.
    long offset();

    /// Whether the reference stores the number of bytes referenced. A reference to a whole
    /// object stores none.
    boolean hasSize();

    /// The number of bytes referenced in the object at [#uri()].
    ///
    /// @throws IllegalStateException if the reference stores no size, which [#hasSize()] answers
    long size();

    /// The media type of the referenced bytes, or `null` if the reference has none. The format
    /// states that a reference without one can be assumed to be `application/octet-stream`.
    String contentType();

    /// The integrity token for the referenced bytes, as stored, or `null` if the reference has
    /// none.
    String checksum();

    /// A copy of the bytes stored inline, or `null` if the reference stores none.
    byte[] inline();
}
