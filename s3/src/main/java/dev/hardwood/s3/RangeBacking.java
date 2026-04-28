/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

/// How an [S3InputFile] caches fetched byte ranges. See #373 and the
/// `_designs/REMOTE_RANGE_BACKING.md` design doc.
public enum RangeBacking {

    /// No range cache. Every [S3InputFile#readRange] call issues an
    /// HTTP `GET` (the tail-cache from `open()` still applies). This is
    /// the default — no behaviour change for existing callers and no
    /// new failure modes (writeable temp dir, disk capacity).
    NONE,

    /// Whole-file cache backed by a sparse temp file mmapped into the
    /// process. Fetched byte ranges are written into the mapping;
    /// repeat reads slice the mapping zero-copy. Real footprint is
    /// proportional to bytes touched, not file size, on filesystems
    /// that support sparse files.
    ///
    /// Inherits the 2 GB cap from `MappedByteBuffer.slice(int, int)`;
    /// not usable for files larger than `Integer.MAX_VALUE` bytes.
    /// Opt in via [S3Source.Builder#rangeBacking] when the workload
    /// re-reads byte ranges (interactive `dive` is the canonical
    /// caller).
    SPARSE_TEMPFILE
}
