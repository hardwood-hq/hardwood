/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

/// The header and layout every data page of a file is written with, set through
/// [WriterConfig.Builder#dataPageVersion(DataPageVersion)].
///
/// Both versions hold the same levels and values under the same encodings; they differ in how a
/// page frames them. Dictionary pages are the same under either.
public enum DataPageVersion {

    /// `DATA_PAGE`: the repetition and definition levels and the values are compressed together
    /// as one body, each level stream prefixed by its length.
    V1,

    /// `DATA_PAGE_V2`: the levels are stored uncompressed ahead of the values, and the values are
    /// compressed where that makes them smaller. The header states each level stream's length,
    /// whether the values are compressed, and the page's row and null counts.
    V2
}
