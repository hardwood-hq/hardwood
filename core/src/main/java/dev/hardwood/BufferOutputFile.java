/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;

/// An [OutputFile] that keeps the file in memory and hands it back.
///
/// Use it to produce Parquet bytes without a filesystem: a file to upload to an object store,
/// a payload to send over a network, or one written and read back in the same process. It is
/// created with [OutputFile#inMemory()], and the buffer [#buffer()] returns can be passed
/// straight to [InputFile#of(ByteBuffer)]:
///
/// ```java
/// BufferOutputFile out = OutputFile.inMemory();
/// try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
///     // write the file
/// }
/// try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
///     // read it back
/// }
/// ```
///
/// The whole file is held on the heap, which costs the size of the finished file in addition
/// to the memory the writer uses for the row group it has open.
public interface BufferOutputFile extends OutputFile {

    /// The file that was written.
    ///
    /// The returned buffer contains the whole file and nothing else: its position is `0` and
    /// its limit and capacity are the number of bytes written, so it can be passed to
    /// [InputFile#of(ByteBuffer)] unchanged. It is a view of the written bytes rather than a
    /// copy, and each call returns a new view, so reading one buffer does not affect another.
    ///
    /// @return the bytes of the finished file
    /// @throws IllegalStateException if the file has not been closed, or was discarded
    ByteBuffer buffer();
}
