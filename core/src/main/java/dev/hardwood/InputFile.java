/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.ByteBufferInputFile;
import dev.hardwood.internal.reader.MappedInputFile;

/// Abstraction for reading Parquet file data.
///
/// This interface decouples the read pipeline from memory-mapped local files,
/// enabling alternative backends such as object stores or in-memory buffers.
///
/// An `InputFile` starts in an unopened state. The [#open()] method
/// must be called before [#readRange] or [#length] can be used.
/// The framework ([Hardwood], [dev.hardwood.reader.ParquetFileReader])
/// calls `open()` automatically; callers only need to create instances via
/// [#of(Path)] and close them when done.
///
/// Implementations must be safe for concurrent use from multiple threads once opened.
/// The returned [ByteBuffer] instances are owned by the caller and may
/// be slices of a larger mapping or freshly allocated buffers, depending on
/// the implementation.
///
/// @see dev.hardwood.reader.ParquetFileReader#open(InputFile)
public interface InputFile extends Closeable {

    /// Performs expensive resource acquisition (e.g. memory-mapping, network connect).
    /// Must be called before [#readRange] or [#length].
    ///
    /// @throws IOException if the resource cannot be acquired
    void open() throws IOException;

    /// Read a range of bytes from the file.
    ///
    /// @param offset the byte offset to start reading from
    /// @param length the number of bytes to read
    /// @return a [ByteBuffer] containing the requested data
    /// @throws IOException if the read fails
    /// @throws IllegalStateException if [#open()] has not been called
    /// @throws IndexOutOfBoundsException if offset or length is out of range
    ByteBuffer readRange(long offset, int length) throws IOException;

    /// Returns the total size of the file in bytes.
    ///
    /// @return the file size
    /// @throws IOException if the size cannot be determined
    /// @throws IllegalStateException if [#open()] has not been called
    long length() throws IOException;

    /// Returns an identifier for this file, used in log messages and JFR events.
    ///
    /// @return a human-readable name or path
    String name();

    /// Creates an [InputFile] backed by an in-memory [ByteBuffer].
    ///
    /// Since the data is already in memory, no resource acquisition is needed
    /// and [#open()] is a no-op.
    ///
    /// @param buffer the buffer containing Parquet file data
    /// @return a new InputFile backed by the buffer
    static InputFile of(ByteBuffer buffer) {
        return new ByteBufferInputFile(buffer);
    }

    /// Creates an unopened [InputFile] for a local file path.
    ///
    /// @param path the file to read
    /// @return a new unopened InputFile
    static InputFile of(Path path) {
        return new MappedInputFile(path);
    }

    /// Creates unopened [InputFile] instances for a list of local file paths.
    ///
    /// @param paths the files to read
    /// @return a list of new unopened InputFile instances
    static List<InputFile> ofPaths(List<Path> paths) {
        List<InputFile> files = new ArrayList<>(paths.size());
        for (Path p : paths) {
            files.add(of(p));
        }
        return files;
    }

    /// Creates [InputFile] instances for a list of in-memory [ByteBuffer]s.
    ///
    /// Since the data is already in memory, no resource acquisition is needed
    /// and [#open()] is a no-op for each instance.
    ///
    /// @param buffers the buffers containing Parquet file data
    /// @return a list of new InputFile instances backed by the buffers
    static List<InputFile> ofBuffers(List<ByteBuffer> buffers) {
        List<InputFile> files = new ArrayList<>(buffers.size());
        for (ByteBuffer b : buffers) {
            files.add(of(b));
        }
        return files;
    }
}
