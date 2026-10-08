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
import java.util.Objects;
import java.util.Optional;

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
/// calls `open()` automatically and takes ownership of the files passed to it:
/// the reader closes them when it is closed or when opening it fails. Callers
/// create instances via [#of(Path)] and close only files they have not handed
/// to a reader. [#close()] may be called on a file that was never opened.
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

    /// An identifier of the file's content, which differs for different content, if the
    /// backend can supply one without a request of its own; empty when it cannot.
    ///
    /// Resolved by [#open()] and fixed for the file's lifetime: it names the content the file was
    /// opened against, not the content at its location at the time of the call. A
    /// [dev.hardwood.metadata.ParsedFooter] records the identity of the file it was read from,
    /// and a reader compares the two before using a footer a [MetadataSource] supplied.
    ///
    /// | Backend | Identity |
    /// |---|---|
    /// | Local file ([#of(Path)]) | the file's size, modification time and file key (or absolute path where the file system has no file keys), read when it is opened |
    /// | S3 | the object's `ETag` |
    /// | In-memory ([#of(ByteBuffer)]) | empty |
    ///
    /// Implement it whenever the backend can supply an identity: a [MetadataSource] that keys
    /// its cache on it can only detect a replaced file through it, and an empty identity turns
    /// that detection off. An `InputFile` that wraps another returns the wrapped file's identity.
    /// The default implementation returns empty.
    ///
    /// @return the identity, or empty when the backend has none
    /// @throws IOException if the identity cannot be determined
    /// @throws IllegalStateException if the implementation resolves its identity in [#open()]
    ///         and [#open()] has not been called
    @Experimental
    default Optional<String> identity() throws IOException {
        return Optional.empty();
    }

    /// Creates an [InputFile] backed by an in-memory [ByteBuffer].
    ///
    /// The file is the buffer's remaining content, from its position to its limit,
    /// taken when this method is called. The content is not copied, and the buffer's
    /// position and limit are not changed; later changes to them do not affect the file.
    /// Since the data is already in memory, no resource acquisition is needed
    /// and [#open()] is a no-op.
    ///
    /// @param buffer the buffer containing Parquet file data between its position and limit
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

    /// Creates unopened [InputFile] instances for the given local file paths.
    ///
    /// @param first the first file to read
    /// @param more additional files to read
    /// @return a list of new unopened InputFile instances
    static List<InputFile> ofPaths(Path first, Path... more) {
        Objects.requireNonNull(first, "first path must not be null");
        List<InputFile> files = new ArrayList<>(1 + more.length);
        files.add(of(first));
        for (Path p : more) {
            Objects.requireNonNull(p, "path must not be null");
            files.add(of(p));
        }
        return files;
    }

    /// Creates [InputFile] instances for a list of in-memory [ByteBuffer]s.
    /// Each file is its buffer's remaining content, as described in [#of(ByteBuffer)].
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

    /// Creates [InputFile] instances for the given in-memory [ByteBuffer]s.
    /// Each file is its buffer's remaining content, as described in [#of(ByteBuffer)].
    ///
    /// Since the data is already in memory, no resource acquisition is needed
    /// and [#open()] is a no-op for each instance.
    ///
    /// @param first the first buffer containing Parquet file data
    /// @param more additional buffers containing Parquet file data
    /// @return a list of new InputFile instances backed by the buffers
    static List<InputFile> ofBuffers(ByteBuffer first, ByteBuffer... more) {
        Objects.requireNonNull(first, "first buffer must not be null");
        List<InputFile> files = new ArrayList<>(1 + more.length);
        files.add(of(first));
        for (ByteBuffer b : more) {
            Objects.requireNonNull(b, "buffer must not be null");
            files.add(of(b));
        }
        return files;
    }
}
