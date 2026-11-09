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

import dev.hardwood.internal.writer.ChannelOutputFile;

/// Abstraction for writing Parquet file data.
///
/// This is the write-side counterpart to [InputFile]. Where reading requires
/// random access, the Parquet container is produced front to back and never
/// seeked backward: the footer carries every offset a reader needs and is
/// emitted last. An `OutputFile` is therefore a purely sequential sink.
///
/// An `OutputFile` is used for one file. It starts uncreated; [#create()] opens it,
/// and the writer framework ([dev.hardwood.writer.ParquetFileWriter]) calls it
/// automatically. [#write] and [#position] are allowed only while the file is open.
/// The file ends either published by [#close()] or thrown away by [#discard()], and
/// an ended file cannot be created again:
///
/// | Call | Uncreated | Open | Published | Discarded |
/// |---|---|---|---|---|
/// | `create()` | opens the file | throws | throws | throws |
/// | `write`, `position` | throws | allowed | throws | throws |
/// | `close()` | discards | publishes, or discards and throws | nothing | nothing |
/// | `discard()` | discards | discards | nothing | nothing |
///
/// The rejections are `IllegalStateException`s. A file is valid only after
/// [#close()] returns successfully. A writer abandoned before `close()` produces no
/// footer and therefore no readable file.
///
/// To implement an `OutputFile`, extend [AbstractOutputFile], which enforces this
/// lifecycle and leaves only the work at the destination to the subclass.
public interface OutputFile extends Closeable {

    /// Performs resource acquisition (e.g. opening a file channel).
    /// Must be called before [#write] or [#position].
    ///
    /// @throws IOException if the resource cannot be acquired
    /// @throws IllegalStateException if the file is open, published or discarded
    void create() throws IOException;

    /// Appends the remaining bytes of `data` to the file.
    ///
    /// The buffer is consumed (its position advances to its limit).
    ///
    /// @param data the bytes to append
    /// @throws IOException if the write fails
    /// @throws IllegalStateException if the file is not open
    void write(ByteBuffer data) throws IOException;

    /// Returns the number of bytes written so far, which is the offset at which
    /// the next [#write] will begin.
    ///
    /// @return the running byte offset
    /// @throws IllegalStateException if the file is not open
    long position();

    /// Finalizes the file and publishes it at the destination.
    ///
    /// When publishing fails, `close()` releases the resources and discards what was
    /// written before it throws, leaving the destination as if nothing was written. A
    /// failure to discard is attached to the thrown exception as suppressed.
    ///
    /// On a file that was never created, `close()` publishes nothing and ends the file
    /// as discarded. On a file that is already published or discarded, it does nothing.
    ///
    /// @throws IOException if the file cannot be published
    @Override
    void close() throws IOException;

    /// Discards everything written and releases resources without publishing a
    /// file at the destination. This is the failure counterpart to [#close()]:
    /// [#close()] finalizes (commits) the file, `discard()` throws it away. The
    /// writer calls `discard()` when it cannot finish a valid file, so a partially
    /// written file is never presented as valid. After `discard()` the destination
    /// is left as if nothing was written; calling `close()` afterwards is a no-op.
    /// On a file that is already published or discarded, `discard()` does nothing, so a
    /// published file stays published.
    ///
    /// @throws IOException if resources cannot be released
    void discard() throws IOException;

    /// Creates an uncreated [OutputFile] for a local file path.
    ///
    /// @param path the file to write
    /// @return a new uncreated OutputFile
    static OutputFile of(Path path) {
        return new ChannelOutputFile(path);
    }

    /// Creates an uncreated [OutputFile] that keeps the file in memory. The finished file is
    /// retrieved from [InMemoryOutputFile#buffer()] after the writer is closed.
    ///
    /// The buffer grows with the file, so no size has to be given up front; the length of a
    /// Parquet file is only known once it has been written.
    ///
    /// **This API is [Experimental]:** the shape may change in future releases.
    ///
    /// @return a new uncreated in-memory OutputFile
    @Experimental
    static InMemoryOutputFile inMemory() {
        return new InMemoryOutputFile();
    }
}
