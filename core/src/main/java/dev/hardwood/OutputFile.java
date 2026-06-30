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
/// An `OutputFile` starts in an uncreated state. [#create()] must be called
/// before [#write] or [#position]; the writer framework
/// ([dev.hardwood.writer.ParquetFileWriter]) calls it automatically.
///
/// A file is valid only after [#close()] returns successfully. A writer
/// abandoned before `close()` produces no footer and therefore no readable file.
public interface OutputFile extends Closeable {

    /// Performs resource acquisition (e.g. opening a file channel).
    /// Must be called before [#write] or [#position].
    ///
    /// @throws IOException if the resource cannot be acquired
    void create() throws IOException;

    /// Appends the remaining bytes of `data` to the file.
    ///
    /// The buffer is consumed (its position advances to its limit).
    ///
    /// @param data the bytes to append
    /// @throws IOException if the write fails
    /// @throws IllegalStateException if [#create()] has not been called
    void write(ByteBuffer data) throws IOException;

    /// Returns the number of bytes written so far, which is the offset at which
    /// the next [#write] will begin.
    ///
    /// @return the running byte offset
    /// @throws IllegalStateException if [#create()] has not been called
    long position();

    /// Discards everything written and releases resources without publishing a
    /// file at the destination. This is the failure counterpart to [#close()]:
    /// [#close()] finalizes (commits) the file, `discard()` throws it away. The
    /// writer calls `discard()` when it cannot finish a valid file, so a partially
    /// written file is never presented as valid. After `discard()` the destination
    /// is left as if nothing was written; calling `close()` afterwards is a no-op.
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
}
