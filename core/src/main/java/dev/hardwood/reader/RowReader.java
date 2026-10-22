/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.Closeable;
import java.io.IOException;

import dev.hardwood.row.StructAccessor;

/// Provides row-oriented iteration over a Parquet file.
///
/// A `RowReader` is a stateful, mutable view providing access to the current row
/// in the iterator: its accessors read the row [#next()] moved to. A value an accessor
/// has returned does not change when the reader moves on, and can be held. A binary
/// value (`getBinary`, or `getRawValue` of a binary column) is a new `byte[]` per call,
/// which the caller owns. A [dev.hardwood.row.PqStruct], [dev.hardwood.row.PqList] or
/// [dev.hardwood.row.PqMap] reads the batch of rows it was obtained from, and keeps that
/// batch in memory while it is reachable; to keep a few values of many rows, copy them
/// out instead. A value can be handed to another thread and read
/// there while this reader moves on; neither this reader nor a value is safe for use by
/// several threads at once.
///
/// Usage example:
/// ```java
/// try (RowReader rowReader = fileReader.rowReader()) {
///     while (rowReader.hasNext()) {
///         rowReader.next();
///         long id = rowReader.getLong("id");
///         PqStruct address = rowReader.getStruct("address");
///         String city = address.getString("city");
///     }
/// }
/// ```
public interface RowReader extends StructAccessor, Closeable {

    /// Check if there are more rows to read.
    ///
    /// Reaches the file when the current batch runs out, so it can fail the way
    /// any read can.
    ///
    /// @return true if there are more rows available
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say: a footer or a page index that will not parse, a
    ///         dictionary page the metadata places outside its column chunk, a page whose
    ///         checksum fails, values that do not decode under the encoding declared for
    ///         them. In a multi-file read this covers a later file that is not Parquet at
    ///         all, or whose schema cannot be reconciled with the first file's
    boolean hasNext() throws IOException;

    /// Advance to the next row. Must be called before accessing row data.
    /// Call [#hasNext()] before every `next()`: `next()` does not load rows
    /// itself.
    ///
    /// @throws java.util.NoSuchElementException if no row is available to `next()`
    ///         without a preceding [#hasNext()]: at the end of the current batch, or on
    ///         a filtered read when `hasNext()` has not selected the next matching row
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say: a footer or a page index that will not parse, a
    ///         dictionary page the metadata places outside its column chunk, a page whose
    ///         checksum fails, values that do not decode under the encoding declared for
    ///         them. In a multi-file read this covers a later file that is not Parquet at
    ///         all, or whose schema cannot be reconciled with the first file's
    void next() throws IOException;

    /// Releases the resources held by this reader. Idempotent: calling it more
    /// than once has no further effect, and a call on another thread while a close runs, such
    /// as the [ParquetFileReader]'s, returns once that close has completed.
    ///
    /// @throws IOException if a file this reader owns could not be closed
    @Override
    void close() throws IOException;
}
