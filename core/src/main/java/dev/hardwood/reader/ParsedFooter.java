/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.util.Optional;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.schema.FileSchema;

/// A parsed Parquet footer together with optional content-identity information.
///
/// Callers that open many readers over the same file can hoist the footer parse
/// out of the per-read path by installing a [MetadataSource] on a
/// [dev.hardwood.HardwoodContext]. Every `open` or `openAll` call on that context
/// then obtains the footer through the source rather than re-reading it from disk.
///
/// A `ParsedFooter` is read-only once created. The [FileMetaData] it carries is
/// not deeply immutable — `Statistics` holds `byte[]` bounds — so a shared
/// instance must not be mutated after being returned from a [MetadataSource].
///
/// @see MetadataSource
/// @see dev.hardwood.HardwoodContext#builder()
public final class ParsedFooter {

    private final ReadFooter readFooter;
    private final FileSchema schema;
    private final long footerLength;
    private final long fileLength;
    private final Optional<String> sourceIdentity;

    private ParsedFooter(ReadFooter readFooter, FileSchema schema,
                         long footerLength, long fileLength,
                         Optional<String> sourceIdentity) {
        this.readFooter = readFooter;
        this.schema = schema;
        this.footerLength = footerLength;
        this.fileLength = fileLength;
        this.sourceIdentity = sourceIdentity;
    }

    /// The Parquet file metadata (row groups, column chunks, key-value metadata, ...).
    public FileMetaData metaData() {
        return readFooter.metaData();
    }

    /// The schema derived from [#metaData()].
    public FileSchema schema() {
        return schema;
    }

    /// Size of the serialised Thrift footer in bytes, not including the 4-byte
    /// length prefix and trailing magic.
    public long footerLength() {
        return footerLength;
    }

    /// Total file size at the time the footer was read.
    public long fileLength() {
        return fileLength;
    }

    /// A stable content identity for the file this footer was read from, if the
    /// [InputFile] implementation supports it.
    ///
    /// hardwood uses this to detect staleness: if a [MetadataSource] returns a
    /// `ParsedFooter` whose `sourceIdentity` is present and different from the
    /// current [InputFile#identity()], a [StaleMetadataException] is thrown before
    /// any data is read.
    ///
    /// @see InputFile#identity()
    /// @see StaleMetadataException
    public Optional<String> sourceIdentity() {
        return sourceIdentity;
    }

    /// The underlying [ReadFooter], carrying the [FileMetaData] and any
    /// logical-type-unread flags set during Thrift parsing.
    ///
    /// Used internally by [dev.hardwood.reader.ParquetFileReader] to pass through
    /// the full footer to the file-metadata cache.
    ReadFooter readFooter() {
        return readFooter;
    }

    /// Read and parse the footer from an already-opened [InputFile], producing a
    /// `ParsedFooter` that can be returned from a [MetadataSource].
    ///
    /// @param openFile an [InputFile] on which [InputFile#open()] has already been called
    public static ParsedFooter readFrom(InputFile openFile) throws IOException {
        ParquetMetadataReader.MetadataWithLength result =
                ParquetMetadataReader.readMetadataWithLength(openFile);
        ReadFooter rf = result.readFooter();
        FileSchema schema = FileSchema.fromSchemaElements(rf.metaData().schema());
        Optional<String> identity = openFile.identity();
        return new ParsedFooter(rf, schema, result.footerLength(),
                openFile.length(), identity);
    }

    /// Wrap an already-parsed [FileMetaData] without performing any I/O.
    ///
    /// For new code that obtains the footer for the first time, prefer
    /// [#readFrom(InputFile)] — it populates all fields and captures the content
    /// identity in one step. Use this factory when you already hold a `FileMetaData`
    /// and want to install it in a [MetadataSource].
    ///
    /// @param metaData     the Parquet file metadata
    /// @param schema       the schema derived from `metaData`
    /// @param footerLength the serialised Thrift footer length in bytes; used by
    ///                     [dev.hardwood.internal.reader.ParquetMetadataReader#validateTrailer]
    ///                     to confirm the file has not been replaced
    /// @param fileLength   the total file size at the time `metaData` was read; must be
    ///                     accurate — it is the primary staleness signal in the trailer check
    /// @param identity     a stable content identity, or empty if none is available
    public static ParsedFooter of(FileMetaData metaData, FileSchema schema,
                                   long footerLength, long fileLength,
                                   Optional<String> identity) {
        if (metaData == null) throw new IllegalArgumentException("metaData must not be null");
        if (schema == null) throw new IllegalArgumentException("schema must not be null");
        if (identity == null) throw new IllegalArgumentException("identity must not be null");
        // Wrap the FileMetaData with an empty logicalTypeUnread set. The original
        // Thrift parse result is not recoverable from a raw FileMetaData, so we
        // supply a conservative empty set. Prefer readFrom() for full fidelity.
        return new ParsedFooter(FileMetaDataReader.wrap(metaData), schema,
                footerLength, fileLength, identity);
    }
}
