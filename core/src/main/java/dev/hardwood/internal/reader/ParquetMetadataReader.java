/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.EncryptedFileException;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.schema.BareRepeatedGroups;
import dev.hardwood.internal.thrift.FileMetaDataReader;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.ParsedFooter;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.reader.StaleMetadataException.Check;
import dev.hardwood.schema.FileSchema;

/// Utility class for reading Parquet file metadata from an [InputFile].
///
/// This centralizes the metadata reading logic used by ParquetFileReader
/// (for the first file) and FileMetadataCache (for the further files of a
/// multi-file read), including taking a footer from a [MetadataSource] and
/// checking it against the file it is used for.
public final class ParquetMetadataReader {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    /// Magic written in place of [#MAGIC] when the footer itself is encrypted
    /// (Parquet Modular Encryption, encrypted-footer mode).
    private static final byte[] ENCRYPTED_MAGIC = "PARE".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    /// Named once because two places raise it: the magic-byte check here, and the
    /// `encryption_algorithm` field a plaintext footer carries.
    public static final String ENCRYPTED_MESSAGE =
            "Encrypted Parquet files are not supported (Parquet Modular Encryption)";

    /// The source of a context with none installed: every footer is read from its own file.
    /// [#load] recognizes it and reads directly, skipping the checks a supplied footer needs,
    /// since a footer read from the file describes that file by construction.
    public static final MetadataSource FROM_FILE = ParsedFooter::readFrom;

    private ParquetMetadataReader() {
        // Utility class
    }

    /// Reads file metadata from an [InputFile].
    ///
    /// @param inputFile the input file to read metadata from
    /// @return the parsed FileMetaData
    /// @throws IOException if the file cannot be read
    /// @throws ParquetReadException if what it holds is not a Parquet file
    public static FileMetaData readMetadata(InputFile inputFile) throws IOException {
        long fileSize = inputFile.length();
        return parse(inputFile, readTrailer(inputFile, fileSize), fileSize).metaData();
    }

    /// Reads the footer of an [InputFile] and derives the reader's schema from it.
    ///
    /// A failure of the parse or of the schema derivation is a read failure of the file and
    /// names it.
    ///
    /// @param inputFile the opened input file to read the footer from
    /// @return the footer, its schema and lengths
    /// @throws IOException if the file cannot be read
    /// @throws ParquetReadException if what it holds is not a Parquet file
    public static FileFooter read(InputFile inputFile) throws IOException {
        try {
            long fileSize = inputFile.length();
            int footerLength = readTrailer(inputFile, fileSize);
            ReadFooter footer = parse(inputFile, footerLength, fileSize);
            return new FileFooter(footer, schemaOf(footer.metaData()), footerLength, fileSize);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(inputFile.name(), ExceptionContext.asReadFailure(e));
        }
    }

    /// The schema the reader reads a file with: the footer's schema elements, less the
    /// annotations [BareRepeatedGroups] drops. Every footer the reader uses passes through here,
    /// whether read by the reader or supplied by a [MetadataSource].
    public static FileSchema schemaOf(FileMetaData metaData) {
        return FileSchema.fromSchemaElements(BareRepeatedGroups.dropAnnotations(metaData.schema()));
    }

    /// The footer of an opened file: read from the file for [#FROM_FILE], otherwise taken from
    /// `source` and checked against the file.
    ///
    /// @param inputFile the opened input file
    /// @param source the context's source, [#FROM_FILE] when none is installed
    /// @throws StaleMetadataException if the supplied footer does not describe the file
    public static FileFooter load(InputFile inputFile, MetadataSource source) throws IOException {
        if (source == FROM_FILE) {
            return read(inputFile);
        }
        ParsedFooter supplied;
        try {
            supplied = source.footerOf(inputFile);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(inputFile.name(), e);
        }
        if (supplied == null) {
            throw new IllegalStateException(ExceptionContext.filePrefix(inputFile.name())
                    + "MetadataSource returned no footer");
        }
        FileFooter footer = ((ParsedFooterImpl) supplied).fileFooter();
        try {
            StaleCheck stale = new StaleCheck(inputFile.name(), supplied.sourceIdentity(), inputFile.identity());
            long fileSize = inputFile.length();
            checkIdentity(stale);
            checkStructure(footer, fileSize, stale);
            checkTrailer(inputFile, fileSize, footer, stale);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(inputFile.name(), ExceptionContext.asReadFailure(e));
        }
        return footer;
    }

    /// The file and the two identities every [StaleMetadataException] of one load names.
    private record StaleCheck(String fileName, Optional<String> sourceIdentity, Optional<String> fileIdentity) {

        StaleMetadataException failure(Check check, String detail) {
            return new StaleMetadataException(fileName, check, sourceIdentity, fileIdentity, detail);
        }
    }

    private static void checkIdentity(StaleCheck stale) throws StaleMetadataException {
        if (stale.sourceIdentity().isPresent() && stale.fileIdentity().isPresent()
                && !stale.sourceIdentity().equals(stale.fileIdentity())) {
            throw stale.failure(Check.IDENTITY, "it was read from content with identity '"
                    + stale.sourceIdentity().get() + "', the file has identity '"
                    + stale.fileIdentity().get() + "'");
        }
    }

    /// No byte range the footer locates lies past the end of the file. Costs no I/O.
    private static void checkStructure(FileFooter footer, long fileSize, StaleCheck stale)
            throws StaleMetadataException {
        long end = maxLocatedByte(footer.metaData());
        if (end > fileSize) {
            throw stale.failure(Check.STRUCTURE, "it locates data up to byte " + end
                    + ", past the end of the file at " + fileSize);
        }
    }

    /// The file length and the footer length the file's trailer records, against those the
    /// footer was read with. The trailer is the last eight bytes, which a remote file pre-fetches
    /// on open and a mapped file has in memory.
    private static void checkTrailer(InputFile inputFile, long fileSize, FileFooter footer, StaleCheck stale)
            throws IOException {
        if (fileSize != footer.fileLength()) {
            throw stale.failure(Check.TRAILER, "it was read from a file of " + footer.fileLength()
                    + " bytes, the file has " + fileSize);
        }
        int footerLength = readTrailer(inputFile, fileSize);
        if (footerLength != footer.footerLength()) {
            throw stale.failure(Check.TRAILER, "it is " + footer.footerLength()
                    + " bytes long, the file's trailer records " + footerLength);
        }
    }

    /// The end of the furthest byte range `metaData` locates in its own file: column chunks,
    /// page indexes and bloom filters. Chunks stored in another file are skipped.
    private static long maxLocatedByte(FileMetaData metaData) {
        long max = 0;
        for (RowGroup rowGroup : metaData.rowGroups()) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                max = Math.max(max, maxLocatedByte(chunk));
            }
        }
        return max;
    }

    private static long maxLocatedByte(ColumnChunk chunk) {
        if (chunk.metaData() == null || !chunk.filePath().isEmpty()) {
            return 0;
        }
        ColumnMetaData metaData = chunk.metaData();
        long max = saturatedAdd(chunk.chunkStartOffset(), metaData.totalCompressedSize());
        max = Math.max(max, end(chunk.offsetIndexOffset(), chunk.offsetIndexLength()));
        max = Math.max(max, end(chunk.columnIndexOffset(), chunk.columnIndexLength()));
        return Math.max(max, end(metaData.bloomFilterOffset(), metaData.bloomFilterLength()));
    }

    private static long end(Long offset, Integer length) {
        if (offset == null) {
            return 0;
        }
        return saturatedAdd(offset, length == null ? 0 : length);
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    /// Reads and checks the eight-byte trailer: the footer length and the trailing magic.
    ///
    /// The magic number at the start is not read: on a remote file it would cost a request of its
    /// own at the far end of the file, and the four bytes carry nothing the read uses, since
    /// every page is located through the footer. The trailing magic tells a Parquet file, and an
    /// encrypted one, apart from anything else.
    ///
    /// @return the length of the serialized footer
    private static int readTrailer(InputFile inputFile, long fileSize) throws IOException {
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "File too small to be a valid Parquet file");
        }
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf;
        try (FetchReason.Scope ignored = FetchReason.set("footer-info")) {
            footerInfoBuf = inputFile.readRange(footerInfoPos, FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        }
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (Arrays.equals(endMagic, ENCRYPTED_MAGIC)) {
            throw encrypted(inputFile);
        }
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Not a Parquet file (invalid magic number at end)");
        }

        // A negative length would move the footer start past the end of the file rather than
        // before its beginning, so it is rejected on its own.
        long footerStart = footerInfoPos - footerLength;
        if (footerLength < 0 || footerStart < MAGIC_SIZE) {
            throw new ParquetReadException(ExceptionContext.filePrefix(inputFile.name())
                    + "Invalid footer length: " + footerLength);
        }
        return footerLength;
    }

    private static ReadFooter parse(InputFile inputFile, int footerLength, long fileSize) throws IOException {
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        ByteBuffer footerBuffer;
        try (FetchReason.Scope ignored = FetchReason.set("footer-body")) {
            footerBuffer = inputFile.readRange(footerStart, footerLength);
        }
        ThriftCompactReader reader = new ThriftCompactReader(footerBuffer);
        try {
            return FileMetaDataReader.readFooter(reader);
        }
        catch (EncryptedFileException e) {
            // Plaintext-footer encryption: the footer parsed, but the data is
            // encrypted. Re-throw with file context for an attributable error.
            throw encrypted(inputFile);
        }
        catch (ParquetReadException e) {
            // Negative sizes, an unknown field type, a struct that ends early —
            // the reader already types all of them as the file being wrong. What
            // it cannot name is the file, which only this frame knows.
            throw new ParquetReadException(
                    ExceptionContext.filePrefix(inputFile.name()) + e.getMessage(), e);
        }
    }

    /// An encrypted file is a correct file this library does not read, which is
    /// what [UnsupportedOperationException] says everywhere else the reader meets
    /// something it has not implemented — an absent codec library, an encoding it
    /// does not decode.
    private static UnsupportedOperationException encrypted(InputFile inputFile) {
        return new UnsupportedOperationException(
                ExceptionContext.filePrefix(inputFile.name()) + ENCRYPTED_MESSAGE);
    }
}
