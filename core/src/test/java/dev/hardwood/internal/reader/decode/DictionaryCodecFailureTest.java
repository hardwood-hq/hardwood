/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A codec Hardwood cannot provide leaves the dictionary path as the
/// [UnsupportedOperationException] it was raised as.
///
/// The message names what to do about it — for an absent library, the dependency to add — and that
/// is the only actionable thing about the failure, so nothing on the way out may re-type it as a
/// problem with the file. Reading a dictionary reaches the codec through
/// [DictionaryParser#parse], which used to catch `Exception` around the whole of it and report
/// whatever it caught as a failed read.
///
/// `LZO` stands in for the absent-library case. Both leave [DecompressorFactory#getDecompressor]
/// as an `UnsupportedOperationException` naming its remedy, and `LZO` does it with every
/// compression dependency on the classpath, which is what the build has.
class DictionaryCodecFailureTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    @Test
    void aCodecHardwoodCannotProvideIsNotReportedAsAFailedRead() throws Exception {
        InputFile file = InputFile.of(FIXTURE);
        try (ParquetFileReader reader = ParquetFileReader.open(file);
             HardwoodContextImpl context = HardwoodContextImpl.create()) {

            ColumnMetaData present = reader.getFileMetaData().rowGroups().getFirst()
                    .columns().get(1).metaData();
            ColumnSchema columnSchema = reader.getFileSchema().getColumn(1);

            long dictionaryStart = present.dictionaryPageOffset();
            int regionLength = Math.toIntExact(present.dataPageOffset() - dictionaryStart);
            ByteBuffer dictionaryRegion = file.readRange(dictionaryStart, regionLength);

            assertThatThrownBy(() ->
                    DictionaryParser.parse(dictionaryRegion, columnSchema, withCodec(present), context))
                    .as("the remedy is a dependency, not a different file, so the type must say so")
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("LZO compression is not supported");
        }
    }

    /// The fixture's own dictionary page, relabelled as a codec this reader does not provide. The
    /// bytes stay a well-formed dictionary page, so the header parses and the failure comes from
    /// asking for the codec rather than from anything wrong with the file.
    private static ColumnMetaData withCodec(ColumnMetaData source) {
        return new ColumnMetaData(
                source.type(), source.encodings(), source.pathInSchema(), CompressionCodec.LZO,
                source.numValues(), source.totalUncompressedSize(), source.totalCompressedSize(),
                source.keyValueMetadata(), source.dataPageOffset(), source.dictionaryPageOffset(),
                source.statistics(), source.geospatialStatistics(), source.bloomFilterOffset(),
                source.bloomFilterLength(), source.encodingStats(), source.sizeStatistics());
    }
}
