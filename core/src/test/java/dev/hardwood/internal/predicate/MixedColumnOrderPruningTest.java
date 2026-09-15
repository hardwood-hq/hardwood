/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// The files of one read may declare different column orders: parquet-java writes
/// `IEEE_754_TOTAL_ORDER` for floating-point columns, PyArrow and older writers `TYPE_ORDER`.
/// Under the type-defined order a `+0` minimum may hide a `-0`, so a file that declares it has
/// its zero bounds read as covering both zeroes, whatever the first file of the read declares.
class MixedColumnOrderPruningTest {

    @TempDir
    Path tempDir;

    @Test
    void typeDefinedZeroBoundCoversNegativeZeroAfterATotalOrderFile() throws Exception {
        Path totalOrder = floats("total-order.parquet", new float[] { 1.0f, 2.0f },
                metaData -> withColumnOrders(metaData, ColumnOrder.IEEE754_TOTAL_ORDER));
        Path typeDefined = floats("type-defined.parquet", new float[] { -0.0f, 5.0f },
                metaData -> withMin(metaData, 0.0f));

        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(
                InputFile.of(totalOrder), InputFile.of(typeDefined)));
                RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("f", -0.0f)).build()) {
            List<Float> values = new ArrayList<>();
            while (rows.hasNext()) {
                rows.next();
                values.add(rows.getFloat("f"));
            }
            assertThat(values).containsExactly(-0.0f);
        }
    }

    /// A single required `FLOAT` column `f` holding `values`, its footer passed through `patch`.
    private Path floats(String fileName, float[] values, UnaryOperator<FileMetaData> patch) throws IOException {
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out,
                FileSchema.builder("s").addColumn("f", PhysicalType.FLOAT, RepetitionType.REQUIRED).build())) {
            writer.columnWriter().writeBatch(batch -> batch.floats(0, values));
        }
        byte[] rewritten = FooterRewriter.rewrite(out.toByteArray(), patch);
        Path path = tempDir.resolve(fileName);
        Files.write(path, rewritten);
        return path;
    }

    private static FileMetaData withColumnOrders(FileMetaData metaData, ColumnOrder order) {
        return new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(), metaData.rowGroups(),
                metaData.keyValueMetadata(), metaData.createdBy(),
                Collections.nCopies(metaData.columnOrders().size(), order));
    }

    /// Replaces the row-group minimum with `min`, as a writer that does not normalise a zero
    /// bound to `-0.0` records it.
    private static FileMetaData withMin(FileMetaData metaData, float min) {
        List<RowGroup> rowGroups = new ArrayList<>();
        for (RowGroup rowGroup : metaData.rowGroups()) {
            List<ColumnChunk> columns = new ArrayList<>();
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData m = chunk.metaData();
                Statistics s = m.statistics();
                Statistics foreign = new Statistics(floatBytes(min), s.maxValue(), s.nullCount(), s.distinctCount(),
                        false, true, true, s.nanCount());
                columns.add(new ColumnChunk(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                        m.codec(), m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(),
                        m.keyValueMetadata(), m.dataPageOffset(), m.dictionaryPageOffset(), foreign,
                        m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                        m.encodingStats(), m.sizeStatistics()),
                        chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                        chunk.columnIndexLength(), chunk.filePath()));
            }
            rowGroups.add(new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows()));
        }
        return new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(), rowGroups,
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }

    private static byte[] floatBytes(float value) {
        return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }
}
