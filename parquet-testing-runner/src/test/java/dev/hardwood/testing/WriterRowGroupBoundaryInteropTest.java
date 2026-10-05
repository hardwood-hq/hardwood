/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// The interop gate over a file whose row groups the caller ended: parquet-java reads every value
/// back, finds the row groups where the caller placed them, and reports for each one bounds and a
/// page index over its own records only.
class WriterRowGroupBoundaryInteropTest {

    /// Record counts of the three row groups, each ended by the caller.
    private static final int[] GROUP_ROWS = { 1_000, 700, 300 };
    private static final int ROWS = 2_000;

    enum WriteApi {
        COLUMNS,
        ROWS
    }

    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                    c -> c.logicalType(LogicalType.string()))
            .addColumn("temp", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
            .build();

    @ParameterizedTest
    @EnumSource(WriteApi.class)
    void parquetJavaReadsRowGroupsTheCallerEnded(WriteApi api, @TempDir Path dir) throws IOException {
        Path file = dir.resolve("boundaries-" + api + ".parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), SCHEMA)) {
            int from = 0;
            for (int group = 0; group < GROUP_ROWS.length; group++) {
                if (group > 0) {
                    writer.endRowGroup();
                }
                write(writer, api, from, GROUP_ROWS[group]);
                from += GROUP_ROWS[group];
            }
        }

        List<Group> rows = ParquetJavaReader.readGroups(file);
        assertThat(rows).hasSize(ROWS);
        for (int r = 0; r < ROWS; r++) {
            Group row = rows.get(r);
            assertThat(row.getLong("id", 0)).isEqualTo(r);
            if (isNull(r)) {
                assertThat(row.getFieldRepetitionCount("name")).isZero();
            }
            else {
                assertThat(row.getBinary("name", 0).toStringUsingUTF8()).isEqualTo(name(r));
            }
            assertThat(row.getDouble("temp", 0)).isEqualTo(temp(r));
        }

        ParquetMetadata footer = ParquetJavaReader.readFooter(file);
        List<BlockMetaData> blocks = footer.getBlocks();
        assertThat(blocks).extracting(BlockMetaData::getRowCount).containsExactly(1_000L, 700L, 300L);

        long firstId = 0;
        for (BlockMetaData block : blocks) {
            ColumnChunkMetaData id = block.getColumns().getFirst();
            assertThat(id.getStatistics().genericGetMin()).isEqualTo(firstId);
            assertThat(id.getStatistics().genericGetMax()).isEqualTo(firstId + block.getRowCount() - 1);
            firstId += block.getRowCount();
        }

        ParquetJavaReader.assertPageIndex(file);
        ParquetJavaReader.assertParseableCreatedBy(footer);
    }

    private static void write(ParquetFileWriter writer, WriteApi api, int from, int count) throws IOException {
        if (api == WriteApi.COLUMNS) {
            long[] ids = new long[count];
            byte[][] names = new byte[count][];
            boolean[] nulls = new boolean[count];
            double[] temps = new double[count];
            for (int i = 0; i < count; i++) {
                int r = from + i;
                ids[i] = r;
                nulls[i] = isNull(r);
                names[i] = nulls[i] ? null : name(r).getBytes(StandardCharsets.UTF_8);
                temps[i] = temp(r);
            }
            writer.columnWriter().writeBatch(batch -> batch
                    .longs("id", ids).bytes("name", names, nulls).doubles("temp", temps));
            return;
        }
        for (int r = from; r < from + count; r++) {
            int row = r;
            writer.rowWriter().writeRow(record -> {
                record.setLong("id", row).setDouble("temp", temp(row));
                if (!isNull(row)) {
                    record.setString("name", name(row));
                }
            });
        }
    }

    private static boolean isNull(int row) {
        return row % 9 == 0;
    }

    private static String name(int row) {
        return "name-" + row % 50;
    }

    private static double temp(int row) {
        return row * 0.5;
    }
}
