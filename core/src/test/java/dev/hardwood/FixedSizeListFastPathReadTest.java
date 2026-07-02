/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;
import dev.hardwood.reader.Validity;
import dev.hardwood.row.PqList;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for the fixed-size-list read fast path. Each configuration
/// is written as both `DataPageV2` and `DataPageV1`; both engage the fast path
/// (via separate decode seams). The `fixedSizeListFastPathEnabled` switch is the
/// fast-vs-reconstruction control, and values are cross-checked against the known
/// generator formula, together proving the fast path is a transparent,
/// bitwise-identical optimization.
///
/// The `k` sweep spans both encoding regimes: `k = 1` (rep stream is a single
/// RLE run of zeros), `k = 4` (fully bit-packed, multiple pages), `k = 8`
/// (threshold), and `k = 768` (RLE interior).
class FixedSizeListFastPathReadTest {

    private static final String COLUMN = "vec.list.element";

    /// The fast path is opt-in (off by default), so every read here that means to
    /// exercise it must enable it explicitly — otherwise these assertions would
    /// pass vacuously against the regular reconstruction path.
    private static final ReaderConfig FAST_PATH_ON =
            ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "true").build();

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 768})
    void fastPathMatchesAcrossPageVersionsAndExpectedValues(int k) throws Exception {
        // Both page versions take the fast path (V1 and V2 seams decode
        // independently); cross-checking them and the generator formula catches
        // a divergence in either seam.
        List<float[]> viaV2 = readVectors(resource(k, "v2"), k);
        List<float[]> viaV1 = readVectors(resource(k, "v1"), k);

        assertThat(viaV2).as("v2 vs v1 record count").hasSameSizeAs(viaV1);
        for (int r = 0; r < viaV2.size(); r++) {
            assertThat(viaV2.get(r)).as("record %d values (v2 vs v1)", r).containsExactly(viaV1.get(r));
        }

        // Cross-check against the deterministic generator: flat[i] = i*1.5 + 0.25.
        int rows = viaV2.size();
        for (int r = 0; r < rows; r++) {
            float[] vec = viaV2.get(r);
            assertThat(vec).as("record %d length", r).hasSize(k);
            for (int i = 0; i < k; i++) {
                float expected = (r * k + i) * 1.5f + 0.25f;
                assertThat(vec[i]).as("record %d element %d", r, i).isEqualTo(expected);
            }
        }
    }

    /// The fast-vs-regular correctness control: with the fast path disabled via
    /// the context option the same file decodes via record reconstruction, and
    /// its output must be identical. Covers both page versions.
    @ParameterizedTest
    @ValueSource(strings = {"v1", "v2"})
    void fastPathMatchesReconstructionPath(String version) throws Exception {
        List<float[]> viaFast;
        List<float[]> viaRegular;
        // One shared context (one thread pool) backs both configs — the point of
        // splitting behaviour (ReaderConfig) from resources (HardwoodContext).
        try (HardwoodContext context = HardwoodContext.create()) {
            viaFast = readVectors(resource(4, version), 4, context, FAST_PATH_ON);
            viaRegular = readVectors(resource(4, version), 4, context,
                    ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "false").build());
        }
        assertThat(viaFast).as("%s fast vs reconstruction record count", version).hasSameSizeAs(viaRegular);
        for (int r = 0; r < viaFast.size(); r++) {
            assertThat(viaFast.get(r)).as("%s record %d (fast vs reconstruction)", version, r)
                    .containsExactly(viaRegular.get(r));
        }
    }

    /// On a fast-path batch the raw-levels escape hatch synthesizes the levels
    /// on demand: definition levels are all `maxDef`, repetition levels are the
    /// `0, 1…1` per-row pattern — the same the regular path would materialize.
    @Test
    void escapeHatchSynthesizesLevelsOnFastPath() throws Exception {
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader =
                     ParquetFileReader.open(InputFile.of(resource(4, "v2")), context, FAST_PATH_ON);
             ColumnReader col = reader.columnReader(COLUMN)) {
            int records = 0;
            while (col.nextBatch()) {
                int[] def = col.getDefinitionLevels();
                int[] rep = col.getRepetitionLevels();
                int valueCount = col.getValueCount();
                assertThat(def).as("synthesized def levels").isNotNull();
                assertThat(rep).as("synthesized rep levels").isNotNull();
                for (int i = 0; i < valueCount; i++) {
                    assertThat(def[i]).as("def[%d]", i).isEqualTo(2);
                    assertThat(rep[i]).as("rep[%d]", i).isEqualTo(i % 4 == 0 ? 0 : 1);
                }
                records += col.getRecordCount();
            }
            assertThat(records).isEqualTo(300);
        }
    }

    /// The row-reader path routes fixed-width batches through `NestedBatchIndex` /
    /// `PqList`, which read the (now `null`) level arrays and the arithmetic
    /// offsets. This confirms those consumers tolerate the fixed-width-batch shape.
    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 768})
    void rowReaderMatchesExpectedValues(int k) throws Exception {
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader =
                     ParquetFileReader.open(InputFile.of(resource(k, "v2")), context, FAST_PATH_ON);
             RowReader rows = reader.rowReader()) {
            int r = 0;
            while (rows.hasNext()) {
                rows.next();
                PqList vec = rows.getList("vec");
                assertThat(vec.size()).as("record %d length", r).isEqualTo(k);
                List<Float> values = vec.floats();
                for (int i = 0; i < k; i++) {
                    float expected = (r * k + i) * 1.5f + 0.25f;
                    assertThat(values.get(i)).as("row %d element %d", r, i).isEqualTo(expected);
                }
                r++;
            }
            assertThat(r).as("row count").isGreaterThan(0);
        }
    }

    /// A small explicit batch size drives the fast path across its own value
    /// accumulator boundary. The fixture's first row group is 20 present rows
    /// (fast-path pages) and its second opens with a null list at row 20. Read
    /// with `batchSize = 32`, one batch spans the row-group boundary: the initial
    /// level arrays hold `2 * 32 = 64` slots, but the leading fixed-width run
    /// fills `20 * 4 = 80` values, growing the value array past that and leaving
    /// the level arrays behind. When the null page then forces the open
    /// fixed-width batch to materialize its levels, the level arrays must grow to
    /// match — the lazy growth the default (large) batch sizes never reach. The
    /// fast-path output must stay identical to the regular reconstruction path,
    /// null rows included, and to the generator formula.
    @Test
    void smallBatchFixedRunThenNullMatchesReconstructionPath() throws Exception {
        Path fixture = Paths.get("src/test/resources/fixed_size_list_k4_leadfast_v2.parquet");
        List<float[]> viaFast;
        List<float[]> viaRegular;
        try (HardwoodContext context = HardwoodContext.create()) {
            viaFast = readMixedRecords(fixture, COLUMN, 32, context, FAST_PATH_ON);
            viaRegular = readMixedRecords(fixture, COLUMN, 32, context,
                    ReaderConfig.builder().option("hardwood.fixed-list-fast-path", "false").build());
        }

        assertThat(viaFast).as("fast vs reconstruction record count").hasSameSizeAs(viaRegular);
        for (int r = 0; r < viaFast.size(); r++) {
            assertThat(viaFast.get(r)).as("record %d (fast vs reconstruction)", r)
                    .isEqualTo(viaRegular.get(r));
        }

        // Independent oracle: null lists at rows 20/27/33, every other row the
        // generator formula flat[i] = i*1.5 + 0.25 over 4 elements.
        assertThat(viaFast).hasSize(40);
        for (int r = 0; r < 40; r++) {
            if (r == 20 || r == 27 || r == 33) {
                assertThat(viaFast.get(r)).as("row %d is a null list", r).isNull();
                continue;
            }
            float[] expected = new float[4];
            for (int i = 0; i < 4; i++) {
                expected[i] = (r * 4 + i) * 1.5f + 0.25f;
            }
            assertThat(viaFast.get(r)).as("row %d values", r).containsExactly(expected);
        }
    }

    /// Reads a nullable fixed-size-list column record by record, returning `null`
    /// for a null row and the element slice otherwise, flattened across batches.
    private static List<float[]> readMixedRecords(Path file, String column, int batchSize,
                                                  HardwoodContext context, ReaderConfig config) throws Exception {
        List<float[]> records = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context, config);
             ColumnReader col = reader.buildColumnReader(column).batchSize(batchSize).build()) {
            while (col.nextBatch()) {
                int recordCount = col.getRecordCount();
                int[] offsets = col.getLayerOffsets(0);
                Validity validity = col.getLayerValidity(0);
                float[] values = col.getFloats();
                for (int r = 0; r < recordCount; r++) {
                    if (validity.hasNulls() && validity.isNull(r)) {
                        records.add(null);
                        continue;
                    }
                    float[] vec = new float[offsets[r + 1] - offsets[r]];
                    System.arraycopy(values, offsets[r], vec, 0, vec.length);
                    records.add(vec);
                }
            }
        }
        return records;
    }

    /// Reads every record's element values across all batches, slicing the flat
    /// leaf array by the layer offsets. This flattens away batch boundaries,
    /// which may legitimately differ between the fast and regular paths.
    private static List<float[]> readVectors(Path file, int k) throws Exception {
        try (HardwoodContext context = HardwoodContext.create()) {
            return readVectors(file, k, context, FAST_PATH_ON);
        }
    }

    private static List<float[]> readVectors(Path file, int k, HardwoodContext context,
                                             ReaderConfig config) throws Exception {
        List<float[]> records = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context, config);
             ColumnReader col = reader.columnReader(COLUMN)) {
            while (col.nextBatch()) {
                assertThat(col.getLayerCount()).isEqualTo(1);
                assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
                assertThat(col.getLayerValidity(0).hasNulls()).isFalse();

                int recordCount = col.getRecordCount();
                int[] offsets = col.getLayerOffsets(0);
                float[] values = (float[]) col.getFloats();
                assertThat(offsets[recordCount]).isEqualTo(col.getValueCount());
                for (int r = 0; r < recordCount; r++) {
                    float[] vec = new float[offsets[r + 1] - offsets[r]];
                    System.arraycopy(values, offsets[r], vec, 0, vec.length);
                    records.add(vec);
                }
            }
        }
        return records;
    }

    private static Path resource(int k, String version) {
        return Paths.get("src/test/resources/fixed_size_list_k" + k + "_" + version + ".parquet");
    }
}
