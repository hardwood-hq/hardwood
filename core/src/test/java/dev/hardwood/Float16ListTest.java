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
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Coverage for the `FIXED_LEN_BYTE_ARRAY(2) + Float16Type` branch in
/// `PqListImpl.floats()` (hardwood#470). Plain FLOAT lists take the
/// cast-only branch; this fixture is a `List<FLOAT16>` so the typed
/// accessor decodes each element through `LogicalTypeConverter`.
class Float16ListTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/list_float16_test.parquet");

    @Test
    void float16ListDecodesPerElement() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader r = f.rowReader()) {
            r.next(); // scores = [1.5, -2.0, 0.0]
            List<Float> scores = r.getList("scores").floats();
            assertThat(scores).hasSize(3);
            assertThat(scores.get(0)).isEqualTo(1.5f);
            assertThat(scores.get(1)).isEqualTo(-2.0f);
            assertThat(scores.get(2)).isEqualTo(0.0f);

            r.next(); // scores = [100.0]
            assertThat(r.getList("scores").floats()).containsExactly(100.0f);
        }
    }
}
