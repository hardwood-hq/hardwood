/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ColumnChunkTest {

    @Test
    void nullFilePathIsNormalisedToThisFile() {
        ColumnChunk chunk = new ColumnChunk(null, null, null, null, null, null);
        assertThat(chunk.filePath()).isEmpty();
        assertDoesNotThrow(chunk::requireSameFile);
    }

    @Test
    void aChunkInAnotherFileCannotBeRead() {
        ColumnChunk chunk = new ColumnChunk(null, null, null, null, null, "part-1.parquet");
        assertThatThrownBy(chunk::requireSameFile)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Column chunk stores its data in a separate file ('part-1.parquet'); the "
                         + "split-file layout is not supported")
                ;
    }
}
