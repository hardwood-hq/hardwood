/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Validation of [WriterConfig.Builder]: the size targets are checked eagerly, so a
/// misconfigured writer fails at build time rather than mid-write.
///
/// The footer's file-scope fields are set on [ParquetFileWriter], and covered by
/// [WriterFooterMetadataTest].
class WriterConfigTest {

    @Test
    void defaultsMatchTheDeclaredConstants() {
        WriterConfig config = WriterConfig.defaults();
        assertThat(config.pageTargetBytes()).isEqualTo(WriterConfig.DEFAULT_PAGE_TARGET_BYTES);
        assertThat(config.rowGroupBufferTargetBytes()).isEqualTo(WriterConfig.DEFAULT_ROW_GROUP_BUFFER_TARGET_BYTES);
        assertThat(config.rowGroupTargetRows()).isEqualTo(WriterConfig.DEFAULT_ROW_GROUP_TARGET_ROWS);
        assertThat(config.compression()).isEqualTo(WriterConfig.DEFAULT_COMPRESSION);
        assertThat(config.dataPageVersion()).isEqualTo(DataPageVersion.V1);
        // zstd-jni is on the build classpath, so the classpath-conditional default resolves to
        // ZSTD at its default level here; it degrades to UNCOMPRESSED only when the library is
        // absent.
        assertThat(WriterConfig.DEFAULT_COMPRESSION).isEqualTo(Compression.zstd());
    }

    @Test
    void compressionOverrideIsRetained() {
        assertThat(WriterConfig.builder().compression(Compression.gzip().level(9)).build().compression())
                .isEqualTo(Compression.gzip().level(9));
    }

    @Test
    void dataPageVersionOverrideIsRetained() {
        assertThat(WriterConfig.builder().dataPageVersion(DataPageVersion.V2).build().dataPageVersion())
                .isEqualTo(DataPageVersion.V2);
    }

    @Test
    void rejectsNullDataPageVersion() {
        assertThatThrownBy(() -> WriterConfig.builder().dataPageVersion(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataPageVersion must not be null");
    }

    @Test
    void rejectsNullCompression() {
        assertThatThrownBy(() -> WriterConfig.builder().compression(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("compression must not be null");
    }

    @Test
    void acceptsTheSmallestValidPageTarget() {
        assertThat(WriterConfig.builder().pageTargetBytes(Integer.BYTES).build().pageTargetBytes())
                .isEqualTo(Integer.BYTES);
    }

    @Test
    void rejectsPageTargetSmallerThanOneInt32() {
        assertThatThrownBy(() -> WriterConfig.builder().pageTargetBytes(Integer.BYTES - 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveRowGroupTarget() {
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupBufferTargetBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupBufferTargetBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveRowGroupRowTarget() {
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupTargetRows(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupTargetRows(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
