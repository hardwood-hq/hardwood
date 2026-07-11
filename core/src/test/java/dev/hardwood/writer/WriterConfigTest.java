/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.CompressionCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Validation of [WriterConfig.Builder]: the size targets and `created_by` are checked
/// eagerly, so a misconfigured writer fails at build time rather than mid-write.
class WriterConfigTest {

    @Test
    void defaultsMatchTheDeclaredConstants() {
        WriterConfig config = WriterConfig.defaults();
        assertThat(config.pageTargetBytes()).isEqualTo(WriterConfig.DEFAULT_PAGE_TARGET_BYTES);
        assertThat(config.rowGroupTargetBytes()).isEqualTo(WriterConfig.DEFAULT_ROW_GROUP_TARGET_BYTES);
        assertThat(config.createdBy()).isEqualTo(WriterConfig.DEFAULT_CREATED_BY);
        assertThat(config.codec()).isEqualTo(WriterConfig.DEFAULT_CODEC);
        // zstd-jni is on the build classpath, so the classpath-conditional default resolves to
        // ZSTD here; it degrades to UNCOMPRESSED only when the library is absent.
        assertThat(WriterConfig.DEFAULT_CODEC).isEqualTo(CompressionCodec.ZSTD);
    }

    @Test
    void codecOverrideIsRetained() {
        assertThat(WriterConfig.builder().codec(CompressionCodec.UNCOMPRESSED).build().codec())
                .isEqualTo(CompressionCodec.UNCOMPRESSED);
    }

    @Test
    void rejectsNullCodec() {
        assertThatThrownBy(() -> WriterConfig.builder().codec(null))
                .isInstanceOf(IllegalArgumentException.class);
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
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupTargetBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WriterConfig.builder().rowGroupTargetBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullCreatedBy() {
        assertThatThrownBy(() -> WriterConfig.builder().createdBy(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
