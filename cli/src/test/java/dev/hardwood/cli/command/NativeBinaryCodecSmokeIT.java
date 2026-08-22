/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.hardwood.cli.command.NativeBinarySmokeIT.NativeResult;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration test for the native CLI binary verifying that embedded native
/// codec libraries are extracted and loaded correctly. Reads a snappy-compressed
/// Parquet file, exercising the full extraction/cache/codec-loading pipeline.
class NativeBinaryCodecSmokeIT {

    private final String nativeBinary = System.getProperty("native.image.path");
    private final String snappyFile = getClass().getResource("/plain_snappy.parquet").getPath();

    @Test
    void readsSnappyCompressedFile() throws IOException, InterruptedException {
        NativeResult result = NativeBinarySmokeIT.exec(nativeBinary, "schema", "-f", snappyFile);

        assertThat(result.exitCode())
                .withFailMessage("snappy read failed: stdout=%s stderr=%s",
                        result.stdout(), result.stderr())
                .isZero();
        assertThat(result.stdout()).contains("message schema");
    }
}
