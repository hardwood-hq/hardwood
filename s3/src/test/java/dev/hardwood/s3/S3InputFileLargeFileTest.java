/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies that [S3InputFile] reads files larger than 2 GB from S3.
///
/// The file length, the tail cache offset, and every byte-range offset are
/// `long`-typed in the bare network path, so an offset past
/// [Integer#MAX_VALUE] resolves to the right bytes rather than wrapping. The
/// 2 GB cap [RangeBackedInputFile] enforces only kicks in under
/// [RangeBacking#SPARSE_TEMPFILE], which this test does *not* opt into.
///
/// The test object is sparse-truncated inside the s3proxy container so the
/// 2 GB file costs kilobytes on disk and milliseconds to create.
@Testcontainers
class S3InputFileLargeFileTest {

    private static final long TWO_GB = (long) Integer.MAX_VALUE + 1; // 2_147_483_648

    private static final byte[] AT_ZERO = "START-OF-FILE---".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] STRADDLE = "STRADDLE-2GB-MRK".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BEYOND = "WELL-BEYOND-2GB!".getBytes(StandardCharsets.US_ASCII);

    private static final long STRADDLE_OFFSET = TWO_GB - 8;
    private static final long BEYOND_OFFSET = TWO_GB + 4096;
    /// Padded past `BEYOND_OFFSET + BEYOND.length` by more than the 64 KB
    /// tail cache so the straddle and post-2 GB reads exercise the network
    /// path — that's where the long-typed offset arithmetic lives.
    private static final long FILE_SIZE = TWO_GB + 256 * 1024L + 16;
    private static final String KEY = "large.bin";

    @Container
    static GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked();

    static S3Source source;

    @BeforeAll
    static void setup() throws Exception {
        execOrFail("mkdir", "-p", "/data/" + S3ProxyContainers.BUCKET);
        String objectPath = S3ProxyContainers.objectPath(KEY);
        execOrFail("truncate", "-s", String.valueOf(FILE_SIZE), objectPath);
        writeSentinel(objectPath, 0, AT_ZERO);
        writeSentinel(objectPath, STRADDLE_OFFSET, STRADDLE);
        writeSentinel(objectPath, BEYOND_OFFSET, BEYOND);

        source = S3Source.builder()
                .endpoint(S3ProxyContainers.endpoint(s3))
                .pathStyle(true)
                .credentials(S3Credentials.of(S3ProxyContainers.ACCESS_KEY, S3ProxyContainers.SECRET_KEY))
                .build();
    }

    @AfterAll
    static void tearDown() {
        source.close();
    }

    @Test
    void readsRegionsBeyondTwoGigabytes() throws Exception {
        try (S3InputFile inputFile = source.inputFile(S3ProxyContainers.BUCKET, KEY)) {
            inputFile.open();

            // Whole-file length surfaces unchanged past Integer.MAX_VALUE.
            assertThat(inputFile.length()).isEqualTo(FILE_SIZE);

            assertThat(read(inputFile, 0, AT_ZERO.length)).isEqualTo(AT_ZERO);
            assertThat(read(inputFile, STRADDLE_OFFSET, STRADDLE.length)).isEqualTo(STRADDLE);

            // The decisive case: a start offset past Integer.MAX_VALUE,
            // which an int cast on the offset would corrupt.
            assertThat(read(inputFile, BEYOND_OFFSET, BEYOND.length)).isEqualTo(BEYOND);

            // A hole reads back as zeros.
            assertThat(read(inputFile, TWO_GB + 1024, 8)).containsOnly((byte) 0);
        }
    }

    private static byte[] read(S3InputFile inputFile, long offset, int length) throws IOException {
        ByteBuffer buffer = inputFile.readRange(offset, length);
        byte[] out = new byte[length];
        buffer.get(out);
        return out;
    }

    private static void writeSentinel(String objectPath, long offset, byte[] bytes) throws Exception {
        // dd's `seek` is in blocks of `bs`; with bs=1 it's a byte offset, and
        // `conv=notrunc` preserves the surrounding sparse holes.
        execOrFail("sh", "-c",
                "printf '%s' '" + new String(bytes, StandardCharsets.US_ASCII) + "'"
                        + " | dd of=" + objectPath
                        + " bs=1 seek=" + offset
                        + " count=" + bytes.length
                        + " conv=notrunc status=none");
    }

    private static void execOrFail(String... command) throws Exception {
        ExecResult result = s3.execInContainer(command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("Command failed: " + String.join(" ", command)
                    + "\nstdout: " + result.getStdout()
                    + "\nstderr: " + result.getStderr());
        }
    }
}
