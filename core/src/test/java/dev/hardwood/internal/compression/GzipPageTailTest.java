/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What a GZIP page may hold besides its declared output, read through the `Inflater` path. Members
/// are read until the declared size is produced and the bytes after it are ignored; a member that
/// inflates past the declared size, or whose trailer disagrees with its output, fails.
/// `GzipDecompressionTest` in the integration tests pins the same rule for libdeflate, with the same
/// messages except for a trailer, which libdeflate reports as bad data without naming the field.
class GzipPageTailTest {

    private static final Decompressor DECOMPRESSOR = new GzipDecompressor();

    private static byte[] body(int length, int seed) {
        byte[] body = new byte[length];
        for (int i = 0; i < length; i++) {
            body[i] = (byte) ('a' + (i * seed) % 13);
        }
        return body;
    }

    private static byte[] member(byte[] body) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
            out.write(body);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    private static void assertDecodes(byte[] page, byte[] body) {
        byte[] plain = DECOMPRESSOR.decompress(ByteBuffer.wrap(page), body.length);
        assertThat(Arrays.copyOf(plain, body.length)).isEqualTo(body);
        byte[] dest = new byte[body.length];
        assertThat(DECOMPRESSOR.decompress(ByteBuffer.wrap(page), body.length, dest)).isEqualTo(body);
    }

    @Test
    void ignoresBytesAfterACompletePage() {
        byte[] body = body(4_000, 1);
        byte[] garbage = new byte[16];
        Arrays.fill(garbage, (byte) 0x55);

        assertDecodes(concat(member(body), garbage), body);
    }

    @Test
    void ignoresAMemberAfterACompletePage() {
        byte[] body = body(4_000, 1);

        assertDecodes(concat(member(body), member(body(2_000, 3))), body);
    }

    @Test
    void readsMembersUntilTheDeclaredSize() {
        byte[] first = body(2_000, 1);
        byte[] second = body(2_000, 3);

        assertDecodes(concat(member(first), member(second)), concat(first, second));
    }

    @Test
    void refusesAMemberInflatingPastTheDeclaredSize() {
        byte[] page = concat(member(body(2_000, 1)), member(body(2_000, 3)));

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 3_000, new byte[3_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("GZIP data decompresses to more than the expected 3000 bytes");
    }

    @Test
    void refusesAPageShorterThanDeclared() {
        byte[] page = member(body(4_000, 1));

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 5_000, new byte[5_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Decompressed size mismatch: expected 5000, got 4000");
    }

    /// A copy of `member` with the trailer byte at `fromEnd` bytes before its end flipped: 8 to 5
    /// fall in the CRC32, 4 to 1 in the length.
    static byte[] corruptTrailer(byte[] member, int fromEnd) {
        byte[] corrupt = member.clone();
        corrupt[corrupt.length - fromEnd] ^= 0x01;
        return corrupt;
    }

    @Test
    void refusesAMemberWhoseCrcDoesNotMatchItsData() {
        byte[] page = corruptTrailer(member(body(4_000, 1)), 8);

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 4_000, new byte[4_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("GZIP member's CRC32 does not match its data");
    }

    @Test
    void refusesAMemberWhoseTrailerGivesAnotherLength() {
        byte[] page = corruptTrailer(member(body(4_000, 1)), 4);

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 4_000, new byte[4_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("GZIP member's trailer gives its length as 4001 bytes, but it inflated to 4000");
    }

    /// The declared output is complete, so the bytes after it would be ignored; the member's own
    /// trailer is not among them.
    @Test
    void refusesAMemberWhoseTrailerIsCutShort() {
        byte[] member = member(body(4_000, 1));
        byte[] page = Arrays.copyOf(member, member.length - 3);

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 4_000, new byte[4_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Truncated GZIP trailer");
    }

    @Test
    void refusesAMemberWhoseDataIsCutShort() {
        byte[] member = member(body(4_000, 1));
        byte[] page = Arrays.copyOf(member, member.length / 2);

        assertThatThrownBy(() -> DECOMPRESSOR.decompress(ByteBuffer.wrap(page), 4_000, new byte[4_000]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Truncated GZIP data");
    }
}
