/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// [DeltaLengthByteArrayEncoder] and [DeltaByteArrayEncoder] against the decoders beside them,
/// over the edges each encoding has: empty values, values sharing everything or nothing, and a
/// range taken out of the middle of a chunk's stored values.
class ByteArrayEncodingsTest {

    // ==================== DELTA_LENGTH_BYTE_ARRAY ====================

    @Test
    void deltaLengthRoundTripsOrdinaryValues() throws IOException {
        List<byte[]> values = strings("Hello", "World", "Foobar", "a", "");

        assertThat(decodeDeltaLength(encodeDeltaLength(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaLengthRoundTripsASingleValue() throws IOException {
        List<byte[]> values = strings("only");

        assertThat(decodeDeltaLength(encodeDeltaLength(values), 1)).containsExactlyElementsOf(values);
    }

    @Test
    void deltaLengthRoundTripsAllEmptyValues() throws IOException {
        // Every length is zero, so the length stream packs to nothing and the value section is
        // empty — a page whose body carries no value bytes at all.
        List<byte[]> values = strings("", "", "", "");

        assertThat(decodeDeltaLength(encodeDeltaLength(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaLengthRoundTripsManyValuesAcrossBlocks() throws IOException {
        // More than one delta block of lengths, with the lengths themselves varying.
        List<byte[]> values = new ArrayList<>();
        Random random = new Random(4242L);
        for (int i = 0; i < 500; i++) {
            byte[] value = new byte[random.nextInt(20)];
            random.nextBytes(value);
            values.add(value);
        }

        assertThat(decodeDeltaLength(encodeDeltaLength(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaLengthEncodesOnlyTheRequestedRange() throws IOException {
        List<byte[]> values = strings("skip", "me", "take", "these", "two");
        Packed packed = Packed.of(values);

        byte[] encoded = DeltaLengthByteArrayEncoder.encode(packed.data, packed.offsets, 2, 2);

        assertThat(decodeDeltaLength(encoded, 2)).containsExactly(
                "take".getBytes(StandardCharsets.UTF_8), "these".getBytes(StandardCharsets.UTF_8));
    }

    // ==================== DELTA_BYTE_ARRAY ====================

    @Test
    void deltaByteArrayRoundTripsSharedPrefixes() throws IOException {
        // The encoding's own example: each value shares a start with the one before it.
        List<byte[]> values = strings("apple", "application", "apply");

        assertThat(decodeDeltaByteArray(encodeDeltaByteArray(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaByteArrayRoundTripsWithNoSharedPrefix() throws IOException {
        // Every prefix length is zero, which is the encoding degenerating to
        // DELTA_LENGTH_BYTE_ARRAY plus a stream of zeros.
        List<byte[]> values = strings("alpha", "beta", "gamma", "delta");

        assertThat(decodeDeltaByteArray(encodeDeltaByteArray(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaByteArrayRoundTripsIdenticalValues() throws IOException {
        // Each value shares the whole of the previous one, so every suffix after the first is
        // empty — the maximum the encoding can save. Enough of them that the two delta headers
        // stop dominating and the per-value cost is what the size reflects.
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add("same".getBytes(StandardCharsets.UTF_8));
        }

        byte[] encoded = encodeDeltaByteArray(values);

        assertThat(decodeDeltaByteArray(encoded, values.size())).containsExactlyElementsOf(values);
        // PLAIN would spend a 4-byte length plus the 4 value bytes on every one of them.
        int plainBytes = values.size() * (Integer.BYTES + 4);
        assertThat(encoded.length).as("repeated values collapse to their prefixes")
                .isLessThan(plainBytes / 3);
    }

    @Test
    void deltaByteArrayRoundTripsAValueShorterThanItsPredecessor() throws IOException {
        // The prefix cannot exceed the shorter of the two values, which is what stops a short
        // value following a long one from claiming a prefix it does not have.
        List<byte[]> values = strings("abcdef", "abc", "ab", "abcdefgh");

        assertThat(decodeDeltaByteArray(encodeDeltaByteArray(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaByteArrayRoundTripsEmptyAndSingleByteValues() throws IOException {
        List<byte[]> values = strings("", "a", "", "ab", "b");

        assertThat(decodeDeltaByteArray(encodeDeltaByteArray(values), values.size()))
                .containsExactlyElementsOf(values);
    }

    @Test
    void deltaByteArrayRoundTripsSortedPathsAcrossBlocks() throws IOException {
        // The shape the encoding exists for, at a size that crosses several delta blocks.
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            values.add(("/var/log/service/2026-08-20/part-" + String.format("%05d", i) + ".log")
                    .getBytes(StandardCharsets.UTF_8));
        }

        byte[] encoded = encodeDeltaByteArray(values);

        assertThat(decodeDeltaByteArray(encoded, values.size())).containsExactlyElementsOf(values);
        assertThat(encoded.length).as("shared paths cost far less than their plain bytes")
                .isLessThan(values.size() * 20);
    }

    @Test
    void deltaByteArrayStartsAPageWithoutItsPredecessor() throws IOException {
        // A page is decodable on its own, so encoding a range starting mid-chunk must give its
        // first value a zero prefix rather than one relative to the value before the range.
        List<byte[]> values = strings("prefix-a", "prefix-b", "prefix-c", "prefix-d");
        Packed packed = Packed.of(values);

        byte[] encoded = DeltaByteArrayEncoder.encode(packed.data, packed.offsets, 2, 2);

        assertThat(decodeDeltaByteArray(encoded, 2)).containsExactly(
                "prefix-c".getBytes(StandardCharsets.UTF_8), "prefix-d".getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Helpers ====================

    private static List<byte[]> strings(String... values) {
        List<byte[]> result = new ArrayList<>(values.length);
        for (String value : values) {
            result.add(value.getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    private static byte[] encodeDeltaLength(List<byte[]> values) {
        Packed packed = Packed.of(values);
        return DeltaLengthByteArrayEncoder.encode(packed.data, packed.offsets, 0, values.size());
    }

    private static byte[] encodeDeltaByteArray(List<byte[]> values) {
        Packed packed = Packed.of(values);
        return DeltaByteArrayEncoder.encode(packed.data, packed.offsets, 0, values.size());
    }

    private static List<byte[]> decodeDeltaLength(byte[] encoded, int count) throws IOException {
        DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(encoded, 0, encoded.length);
        decoder.initialize(count);
        byte[][] output = new byte[count][];
        decoder.readByteArrays(output, null, 0);
        return List.of(output);
    }

    private static List<byte[]> decodeDeltaByteArray(byte[] encoded, int count) throws IOException {
        DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(encoded, 0, encoded.length);
        decoder.initialize(count);
        byte[][] output = new byte[count][];
        decoder.readByteArrays(output, null, 0);
        return List.of(output);
    }

    /// Values packed end to end behind cumulative offsets — value `i` occupying
    /// `data[offsets[i], offsets[i + 1])` — the layout
    /// [dev.hardwood.internal.writer.BinaryValueEncoder] stores a chunk's values in.
    private record Packed(byte[] data, int[] offsets) {

        static Packed of(List<byte[]> values) {
            int total = 0;
            for (byte[] value : values) {
                total += value.length;
            }
            byte[] data = new byte[total];
            int[] offsets = new int[values.size() + 1];
            int pos = 0;
            for (int i = 0; i < values.size(); i++) {
                byte[] value = values.get(i);
                System.arraycopy(value, 0, data, pos, value.length);
                pos += value.length;
                offsets[i + 1] = pos;
            }
            return new Packed(data, offsets);
        }
    }
}
