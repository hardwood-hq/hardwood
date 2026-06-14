/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [XxHash64]. The expected hashes are reference values produced by the canonical
/// `xxhash` library (Python `xxhash` 3.7.0, XXH64 with seed 0), so the assertions verify
/// conformance against an independent implementation rather than self-consistency alone.
///
/// The byte-array inputs are chosen so that, between them, they drive every length-dependent
/// branch of [XxHash64#hash(byte[],int,int)]: the empty/short start, the four-lane accumulator
/// loop (entered at length 32, iterated more than once from length 64), and each tail handler
/// (8-byte, 4-byte, and single-byte).
class XxHash64Test {

    /// `length >= 32` enters the accumulator loop; below that the short-input start is used.
    private static final int ACCUMULATOR_THRESHOLD = 32;

    static Stream<Arguments> byteArrayVectors() {
        return Stream.of(
                // length 0 — short start, no tail
                Arguments.of("empty", new byte[0], -1205034819632174695L),
                // length 1 — single-byte tail only
                Arguments.of("len 1 (1-byte tail)", ascii("A"), 1371800463213966980L),
                // length 3 — three single-byte tail iterations
                Arguments.of("len 3 (3x 1-byte tail)", ascii("abc"), 4952883123889572249L),
                // length 4 — exact 4-byte tail
                Arguments.of("len 4 (4-byte tail)", ascii("abcd"), -2449070131962342708L),
                // length 7 — 4-byte tail then three single-byte tails
                Arguments.of("len 7 (4-byte + 3x 1-byte tail)", ascii("abcdefg"), 1756566643212976685L),
                // length 8 — exact 8-byte tail
                Arguments.of("len 8 (8-byte tail)", ascii("abcdefgh"), 4238821247360054455L),
                // length 12 — 8-byte then 4-byte tail
                Arguments.of("len 12 (8-byte + 4-byte tail)", ascii("abcdefghijkl"), 5407054947222279347L),
                // length 15 — 8-byte, 4-byte, then three single-byte tails
                Arguments.of("len 15 (8 + 4 + 3 tail)", ascii("abcdefghijklmno"), 3319742962362437736L),
                // length 31 — largest short input: three 8-byte tails, a 4-byte tail, three single-byte
                Arguments.of("len 31 (short, all tails)", sequential(31), -4375578310507393311L),
                // length 32 — exactly one accumulator iteration, no tail
                Arguments.of("len 32 (one accumulator iteration)", sequential(32), -3749919242623962444L),
                // length 33 — one accumulator iteration plus a single-byte tail
                Arguments.of("len 33 (accumulator + 1-byte tail)", sequential(33), 888155921178136237L),
                // length 40 — one accumulator iteration plus an 8-byte tail
                Arguments.of("len 40 (accumulator + 8-byte tail)", sequential(40), -731200582691896855L),
                // length 64 — two accumulator iterations, no tail
                Arguments.of("len 64 (two accumulator iterations)", sequential(64), -592659849139514384L),
                // length 96 — three accumulator iterations, no tail
                Arguments.of("len 96 (three accumulator iterations)", sequential(96), 4975257207486779926L),
                // length 100 — three accumulator iterations plus a 4-byte tail
                Arguments.of("len 100 (accumulator + 4-byte tail)", sequential(100), 7692681977284421015L));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("byteArrayVectors")
    void hashesByteArrayAgainstReferenceVectors(String name, byte[] data, long expected) {
        assertThat(XxHash64.hash(data)).isEqualTo(expected);
    }

    @Test
    void wholeArrayOverloadMatchesExplicitFullRange() {
        byte[] data = sequential(50);
        assertThat(XxHash64.hash(data)).isEqualTo(XxHash64.hash(data, 0, data.length));
    }

    @Test
    void honoursOffsetAndLengthInsteadOfHashingTheWholeArray() {
        // The 32-byte window range[10..42) hashes to the same value as that window in isolation,
        // proving offset and length — not the backing array's bounds — define the hashed region.
        byte[] embedded = sequential(50);
        byte[] window = new byte[ACCUMULATOR_THRESHOLD];
        System.arraycopy(embedded, 10, window, 0, window.length);

        assertThat(XxHash64.hash(embedded, 10, ACCUMULATOR_THRESHOLD))
                .isEqualTo(XxHash64.hash(window))
                .isEqualTo(-5372929702691221627L);
    }

    static Stream<Arguments> longVectors() {
        return Stream.of(
                Arguments.of(0L, 3803688792395291579L),
                Arguments.of(1L, -6977822845260490347L),
                Arguments.of(-1L, -8804195676797548855L),
                Arguments.of(63L, 4303055942782292998L),
                Arguments.of(0x0123456789ABCDEFL, -1568288375405263892L),
                Arguments.of(Long.MIN_VALUE, 4558309869707674848L));
    }

    @ParameterizedTest(name = "hash(long {0})")
    @MethodSource("longVectors")
    void hashesLongAgainstReferenceVectors(long value, long expected) {
        assertThat(XxHash64.hash(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "hash(long {0}) == hash(little-endian bytes)")
    @ValueSource(longs = {0L, 1L, -1L, 63L, Long.MIN_VALUE, Long.MAX_VALUE, 0x0123456789ABCDEFL})
    void longOverloadMatchesItsLittleEndianPlainEncoding(long value) {
        assertThat(XxHash64.hash(value)).isEqualTo(XxHash64.hash(toLittleEndian(value)));
    }

    static Stream<Arguments> intVectors() {
        return Stream.of(
                Arguments.of(0, 4246796580750024372L),
                Arguments.of(1, -851299076295404719L),
                Arguments.of(-1, 9185342943168159635L),
                Arguments.of(189, 9081713793764081282L),
                Arguments.of(0x01234567, -1310445905565718912L),
                Arguments.of(Integer.MIN_VALUE, -9066219797205712013L),
                Arguments.of(Integer.MAX_VALUE, 2971168436322821236L));
    }

    @ParameterizedTest(name = "hash(int {0})")
    @MethodSource("intVectors")
    void hashesIntAgainstReferenceVectors(int value, long expected) {
        assertThat(XxHash64.hash(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "hash(int {0}) == hash(little-endian bytes)")
    @ValueSource(ints = {0, 1, -1, 189, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x01234567})
    void intOverloadMatchesItsLittleEndianPlainEncoding(int value) {
        assertThat(XxHash64.hash(value)).isEqualTo(XxHash64.hash(toLittleEndian(value)));
    }

    /// ASCII bytes of `s`, used so the reference vectors read as the literal strings they hash.
    private static byte[] ascii(String s) {
        byte[] out = new byte[s.length()];
        for (int i = 0; i < s.length(); i++) {
            out[i] = (byte) s.charAt(i);
        }
        return out;
    }

    /// `length` bytes holding `0, 1, 2, ...` (wrapping past 255), matching the Python reference
    /// inputs `bytes(range(length))`.
    private static byte[] sequential(int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) i;
        }
        return out;
    }

    private static byte[] toLittleEndian(long value) {
        byte[] out = new byte[Long.BYTES];
        for (int i = 0; i < Long.BYTES; i++) {
            out[i] = (byte) (value >>> (i * 8));
        }
        return out;
    }

    private static byte[] toLittleEndian(int value) {
        byte[] out = new byte[Integer.BYTES];
        for (int i = 0; i < Integer.BYTES; i++) {
            out[i] = (byte) (value >>> (i * 8));
        }
        return out;
    }
}
