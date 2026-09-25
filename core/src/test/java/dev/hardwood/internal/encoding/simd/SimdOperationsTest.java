/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks that [ScalarOperations] and the Vector API implementation `VectorOperations`
/// produce identical results.
///
/// `VectorOperations` is compiled into the multi-release overlay
/// (`META-INF/versions/22`). When the tests run from the class directory rather than the
/// packaged JAR, the JVM does not resolve that overlay, and [VectorSupport] selects the
/// scalar kernels. The vector class is then loaded from the overlay directory through a
/// child class loader, so the comparison always runs against the vector kernels.
class SimdOperationsTest {

    private static final String VECTOR_OPERATIONS = "dev.hardwood.internal.encoding.simd.VectorOperations";

    private static final SimdOperations SCALAR = new ScalarOperations();
    private static SimdOperations SIMD;
    private static URLClassLoader overlayLoader;
    private static final Random RANDOM = new Random(42);

    @BeforeAll
    static void setup() throws ReflectiveOperationException, URISyntaxException, MalformedURLException {
        assertThat(ModuleLayer.boot().findModule("jdk.incubator.vector"))
                .as("Vector API module jdk.incubator.vector is not loaded")
                .isPresent();

        if (VectorSupport.isAvailable()) {
            SIMD = VectorSupport.operations();
        }
        else {
            Path classes = Path.of(ScalarOperations.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path overlay = classes.resolve("META-INF/versions/22");
            assertThat(overlay).as("multi-release overlay next to %s", classes).isDirectory();
            overlayLoader = new URLClassLoader(new URL[]{ overlay.toUri().toURL() }, SimdOperationsTest.class.getClassLoader());
            SIMD = (SimdOperations) overlayLoader.loadClass(VECTOR_OPERATIONS).getDeclaredConstructor().newInstance();
        }
        assertThat(SIMD.getClass().getName()).isEqualTo(VECTOR_OPERATIONS);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (overlayLoader != null) {
            overlayLoader.close();
        }
    }

    // ==================== countNonNulls Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000, 8192})
    void countNonNullsMatchesScalar(int size) {
        int[] defLevels = generateDefLevels(size);
        int maxDef = 3;

        int scalarResult = SCALAR.countNonNulls(defLevels, defLevels.length, maxDef);
        int simdResult = SIMD.countNonNulls(defLevels, defLevels.length, maxDef);

        assertThat(simdResult).isEqualTo(scalarResult);
    }

    @Test
    void countNonNullsAllNulls() {
        int[] defLevels = new int[100];
        // All zeros, maxDef = 1 means all are null
        assertThat(SIMD.countNonNulls(defLevels, defLevels.length, 1)).isEqualTo(0);
    }

    @Test
    void countNonNullsNoNulls() {
        int[] defLevels = IntStream.range(0, 100).map(i -> 3).toArray();
        assertThat(SIMD.countNonNulls(defLevels, defLevels.length, 3)).isEqualTo(100);
    }

    @Test
    void countNonNullsAlternating() {
        int[] defLevels = new int[100];
        for (int i = 0; i < 100; i++) {
            defLevels[i] = i % 2 == 0 ? 3 : 0;
        }
        assertThat(SIMD.countNonNulls(defLevels, defLevels.length, 3)).isEqualTo(50);
    }

    @Test
    void countNonNullsIgnoresScratchTail() {
        int[] defLevels = {3, 0, 3, 3, 3, 3};
        assertThat(SCALAR.countNonNulls(defLevels, 3, 3)).isEqualTo(2);
        assertThat(SIMD.countNonNulls(defLevels, 3, 3)).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 9, 63, 64, 65, 200})
    void countNonNullsIgnoresEntriesBeyondLen(int len) {
        // A reused level buffer is longer than the page's value count: the tail
        // holds stale present-looking values that must not be counted. The lengths
        // straddle the SIMD main-loop/tail boundary (MIN_BATCH_SIZE and vector
        // width), so the vectorised path is exercised with a truncated length —
        // not just the scalar fallback that a single small length would hit.
        int[] defLevels = IntStream.range(0, len + 137).map(i -> 3).toArray();
        int expected = 0;
        for (int i = 0; i < len; i++) {
            defLevels[i] = i % 3 == 0 ? 3 : 0;
            if (defLevels[i] == 3) {
                expected++;
            }
        }
        assertThat(SCALAR.countNonNulls(defLevels, len, 3)).isEqualTo(expected);
        assertThat(SIMD.countNonNulls(defLevels, len, 3)).isEqualTo(expected);
    }

    // ==================== Dictionary Application Tests ====================

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryIntsMatchesScalar(int count) {
        int[] dict = {100, 200, 300, 400, 500, 600, 700, 800};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        int[] scalarOutput = new int[count];
        int[] simdOutput = new int[count];

        SCALAR.applyDictionaryInts(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryInts(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryLongsMatchesScalar(int count) {
        long[] dict = {100L, 200L, 300L, 400L, 500L};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        long[] scalarOutput = new long[count];
        long[] simdOutput = new long[count];

        SCALAR.applyDictionaryLongs(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryLongs(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryDoublesMatchesScalar(int count) {
        double[] dict = {1.1, 2.2, 3.3, 4.4, 5.5};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        double[] scalarOutput = new double[count];
        double[] simdOutput = new double[count];

        SCALAR.applyDictionaryDoubles(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryDoubles(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1000})
    void applyDictionaryFloatsMatchesScalar(int count) {
        float[] dict = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f, 7.7f};
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = RANDOM.nextInt(dict.length);
        }

        float[] scalarOutput = new float[count];
        float[] simdOutput = new float[count];

        SCALAR.applyDictionaryFloats(scalarOutput, dict, indices, count);
        SIMD.applyDictionaryFloats(simdOutput, dict, indices, count);

        assertThat(simdOutput).isEqualTo(scalarOutput);
    }

    @Test
    void applyDictionaryCorrectValues() {
        int[] dict = {100, 200, 300, 400, 500};
        int[] indices = {0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0, 2, 4, 1, 3, 0};
        int[] output = new int[16];

        SIMD.applyDictionaryInts(output, dict, indices, 16);

        assertThat(output[0]).isEqualTo(100);
        assertThat(output[1]).isEqualTo(300);
        assertThat(output[2]).isEqualTo(500);
        assertThat(output[3]).isEqualTo(200);
        assertThat(output[4]).isEqualTo(400);
    }

    // ==================== Helper Methods ====================

    private int[] generateDefLevels(int size) {
        int[] defLevels = new int[size];
        for (int i = 0; i < size; i++) {
            // Generate def levels 0-3 with ~25% each
            defLevels[i] = RANDOM.nextInt(4);
        }
        return defLevels;
    }
}
