/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import java.io.File;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static dev.hardwood.cli.NativeLibraryLoader.Codec.LZ4;
import static dev.hardwood.cli.NativeLibraryLoader.Codec.SNAPPY;
import static dev.hardwood.cli.NativeLibraryLoader.Codec.ZSTD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NativeLibraryLoaderTest {

    private static final String SEP = File.separator;

    private String savedOsName;
    private String savedOsArch;

    @BeforeEach
    void saveSystemProperties() {
        savedOsName = System.getProperty("os.name");
        savedOsArch = System.getProperty("os.arch");
    }

    @AfterEach
    void restoreSystemProperties() {
        restoreProperty("os.name", savedOsName);
        restoreProperty("os.arch", savedOsArch);
    }

    private static void restoreProperty(String key, String saved) {
        if (saved == null) {
            System.clearProperty(key);
        }
        else {
            System.setProperty(key, saved);
        }
    }

    @ParameterizedTest(name = "{0} os={1} arch={2} -> {3}")
    @MethodSource("fragmentCases")
    void osArchFragment(NativeLibraryLoader.Codec codec, String osName, String osArch, String expectedFragment) {
        System.setProperty("os.name", osName);
        System.setProperty("os.arch", osArch);
        assertThat(NativeLibraryLoader.osArchFragment(codec)).isEqualTo(expectedFragment);
    }

    static Stream<Arguments> fragmentCases() {
        return Stream.of(
                // ZSTD: darwin/aarch64, darwin/x86_64, linux/aarch64, linux/amd64, win/amd64
                arguments(ZSTD, "Mac OS X",   "aarch64", "darwin" + SEP + "aarch64"),
                arguments(ZSTD, "Mac OS X",   "arm64",   "darwin" + SEP + "aarch64"),
                arguments(ZSTD, "Mac OS X",   "x86_64",  "darwin" + SEP + "x86_64"),
                arguments(ZSTD, "Linux",      "aarch64", "linux"  + SEP + "aarch64"),
                arguments(ZSTD, "Linux",      "amd64",   "linux"  + SEP + "amd64"),
                arguments(ZSTD, "Linux",      "x86_64",  "linux"  + SEP + "amd64"),
                arguments(ZSTD, "Windows 10", "amd64",   "win"    + SEP + "amd64"),
                arguments(ZSTD, "SunOS",      "aarch64", null),

                // LZ4: darwin/aarch64, darwin/x86_64, linux/aarch64, linux/amd64, win32/amd64
                arguments(LZ4, "Mac OS X",   "aarch64", "darwin" + SEP + "aarch64"),
                arguments(LZ4, "Mac OS X",   "arm64",   "darwin" + SEP + "aarch64"),
                arguments(LZ4, "Mac OS X",   "x86_64",  "darwin" + SEP + "x86_64"),
                arguments(LZ4, "Mac OS X",   "amd64",   "darwin" + SEP + "x86_64"),
                arguments(LZ4, "Linux",      "aarch64", "linux"  + SEP + "aarch64"),
                arguments(LZ4, "Linux",      "amd64",   "linux"  + SEP + "amd64"),
                arguments(LZ4, "Linux",      "x86_64",  "linux"  + SEP + "amd64"),
                arguments(LZ4, "Windows 10", "amd64",   "win32"  + SEP + "amd64"),
                arguments(LZ4, "SunOS",      "aarch64", null),

                // SNAPPY: Mac/aarch64, Linux/x86_64, Windows/x86_64, Linux/x86
                arguments(SNAPPY, "Mac OS X",   "aarch64", "Mac"     + SEP + "aarch64"),
                arguments(SNAPPY, "Mac OS X",   "arm64",   "Mac"     + SEP + "aarch64"),
                arguments(SNAPPY, "Mac OS X",   "x86_64",  "Mac"     + SEP + "x86_64"),
                arguments(SNAPPY, "Linux",      "aarch64", "Linux"   + SEP + "aarch64"),
                arguments(SNAPPY, "Linux",      "amd64",   "Linux"   + SEP + "x86_64"),
                arguments(SNAPPY, "Linux",      "x86_64",  "Linux"   + SEP + "x86_64"),
                arguments(SNAPPY, "Linux",      "x86",     "Linux"   + SEP + "x86"),
                arguments(SNAPPY, "Linux",      "i386",    "Linux"   + SEP + "x86"),
                arguments(SNAPPY, "Windows 10", "amd64",   "Windows" + SEP + "x86_64"),
                arguments(SNAPPY, "SunOS",      "aarch64", null));
    }
}