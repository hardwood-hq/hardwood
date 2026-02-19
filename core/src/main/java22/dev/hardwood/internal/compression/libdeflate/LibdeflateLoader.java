/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Handles loading the libdeflate native library across platforms.
 */
public final class LibdeflateLoader {

    private static final String[] LINUX_NAMES = {
            "libdeflate.so.0", "libdeflate.so"
    };
    private static final String[] MACOS_NAMES = {
            "libdeflate.0.dylib", "libdeflate.dylib"
    };
    private static final String[] WINDOWS_NAMES = {
            "deflate.dll", "libdeflate.dll"
    };

    private static volatile SymbolLookup symbolLookup;
    private static volatile boolean loadAttempted;
    private static volatile Throwable loadError;

    private LibdeflateLoader() {
    }

    /**
     * Returns true if libdeflate is available on this system.
     */
    public static boolean isAvailable() {
        ensureLoaded();
        return symbolLookup != null;
    }

    /**
     * Returns the symbol lookup for libdeflate, or throws if unavailable.
     */
    static SymbolLookup getSymbolLookup() {
        ensureLoaded();
        if (symbolLookup == null) {
            throw new UnsupportedOperationException(
                    "libdeflate is not available: " +
                            (loadError != null ? loadError.getMessage() : "library not found"),
                    loadError);
        }
        return symbolLookup;
    }

    private static synchronized void ensureLoaded() {
        if (loadAttempted) {
            return;
        }
        loadAttempted = true;

        String[] libraryNames = getLibraryNames();

        // Strategy 1: Try system library path
        for (String name : libraryNames) {
            try {
                symbolLookup = SymbolLookup.libraryLookup(name, Arena.global());
                return;
            }
            catch (IllegalArgumentException e) {
                // Library not found in system path, continue
            }
        }

        // Strategy 2: Try common installation paths
        for (Path searchPath : getSearchPaths()) {
            for (String name : libraryNames) {
                Path libPath = searchPath.resolve(name);
                if (Files.exists(libPath)) {
                    try {
                        symbolLookup = SymbolLookup.libraryLookup(libPath, Arena.global());
                        return;
                    }
                    catch (IllegalArgumentException e) {
                        loadError = e;
                    }
                }
            }
        }

        loadError = new UnsupportedOperationException(
                "libdeflate not found. Install via: " + getInstallInstructions());
    }

    private static String[] getLibraryNames() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("linux")) {
            return LINUX_NAMES;
        }
        else if (os.contains("mac") || os.contains("darwin")) {
            return MACOS_NAMES;
        }
        else if (os.contains("windows")) {
            return WINDOWS_NAMES;
        }
        return LINUX_NAMES;
    }

    private static Path[] getSearchPaths() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("linux")) {
            return new Path[]{
                    Path.of("/usr/lib"),
                    Path.of("/usr/lib64"),
                    Path.of("/usr/local/lib"),
                    Path.of("/usr/lib/x86_64-linux-gnu"),
                    Path.of("/usr/lib/aarch64-linux-gnu")
            };
        }
        else if (os.contains("mac") || os.contains("darwin")) {
            return new Path[]{
                    Path.of("/usr/local/lib"),
                    Path.of("/opt/homebrew/lib"),
                    Path.of("/usr/lib")
            };
        }
        else if (os.contains("windows")) {
            String userProfile = System.getenv("USERPROFILE");
            if (userProfile != null) {
                return new Path[]{
                        Path.of("C:\\Windows\\System32"),
                        Path.of(userProfile, "lib")
                };
            }
            return new Path[]{
                    Path.of("C:\\Windows\\System32")
            };
        }
        return new Path[0];
    }

    private static String getInstallInstructions() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("linux")) {
            return "apt install libdeflate-dev (Debian/Ubuntu) or dnf install libdeflate-devel (Fedora)";
        }
        else if (os.contains("mac") || os.contains("darwin")) {
            return "brew install libdeflate";
        }
        else if (os.contains("windows")) {
            return "vcpkg install libdeflate or download from https://github.com/ebiggers/libdeflate/releases";
        }
        return "https://github.com/ebiggers/libdeflate";
    }
}
