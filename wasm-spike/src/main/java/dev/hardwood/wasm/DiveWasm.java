/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.util.function.Consumer;

import org.graalvm.webimage.api.JS;
import org.graalvm.webimage.api.JSString;

/// Browser entry point for the GraalVM Web Image (WebAssembly) build of `dive`.
///
/// The runtime is booted once: `main` installs a `globalThis.__loadFile` handler and returns,
/// but the VM stays alive (the browser is the event loop). To open a file the page stages the
/// bytes in `globalThis.__inputBytes` (a `Uint8Array`) plus the terminal size in
/// `globalThis.__cols` / `globalThis.__rows`, then calls `__loadFile()`. That handler parses
/// the file into a [DiveSession], renders the initial screen, and installs a per-keystroke
/// handler into `globalThis.__diveKey`. Every subsequent upload reuses the same VM — no
/// module re-instantiation, no runtime restart — so only the (cheap) metadata read is repeated.
///
/// Bytes cross from JavaScript as a Latin-1 binary string (one code unit per byte). This
/// avoids a base64 round-trip (no `btoa`/`atob`, no ~1.33× inflation); the Java side rebuilds
/// the `byte[]` by narrowing each `char`. Zero-copy typed-array transfer (`Int8Array` →
/// `byte[]`) is not available: the Web Image WASM backend does not emit the array-coercion
/// runtime, so it fails with `ReferenceError: byteArrayHub is not defined`.
///
/// (`@JS.Export` — calling a Java class directly from JavaScript — is not yet implemented in
/// Web Image, so handlers are exposed as lambdas stored on globals via `@JS` helpers.)
public final class DiveWasm {

    private static DiveSession session;

    private DiveWasm() {
    }

    /// The uploaded bytes as a Latin-1 string (each code unit is one byte), or `""` if none.
    @JS.Coerce
    @JS("return (typeof globalThis.__inputStr === 'string') ? globalThis.__inputStr : '';")
    private static native String readInputStr();

    @JS.Coerce
    @JS("return (typeof globalThis.__cols === 'number') ? globalThis.__cols : 0;")
    private static native int readCols();

    @JS.Coerce
    @JS("return (typeof globalThis.__rows === 'number') ? globalThis.__rows : 0;")
    private static native int readRows();

    /// Install the file-open handler as a JavaScript global. The page stages `__inputBytes`
    /// and calls it with no arguments; the resulting frame is published via [#writeOutput].
    @JS(args = { "handler" }, value = "globalThis.__loadFile = handler;")
    private static native void exportLoadFile(Runnable handler);

    /// Install the per-keystroke handler as a JavaScript global. The page calls it with the
    /// raw xterm.js input; the handler renders the next frame into `globalThis.__output`. The
    /// argument is a [JSString] (the raw JS value, no coercion needed); the frame is published
    /// through [#writeOutput] rather than returned, because a Java return value is not coerced
    /// back to JavaScript at the lambda boundary.
    @JS(args = { "handler" }, value = "globalThis.__diveKey = handler;")
    private static native void exportKeyHandler(Consumer<JSString> handler);

    /// Publish an ANSI frame back to the page.
    @JS.Coerce
    @JS("globalThis.__output = text;")
    private static native void writeOutput(String text);

    public static void main(String[] args) {
        // Boot once: install the load handler and return. The VM stays alive, so each upload
        // just calls globalThis.__loadFile() — no re-instantiation of the WebAssembly module.
        exportLoadFile(DiveWasm::loadCurrentFile);
    }

    private static void loadCurrentFile() {
        try {
            String encoded = readInputStr();
            if (encoded.isEmpty()) {
                writeOutput("hardwood dive (wasm) — select a Parquet file\r\n");
                return;
            }
            byte[] data = new byte[encoded.length()];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) encoded.charAt(i);
            }
            session = DiveSession.open(data, readCols(), readRows());
            exportKeyHandler(key -> writeOutput(session.onKey(key.asString())));
            writeOutput(session.render());
        }
        catch (Throwable t) {
            StringBuilder sb = new StringBuilder("load failed: " + t.getClass().getName() + ": " + t.getMessage());
            StackTraceElement[] st = t.getStackTrace();
            for (int i = 0; i < Math.min(5, st.length); i++) {
                sb.append("\r\n  at ").append(st[i]);
            }
            writeOutput(sb.toString());
        }
    }
}
