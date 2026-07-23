/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.nio.ByteBuffer;

import dev.hardwood.HardwoodContext;
import dev.hardwood.cli.dive.DiveApp;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.internal.reader.ByteBufferInputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

/// A live `dive` session held in the WebAssembly heap across keystrokes.
///
/// This drives the real [DiveApp] — the same chrome, screens, navigation stack, and key
/// handling used by the terminal `dive` — headlessly: [DiveApp#renderOnce] paints a frame
/// into an in-memory [Buffer], and [DiveApp#dispatchKey] applies a keystroke. The browser is
/// the event loop, so there is no blocking input loop and no threads.
///
/// Screens that read row data (the data preview) need the reader's concurrent decode path,
/// which the single-threaded WebAssembly runtime cannot run; navigating there is caught and
/// shown as a message instead of crashing.
final class DiveSession {

    private static final char ESC = '\u001b';
    private static final char DEL = '\u007f';

    private final DiveApp app;
    private final String error;
    private final int cols;
    private final int rows;

    private DiveSession(DiveApp app, String error, int cols, int rows) {
        this.app = app;
        this.error = error;
        this.cols = cols > 0 ? cols : 100;
        this.rows = rows > 0 ? rows : 30;
    }

    /// Open the file (metadata only) and build a dive app around it.
    static DiveSession open(byte[] parquetBytes, int cols, int rows) {
        System.setProperty("hardwood.uselibdeflate", "false");
        try {
            // Thread-free, pure-Java-codec context: the reader runs in pull-based synchronous
            // mode (no virtual threads) and decodes with pure-Java codecs (no JNI) — both
            // required in a WebAssembly runtime.
            HardwoodContext context = HardwoodContextImpl.synchronous(PureJavaDecompressors.overrides());
            ParquetModel model = ParquetModel.open(
                    new ByteBufferInputFile(ByteBuffer.wrap(parquetBytes)), "uploaded.parquet", context);
            return new DiveSession(new DiveApp(model), null, cols, rows);
        }
        catch (Exception e) {
            return new DiveSession(null, e.getClass().getSimpleName() + ": " + e.getMessage(), cols, rows);
        }
    }

    /// Apply one keystroke (raw xterm.js input) and return the new ANSI frame.
    String onKey(String key) {
        if (app != null) {
            KeyEvent event = toKeyEvent(key);
            if (event != null) {
                try {
                    app.dispatchKey(event);
                }
                catch (Throwable ignored) {
                    // A screen may fail (e.g. data preview needs row decoding); recover to root.
                    app.stack().clearToRoot();
                }
            }
        }
        return render();
    }

    /// Render the current screen to a full ANSI frame.
    String render() {
        Buffer buffer = Buffer.empty(new Rect(0, 0, cols, rows));
        if (error != null) {
            buffer.setString(1, 1, fit("error — " + error, cols - 2), Style.create().red());
            return buffer.toAnsiString();
        }
        try {
            app.renderOnce(buffer);
        }
        catch (Throwable t) {
            // The active screen could not render in the browser runtime (typically a data
            // screen needing the reader's threaded decode path). Show a message and step back.
            app.stack().clearToRoot();
            buffer = Buffer.empty(new Rect(0, 0, cols, rows));
            Throwable root = t;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            buffer.setString(1, 1, fit("This screen can't render in the WebAssembly build yet.",
                    cols - 2), Style.create().yellow());
            buffer.setString(1, 3, fit("why: " + t.getMessage(), cols - 2), Style.create().dim());
            buffer.setString(1, 4, fit("root: " + root.getClass().getSimpleName() + ": "
                    + root.getMessage(), cols - 2), Style.create().red());
            buffer.setString(1, 6, fit("Press any key to return.", cols - 2), Style.create().dim());
        }
        return buffer.toAnsiString();
    }

    /// Map raw xterm.js input to a Tamboui [KeyEvent].
    private static KeyEvent toKeyEvent(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }
        char first = key.charAt(0);
        if (first == ESC) {
            if (key.length() == 1) {
                return KeyEvent.ofKey(KeyCode.ESCAPE);
            }
            KeyModifiers mods = parseModifiers(key);
            char last = key.charAt(key.length() - 1);
            return switch (last) {
                case 'A' -> KeyEvent.ofKey(KeyCode.UP, mods);
                case 'B' -> KeyEvent.ofKey(KeyCode.DOWN, mods);
                case 'C' -> KeyEvent.ofKey(KeyCode.RIGHT, mods);
                case 'D' -> KeyEvent.ofKey(KeyCode.LEFT, mods);
                case 'H' -> KeyEvent.ofKey(KeyCode.HOME, mods);
                case 'F' -> KeyEvent.ofKey(KeyCode.END, mods);
                case '~' -> key.contains("5") ? KeyEvent.ofKey(KeyCode.PAGE_UP)
                        : key.contains("6") ? KeyEvent.ofKey(KeyCode.PAGE_DOWN)
                        : KeyEvent.ofKey(KeyCode.ESCAPE);
                default -> KeyEvent.ofKey(KeyCode.ESCAPE);
            };
        }
        if (first == '\r' || first == '\n') {
            return KeyEvent.ofKey(KeyCode.ENTER);
        }
        if (first == '\t') {
            return KeyEvent.ofKey(KeyCode.TAB);
        }
        if (first == DEL || first == '\b') {
            return KeyEvent.ofKey(KeyCode.BACKSPACE);
        }
        return KeyEvent.ofChar(first);
    }

    /// Parse an xterm CSI cursor-key modifier: `ESC [ 1 ; <mod> <final>`, where
    /// `mod = 1 + (shift?1:0) + (alt?2:0) + (ctrl?4:0)`. Returns [KeyModifiers#NONE] when absent.
    private static KeyModifiers parseModifiers(String key) {
        int semi = key.indexOf(';');
        if (semi < 0) {
            return KeyModifiers.NONE;
        }
        int i = semi + 1;
        int mod = 0;
        while (i < key.length() && key.charAt(i) >= '0' && key.charAt(i) <= '9') {
            mod = mod * 10 + (key.charAt(i) - '0');
            i++;
        }
        if (mod <= 1) {
            return KeyModifiers.NONE;
        }
        int bits = mod - 1;
        return KeyModifiers.of((bits & 4) != 0, (bits & 2) != 0, (bits & 1) != 0);
    }

    private static String fit(String s, int width) {
        if (width <= 0) {
            return "";
        }
        return s.length() > width ? s.substring(0, width) : s;
    }
}
