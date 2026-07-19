# dive on the Web — WebAssembly spike

Compiles the `hardwood dive` interactive TUI to WebAssembly with **GraalVM Web Image** so it
runs entirely in a browser. A visitor uploads a Parquet file (or clicks *Load a sample file*)
and navigates all of dive's screens — the file never leaves their machine.

Live at **<https://hardwood.dev/experiments/dive-web>**. Tracking issue: #801.

> Standalone module: not listed in the root `<modules>` and not built by `./mvnw verify`. It
> controls its own classpath precisely (no native codec jars) to keep the read path JNI-free.

## Layout

| Path | What it is |
| --- | --- |
| `src/main/java/dev/hardwood/wasm/DiveWasm.java` | Browser entry point. Installs `globalThis.__loadFile` / `__diveKey` and publishes ANSI frames to `globalThis.__output`. |
| `src/main/java/dev/hardwood/wasm/DiveSession.java` | Drives the real `DiveApp` headlessly (`renderOnce` / `dispatchKey`); the browser is the event loop, so no threads. |
| `src/main/java/dev/hardwood/wasm/PureJavaDecompressors.java` | Per-codec overrides for a JNI-free read path. |
| `src/main/java/dev/hardwood/wasm/SnappyBlock.java` | Self-contained Snappy block decoder (no `sun.misc.Unsafe`). |
| `src/main/java/dev/hardwood/wasm/GzipInflate.java` | GZIP via jzlib (pure-Java zlib; `java.util.zip.Inflater` is native and unavailable). |
| `web/index.html` | The browser host page: xterm.js terminal, upload / drag-drop / sample button, keyboard nav, and a WebAssembly-capability gate. |
| `web/xterm.js`, `web/xterm.css` | Vendored [xterm.js](https://xtermjs.org/) terminal renderer. |
| `web/demo-sample.parquet` | The *Load a sample file* file (full 19-column NYC taxi schema, 3 row groups, Snappy). |
| `web/generate-demo-sample.py` | Regenerates `demo-sample.parquet`. |

## Building the WebAssembly image

Requires **Oracle GraalVM 26-ea+** (ships the `svm-wasm` tool; 25.0.x GA does not),
**Binaryen ≥ v119** on `PATH` (for `wasm-opt`), and **gcc**.

1. Install `hardwood-core` and strip `META-INF/versions/*` from the jar (so closed-world
   analysis never sees the Java 22 FFM/Vector code), producing a `hardwood-core-base.jar`.
2. Compile the sources with the GraalVM `javac`:
   ```
   javac -parameters --add-modules org.graalvm.webimage.api \
     -cp <core-base:aircompressor:brotli:jzlib:tamboui:hardwood-cli> \
     -d out web/../src/main/java/dev/hardwood/wasm/*.java
   ```
3. Run `native-image` to emit the JS glue + `.wasm`:
   ```
   native-image --tool:svm-wasm --no-fallback \
     -cp <out:...same deps...> dev.hardwood.wasm.DiveWasm -o dive-read
   ```
   → `dive-read.js` and `dive-read.js.wasm` (~10 MB).

### Codec support

`UNCOMPRESSED`, `SNAPPY` (via `SnappyBlock`), and `GZIP` (via jzlib) work. `ZSTD` and `LZ4`
do **not**: their decoders rely on `sun.misc.Unsafe`, which Web Image does not support.

### Browser requirements

The image uses WebAssembly **garbage collection** and **exception handling** — a recent
Chrome, Edge, Firefox, or Arc. Safari and older versions are unsupported; `index.html`
feature-detects this and shows a message instead of failing opaquely.

## Bundle & publication flow

The published bundle is six **flat** files (no nesting), relocatable to any directory:

```
index.html            web/index.html
dive-read.js          native-image output
dive-read.js.wasm     native-image output (~10 MB)
xterm.js              web/xterm.js
xterm.css             web/xterm.css
demo-sample.parquet   web/demo-sample.parquet
```

`index.html` references its siblings by relative paths, and `dive-read.js` derives the wasm
URL from its own `src`, so the set works under any path with no absolute-URL assumptions.

Because building the image needs the GraalVM toolchain (not yet wired into CI), the bundle is
published as a **prebuilt snapshot**:

1. Zip the six files at the archive root as `dive-web-demo.zip`.
2. Upload it to the **`1.0-early-access`** GitHub release (a rolling, unversioned asset).
3. The site repo ([`hardwood-hq.github.io`](https://github.com/hardwood-hq/hardwood-hq.github.io))
   `publish.yml` downloads that asset on each deploy and unzips it onto `gh-pages` at the fixed
   path `experiments/dive-web/` — mirroring how the JavaDoc tree is published.
4. `docs/mkdocs.yml` links it from the nav (*dive on the Web*).

To refresh the live demo: rebuild the image, re-zip, re-upload `dive-web-demo.zip` to the
`1.0-early-access` release, and run the docs publish workflow.
