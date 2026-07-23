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
| `build.sh` | Builds the WebAssembly image and assembles the publishable bundle. |
| `smoke-test.py` | Headless-Chromium check that the built bundle boots and renders (Overview + Data preview). |

## Building the WebAssembly image

`build.sh` does the whole build: it resolves the classpath from Maven (dropping the JNI/FFM
jars — native BROTLI and the JLine backend — that Web Image cannot process), strips the Java
22 FFM/Vector multi-release code from the core jar, compiles the entry points with the GraalVM
`javac` (`--add-modules org.graalvm.webimage.api`), runs `native-image --tool:svm-wasm`, and
assembles the bundle.

Prerequisites:

- **Oracle GraalVM 26-ea+** at `$GRAALVM_HOME` — it ships the `svm-wasm` tool; the 25.0.x GA
  release does not.
- **Binaryen ≥ v119** (`wasm-opt`) on `PATH`, and **gcc**.
- The hardwood artifacts installed locally:
  ```
  ./mvnw -q -pl cli -am install -DskipTests \
    -Dspotless.check.skip -Dlicense.skip -Dformatter.skip -Dimpsort.skip -Denforcer.skip
  ```

Then:

```
export GRAALVM_HOME=/path/to/graalvm-26-ea
wasm-spike/build.sh
```

→ `wasm-spike/target/web/` with the six flat bundle files, `dive-read.js` and
`dive-read.js.wasm` (~10 MB), and `dive-web-demo.zip`.

Verify the result in a headless browser (needs `chromium-browser`):

```
.docker-venv/bin/python wasm-spike/smoke-test.py
```

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

1. Run `build.sh` — it emits `target/web/dive-web-demo.zip` (the six files at the archive root).
2. Upload it to the **`1.0-early-access`** GitHub release (a rolling, unversioned asset).
3. The site repo ([`hardwood-hq.github.io`](https://github.com/hardwood-hq/hardwood-hq.github.io))
   `publish.yml` downloads that asset on each deploy and unzips it onto `gh-pages` at the fixed
   path `experiments/dive-web/` — mirroring how the JavaDoc tree is published.
4. `docs/mkdocs.yml` links it from the nav (*dive on the Web*).

To refresh the live demo: rebuild the image, re-zip, re-upload `dive-web-demo.zip` to the
`1.0-early-access` release, and run the docs publish workflow.
