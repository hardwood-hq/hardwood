#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
# Build the dive-on-the-web WebAssembly bundle.
#
# Prerequisites:
#   * GRAALVM_HOME points at an Oracle GraalVM 26-ea+ that ships the svm-wasm tool
#     (`native-image --tool:svm-wasm`). The 25.0.x GA release does not include it.
#   * Binaryen's `wasm-opt` (v119+) is on PATH, and gcc is installed.
#   * The hardwood artifacts are installed in the local Maven repository:
#       ./mvnw -q -pl cli -am install -DskipTests \
#         -Dspotless.check.skip -Dlicense.skip -Dformatter.skip -Dimpsort.skip -Denforcer.skip
#
# Output: target/web/ with the six flat bundle files and dive-web-demo.zip (the asset
# uploaded to the release; see README.md).
set -euo pipefail

MODULE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$MODULE/.." && pwd)"
OUT="${OUT:-$MODULE/target/web}"

: "${GRAALVM_HOME:?set GRAALVM_HOME to a GraalVM that ships the svm-wasm tool}"
[ -x "$GRAALVM_HOME/bin/native-image" ] || { echo "no native-image under GRAALVM_HOME=$GRAALVM_HOME" >&2; exit 1; }
command -v wasm-opt >/dev/null || { echo "wasm-opt (Binaryen v119+) not on PATH" >&2; exit 1; }
export JAVA_HOME="$GRAALVM_HOME"
mkdir -p "$MODULE/target"

echo ">> resolving classpath from Maven"
"$ROOT/mvnw" -q -f "$MODULE/pom.xml" dependency:build-classpath -Dmdep.outputFile="$MODULE/target/cp.txt"
# Web Image needs a minimal, JNI/FFM-free classpath. Keep only the hardwood modules, the
# pure-Java codecs (aircompressor, org.brotli.dec, jzlib), and the tamboui render/widget jars.
# Everything else Maven pulls in transitively — the native codecs (snappy-java, zstd-jni,
# lz4-java), the AWS SDK, aesh, and JLine — is unused by the browser read path and would only
# bloat (or break) the closed-world image, so it is filtered out.
CP=$(tr ':' '\n' < "$MODULE/target/cp.txt" | grep -E \
  'dev/hardwood/hardwood-(core|cli|s3|aws-auth)/|io/airlift/aircompressor/|org/brotli/dec/|com/jcraft/jzlib/|dev/tamboui/tamboui-(core|widgets|annotations|tui)/' \
  | paste -sd:)

echo ">> stripping multi-release (Java 22 FFM/Vector) code from the core jar"
CORE=$(echo "$CP" | tr ':' '\n' | grep 'hardwood-core/' | head -1)
BASE="$MODULE/target/hardwood-core-base.jar"
cp "$CORE" "$BASE"
zip -q -d "$BASE" 'META-INF/versions/*' >/dev/null 2>&1 || true
CP=${CP//$CORE/$BASE}

echo ">> compiling WebAssembly entry points"
CLASSES="$MODULE/target/wasm-classes"
rm -rf "$CLASSES"; mkdir -p "$CLASSES"
"$GRAALVM_HOME/bin/javac" -parameters --add-modules org.graalvm.webimage.api \
  -cp "$CP" -d "$CLASSES" "$MODULE"/src/main/java/dev/hardwood/wasm/*.java

echo ">> native-image (svm-wasm)"
rm -rf "$OUT"; mkdir -p "$OUT"
( cd "$OUT" && "$GRAALVM_HOME/bin/native-image" --tool:svm-wasm --no-fallback \
    -cp "$CLASSES:$CP" dev.hardwood.wasm.DiveWasm -o dive-read )

echo ">> assembling bundle"
cp "$MODULE"/web/index.html "$MODULE"/web/xterm.js "$MODULE"/web/xterm.css \
   "$MODULE"/web/demo-sample.parquet "$OUT/"
( cd "$OUT" && zip -qr9 dive-web-demo.zip \
    index.html dive-read.js dive-read.js.wasm xterm.js xterm.css demo-sample.parquet )
echo ">> done: $OUT/dive-web-demo.zip"
