#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
"""Headless smoke test for the dive-on-the-web WebAssembly bundle.

Serves a built bundle, drives it in headless Chromium via the DevTools Protocol,
loads the bundled sample file, and asserts that the WebAssembly image actually
boots and renders real Parquet content (Overview, then Data preview).

Prerequisites:
  * A built bundle (run build.sh first). Defaults to target/web next to this script;
    pass a directory to override.
  * chromium-browser (or set CHROMIUM to another Chromium/Chrome binary).
  * websocket-client (the project's .docker-venv has it):
      .docker-venv/bin/python wasm-spike/smoke-test.py

Exits 0 on success, 1 on failure.
"""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

import websocket  # from websocket-client

HERE = os.path.dirname(os.path.abspath(__file__))
BUNDLE = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else os.path.join(HERE, "target", "web")
CHROMIUM = os.environ.get("CHROMIUM", "chromium-browser")


def free_port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


HTTP_PORT = free_port()
CDP_PORT = free_port()


def main():
    if not os.path.isfile(os.path.join(BUNDLE, "index.html")):
        print(f"FAIL: no index.html under {BUNDLE} — run build.sh first", file=sys.stderr)
        return 1

    server = subprocess.Popen([sys.executable, "-m", "http.server", str(HTTP_PORT)],
                              cwd=BUNDLE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    chrome = subprocess.Popen(
        [CHROMIUM, "--headless", "--no-sandbox", "--disable-gpu",
         f"--remote-debugging-port={CDP_PORT}", "--remote-allow-origins=*", "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # Wait until our server actually serves the bundle (fail fast if the port was taken).
        served = False
        for _ in range(50):
            try:
                if b"hardwood dive" in urllib.request.urlopen(
                        f"http://localhost:{HTTP_PORT}/index.html", timeout=1).read():
                    served = True
                    break
            except Exception:
                pass
            time.sleep(0.1)
        if not served:
            print(f"FAIL: bundle not served on port {HTTP_PORT}", file=sys.stderr)
            return 1

        ws_url = None
        for _ in range(50):
            try:
                for target in json.load(urllib.request.urlopen(f"http://localhost:{CDP_PORT}/json")):
                    if target.get("type") == "page" and target.get("webSocketDebuggerUrl"):
                        ws_url = target["webSocketDebuggerUrl"]
                        break
                if ws_url:
                    break
            except Exception:
                pass
            time.sleep(0.2)
        if not ws_url:
            print("FAIL: could not reach the Chromium DevTools endpoint", file=sys.stderr)
            return 1

        conn = websocket.create_connection(ws_url, max_size=None)
        msg_id = [0]

        def cmd(method, params=None):
            msg_id[0] += 1
            conn.send(json.dumps({"id": msg_id[0], "method": method, "params": params or {}}))
            while True:
                reply = json.loads(conn.recv())
                if reply.get("id") == msg_id[0]:
                    return reply

        def ev(expr):
            r = cmd("Runtime.evaluate", {"expression": expr, "returnByValue": True, "awaitPromise": True})
            return r["result"]["result"].get("value")

        cmd("Runtime.enable")
        cmd("Page.enable")
        cmd("Page.navigate", {"url": f"http://localhost:{HTTP_PORT}/index.html"})
        time.sleep(2)

        # Boot the VM, then load the bundled sample through the page's own button.
        ev("(async () => { await vmReady; })()")
        ev("document.getElementById('sample').click()")

        line0 = "(unset)"
        for _ in range(30):
            time.sleep(0.5)
            line0 = ev("term.buffer.active.getLine(0).translateToString(true)") or ""
            if "1500 rows" in line0:
                break
        check("Overview renders the sample", "3 RGs" in line0 and "1500 rows" in line0, line0.strip())

        # Drill into Data preview (menu: Schema / Row groups / Footer / Data preview) and
        # confirm real rows decode.
        for key in ("[B", "[B", "[B", "\r"):
            ev(f"dispatch({json.dumps(key)})")
            time.sleep(0.5)
        time.sleep(0.5)
        header = ev("term.buffer.active.getLine(2).translateToString(true)") or ""
        cols = ev("term.buffer.active.getLine(3).translateToString(true)") or ""
        check("Data preview decodes rows", "of 1,500" in header and "VendorID" in cols, header.strip())

        print("SMOKE: PASS")
        return 0
    finally:
        chrome.terminate()
        server.terminate()


def check(label, ok, detail):
    print(f"  [{'ok' if ok else 'FAIL'}] {label}: {detail}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    sys.exit(main())
