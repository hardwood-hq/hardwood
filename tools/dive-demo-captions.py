#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# tools/dive-demo-captions.py — add the tape's captions to the dive demo recording as markers.
#
# Usage:
#   python3 tools/dive-demo-captions.py <tape> <cast>
#
# Each `# @caption <text>` line in the tape becomes an asciicast v2 marker event
# (`[time, "m", text]`) in the cast, rewritten in place. A caption's time is the sum of the tape's
# `Sleep` durations before it; key presses take next to no time, so this tracks the recording to
# within a few hundred milliseconds. Markers already in the cast are replaced, so the script can run
# again on its own output. Afterwards every gap between events longer than MAX_GAP_SECONDS is
# shortened to MAX_GAP_SECONDS, markers included, so that times in the file are playback times and
# the docs page can place its caption bar from them without the player's idle-time compression.

import json
import pathlib
import re
import sys

CAPTION = re.compile(r"#\s*@caption\s+(.+)")
SLEEP = re.compile(r"Sleep\s+(\d+(?:\.\d+)?)(ms|s)", re.IGNORECASE)
MAX_GAP_SECONDS = 4.0


def read_captions(tape: pathlib.Path) -> list[tuple[float, str]]:
    elapsed = 0.0
    captions = []
    for line in tape.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        caption = CAPTION.fullmatch(line)
        if caption:
            captions.append((elapsed, caption.group(1).strip()))
            continue
        sleep = SLEEP.fullmatch(line)
        if sleep:
            amount = float(sleep.group(1))
            elapsed += amount / 1000 if sleep.group(2).lower() == "ms" else amount
    if not captions:
        sys.exit(f"{tape}: no '# @caption' lines")
    return captions


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: dive-demo-captions.py <tape> <cast>")
    tape, cast = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
    lines = cast.read_text(encoding="utf-8").splitlines()
    header = json.loads(lines[0])
    if header.get("version") != 2:
        sys.exit(f"{cast}: expected an asciicast v2 file, found version {header.get('version')}")

    events = [json.loads(line) for line in lines[1:] if line.strip()]
    events = [event for event in events if event[1] != "m"]
    events += [[time, "m", text] for time, text in read_captions(tape)]
    # Stable sort: a marker at the same time as an output event follows it.
    events.sort(key=lambda event: event[0])

    previous_original = 0.0
    shifted = 0.0
    for event in events:
        gap = event[0] - previous_original
        previous_original = event[0]
        shifted += min(gap, MAX_GAP_SECONDS)
        event[0] = round(shifted, 6)

    out = [json.dumps(header, ensure_ascii=False)]
    out += [json.dumps(event, ensure_ascii=False) for event in events]
    cast.write_text("\n".join(out) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
