# Recording an asciinema demo of `hardwood dive`

Produces `dive-demo.cast` — a terminal recording of a guided walk through the
`dive` TUI against the Overture Places sample file. Recording is **manual**:
you drive the UI by hand while `asciinema` captures the session.

Target geometry is **35 rows × 120 cols**; this is what the checked-in cast
uses and what the layout is tuned for. Keep it identical across re-recordings
so the SVG/GIF renders stay consistent.

## One-time setup

```bash
$ brew install asciinema           # or: dnf/apt install asciinema
$ brew install agg                 # optional, for GIF rendering
```

iTerm2 — create a **"Recording" profile** once, used for every take:

- `Settings → Profiles → Colors → Color Presets…` → pick **Dark Background**.
  Neutral, high-contrast, and survives the GIF/SVG render step.
- Leave **Disable session-initiated resizing** *off*. The per-recording step
  below resizes the window with an escape sequence, which that setting blocks.

Open a window with the profile via the `Profiles` menu → "Recording" → `⌘N`.

## Per-recording

```bash
# 1. Point HARDWOOD_BIN at the dive binary (adjust path / platform as needed).
$ export HARDWOOD_BIN="$PWD/hardwood-cli-early-access-macos-aarch64/bin/hardwood"

# 2. Bring up a local s3proxy and upload the Overture sample, following the
#    "Start s3proxy" + "Create bucket and upload" steps in TESTING.md. The demo
#    expects the file at s3://test-bucket/overture_places.zstd.parquet and the
#    AWS_* env vars exported as shown there.

# 3. Resize the window to 35 rows × 120 cols (CSI 8 ; rows ; cols t).
$ printf '\e[8;35;120t'

# 4. Record. COLORTERM=truecolor is the load-bearing part: dive only emits
#    24-bit RGB accent/selection colours when it sees it (Theme.supportsTruecolor),
#    otherwise it falls back to indexed ANSI 33/34 and the cast looks washed out.
$ asciinema rec dive-demo.cast \
      --rows 35 --cols 120 --idle-time-limit 2 \
      --title "hardwood dive — Overture Places" \
      -c "env COLORTERM=truecolor \"$HARDWOOD_BIN\" dive --file s3://test-bucket/overture_places.zstd.parquet"

# 5. Drive the UI by hand (see the walk below). Press q to quit dive, which
#    ends the recording.

# 6. Review.
$ asciinema play dive-demo.cast
$ asciinema play -i 2 dive-demo.cast       # cap idle gaps at 2s on playback too
```

Bad take? Just `rm dive-demo.cast` and rerun step 4.

## The walk

Pause ~1.5 s on each screen so viewers can read it (idle gaps over 2 s are
compressed on playback, so lingering costs nothing). Don't backspace typos —
restart the take instead.

1. **Overview** — let the footer fetch settle.
2. → **Footer & indexes**, linger ~2 s, then `Esc` back.
3. → **Row groups** → 5th row group → **Column chunks**.
4. → the `websites` column chunk → **Dictionary**, linger, then `Esc` back.
5. → **Pages** → first data page (page-header modal).
6. `o` to jump back to **Overview** → **Data preview** → 5th row (row modal).
7. `q` to quit.

## Share / export

```bash
$ asciinema upload dive-demo.cast                          # asciinema.org URL
$ agg dive-demo.cast dive-demo.gif                         # GIF
$ agg --theme monokai --font-size 14 dive-demo.cast dive-demo.gif
```

## Tips for clean takes

- Mute notifications and close chat apps that auto-focus the terminal.
- The shell prompt isn't part of the recording — dive launches directly via
  `-c`, so the prompt theme/length doesn't matter.
