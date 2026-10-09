#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Scripts the walk through `dive` recorded as docs/content/assets/cli/dive-demo.cast.
# The recording output, terminal size, fps and duration cap are supplied by the
# `dive-demo` Maven profile via -Dtamboui.record* system properties.
#
# The single key presses with short pauses in between let a viewer follow the
# cursor; the longer pauses are where the viewer reads a screen.
# tools/dive-demo-captions.py, which the profile runs after recording, shortens
# any pause longer than 4s to 4s.
#
# Each `# @caption` line sits just before the key press that opens the screen it
# describes. The caption bar below the player shows it from that moment until
# the next one, and it marks a chapter on the player's progress bar. Captions are user-facing documentation: they follow
# the prose rules in CLAUDE.md, and docs-prose-check.py does not scan them.

# Overview
# @caption Overview: facts about the whole file, and the screens it drills into.
Sleep 2500ms

# Footer & indexes
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Footer & indexes: file layout, encoding and codec counts, page-index coverage and byte totals.
Enter
Sleep 4000ms
# @caption Overview: facts about the whole file, and the screens it drills into.
Escape
Sleep 1200ms

# Schema: expand `websites` down to its leaf, then expand everything
Up
Sleep 400ms
Up
Sleep 900ms
# @caption Schema: the column tree. → expands a group, e expands all of them.
Enter
Sleep 1800ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
Right
Sleep 800ms
Down
Sleep 400ms
Right
Sleep 800ms
Down
Sleep 1500ms
Type "e"
Sleep 4000ms
# @caption Overview: facts about the whole file, and the screens it drills into.
Escape
Sleep 1200ms

# Row groups -> row group #5 -> Column chunks -> websites.list.element
Down
Sleep 900ms
# @caption Row groups: one row per row group, with its row count, sizes and compression.
Enter
Sleep 3000ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Row group detail: one row group's facts, and a menu into its column chunks and indexes.
Enter
Sleep 3000ms
# @caption Column chunks: one row per column of the row group, with its type, codec, size and dictionary flag.
Enter
Sleep 3000ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Column chunk detail: storage, encoding and statistics of one chunk, and a menu into its pages, indexes and dictionary.
Enter
Sleep 4000ms

# Pages -> first data page's header
# @caption Pages: the chunk's dictionary page and data pages. Enter opens a page's full header.
Enter
Sleep 3000ms
Down
Sleep 900ms
Enter
Sleep 4000ms
Escape
Sleep 800ms
Escape
Sleep 1200ms

# Dictionary (the cursor stops on the Column index and Offset index entries on
# its way down the drill menu)
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Dictionary: the chunk's distinct values, numbered as the data pages reference them. / filters them.
Enter
Sleep 4000ms

# `o` back to Overview -> Data preview -> 5th row
# @caption Overview: facts about the whole file, and the screens it drills into.
Type "o"
Sleep 1800ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Data preview: decoded rows. The # column is each row's position in the file, counted from 0.
Enter
Sleep 3000ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 400ms
Down
Sleep 900ms
# @caption Enter on a row opens every field of that row, nested values included.
Enter
Sleep 4000ms
Escape
Sleep 800ms
Escape
Sleep 1500ms

Type "q"
