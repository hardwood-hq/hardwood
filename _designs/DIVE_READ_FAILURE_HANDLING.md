# Design: read-failure handling in dive

**Status: Implemented.** Tracking issue: #1092. Supersedes #342.

## Goal

A reader who opens a damaged file in `hardwood dive` should be told what failed
and where in the file it failed, and should still have a session afterwards.

Every dive screen that reads from the file does so inside the render callback or
the key handler, and neither is guarded. An unreadable region anywhere ends the
session: the exception escapes, TamboUI paints its crash screen over a buffer it
does not clear, and `Esc` exits the process because there is no longer a loop to
press it in. Which screen dies depends only on which region is damaged.

## Why the guard is central and not per screen

Six screens reach the file from a render or keybar path: `PagesScreen`,
`ColumnIndexScreen`, `OffsetIndexScreen`, `ColumnAcrossRowGroupsScreen`,
`DictionaryScreen` and `DataPreviewScreen`. A per-screen error state solves one
of them and leaves the shape of the bug in the other five, and every future
screen has to remember to opt in. A guard in `DiveApp` covers all of them and
cannot be forgotten.

No screen gains an error state. `ScreenState` stays a description of what a
screen is showing and the handlers stay pure functions of
`(event, model, stack)`, which is what makes them testable without a terminal.

## The model

`DiveApp` holds a nullable read failure — the message, and the scroll offset
into it.

- **Key dispatch** is guarded. A read failure while handling a key records the
  failure; the navigation stack is left as it was.
- **The guard catches `RuntimeException`**, not a list of types. Which one a
  damaged file raises is decided by the decoder that trips over it — a
  dictionary index past its dictionary is an `ArrayIndexOutOfBoundsException`,
  an impossible RLE run header an `IllegalStateException`, a length that will
  not fit an `ArithmeticException` — and the four regions below are parsed
  outside the read pipeline, so nothing narrows that set to
  `ParquetReadException` on the way here. An enumeration would have to be
  revisited every time a decoder learns a new way to fail, and the promise this
  guard exists to make is that no file ends the session. The price is that a
  defect of ours in a render path is reported as a read failure.
- **Render** is guarded, around both `keybarForActive()` and `renderBody()`. A
  failure records itself and paints the overlay in the same frame, over a body
  cleared first: a half-drawn screen under the overlay is what makes a reported
  error look like a corrupted display.
- **While a failure is showing**, keys still reach the screen underneath. This
  is what lets a reader move away from a damaged region rather than only back
  out of it: the stack still holds the last state that rendered, so `PgUp` from
  a failed Data preview page loads an earlier window and clears the failure. A
  key that fails again replaces the failure with the new one.
- **`Esc` is taken from the screen** while a failure shows. A screen normally
  gets first refusal on it so it can claim it for something of its own, but a
  screen that cannot read cannot honour that, and its handler reads too. At the
  root there is nothing to pop and `Esc` dismisses the failure alone; `o`, which
  collapses to the root, clears it on the way. Neither leaves an overlay
  standing over a screen that reads.
- **The overlay scrolls before the screen moves.** While there is more message
  than box the navigation keys address the overlay, and reach the screen again
  once it is at its end.
- **The overlay** is a `ScrollPane` modal, per the navigation model: the
  navigation keys scroll it while there is more message than box.

## What the message says

The overlay adds no wording of its own. It shows the exception's message, which
the reader has already placed — `[data.parquet: row group 0, column 'id']` — per
[EXCEPTION_MODEL.md](EXCEPTION_MODEL.md).

The four regions a dive screen parses for itself (column index, offset index,
page headers, dictionary page) bypass the read pipeline and go at the bytes
directly, so `ParquetModel` places those failures itself. The Thrift structure
that would not parse names itself in the message the reader raises.

Placing a failure restates it, and restating it must not change what it is. A
`ParquetReadException` leaves `ExceptionContext` as one, through the subclass's
own `(String, Throwable)` constructor where there is one and through the base
type where there is not — otherwise the callers that catch that type to report a
broken file cleanly, `hardwood inspect pages` among them, stop seeing it.

## What it does not do

TamboUI's own crash screen paints over an un-cleared buffer. That is upstream of
this project; this design stops dive reaching that screen, it does not fix it.

Only failures the reader **detects** are reported, which is a much smaller set
than "corrupt files". `PLAIN` has no redundancy to check, and an RLE run header
is a varint whose every bit pattern is a syntactically valid header, so most
corruption of page data decodes to wrong values rather than to an error.
Per-page CRCs would catch it and the reader validates them, but only where the
writer emitted one. That gap is #1095.

How the non-interactive commands report a failure is #1094.

## Validation

`DiveReadFailureTest` clobbers a page header, a column index, an offset index, a
dictionary page header, a dictionary page *body* and a row of data in turn, each
at an offset derived from the file's own metadata, and drives the screen that
reads it: the session survives, the overlay names the file and the structure, and
`Esc` leaves. Which screen a damaged file takes down depends on which region is
damaged, so one region proves nothing about the rest.

Truncation is covered separately from malformation. Overwriting bytes in place
tends to raise `ParquetReadException`; a field declaring more bytes than its
buffer holds raises `ThriftTruncatedException`, which is the shape that tells
whether restating a failure has kept its type.
