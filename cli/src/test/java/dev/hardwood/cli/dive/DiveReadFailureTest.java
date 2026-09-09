/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.ToLongFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.cli.dive.internal.Keys;
import dev.hardwood.metadata.ColumnMetaData;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.buffer.Cell;
import dev.tamboui.layout.Rect;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/// A file that will not give up what a screen asked for must not take the
/// session with it.
///
/// Every dive screen that reads does so inside the render callback or the key
/// handler, so an escaping exception leaves no event loop to press `Esc` in —
/// the terminal is left showing a stack trace over whatever was on screen.
class DiveReadFailureTest {

    @TempDir
    private Path tempDir;

    private ParquetModel model;
    private DiveApp app;

    @BeforeEach
    void resetGeometry() {
        Keys.resetObservedGeometry();
    }

    /// Opens a copy of the fixture with `count` bytes overwritten at the offset
    /// `region` picks out of the file's own metadata.
    ///
    /// Derived rather than hardcoded: an offset written into the test is right
    /// only for the fixture it was read off, and stops being checked the moment
    /// either changes.
    private void damage(ToLongFunction<ParquetModel> region, int count) throws Exception {
        Path source = Path.of(getClass().getResource("/column_index_pushdown.parquet").getPath());
        byte[] bytes = Files.readAllBytes(source);

        long at;
        try (ParquetModel intact = ParquetModel.open(InputFile.of(source), source.toString())) {
            at = region.applyAsLong(intact);
        }
        for (int i = 0; i < count; i++) {
            bytes[(int) at + i] = (byte) 0xff;
        }

        Path damaged = tempDir.resolve("damaged.parquet");
        Files.write(damaged, bytes);
        model = ParquetModel.open(InputFile.of(damaged), "damaged.parquet");
        app = new DiveApp(model);
    }

    private void damageFirstDataPageHeader() throws Exception {
        damage(m -> m.chunk(0, 0).metaData().dataPageOffset(), 24);
    }

    @AfterEach
    void closeModel() throws Exception {
        model.close();
    }

    @Test
    void aScreenThatCannotReadRendersAnOverlayInsteadOfEndingTheSession() throws Exception {
        damageFirstDataPageHeader();
        toPagesScreen();

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();

        String frame = frameText();
        assertThat(frame).contains("Read failed");
        // The reader's own message, which by now names where in the file to look.
        assertThat(frame).contains("damaged.parquet");
        assertThat(frame).contains("PageHeader");
    }

    /// The overlay is not a dead end: the keys still reach the screen under it,
    /// so `Esc` leaves the way it always did.
    @Test
    void escapeLeavesTheFailedScreen() throws Exception {
        damageFirstDataPageHeader();
        toPagesScreen();
        app.renderOnce(buffer());

        DiveApp.Action back = app.dispatchKey(new KeyEvent(KeyCode.ESCAPE, KeyModifiers.NONE, '\0'));

        assertThat(back).isEqualTo(DiveApp.Action.HANDLED);
        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).doesNotContain("Read failed");
    }

    /// Pages is the screen that has to walk page headers, so it is the one the
    /// damaged header stops. Pushed rather than navigated to: how a reader gets
    /// there is the navigation tests' subject, not this one's.
    private void toPagesScreen() {
        app.stack().push(new ScreenState.Pages(0, 0, 0, false, true, 0));
    }

    private Buffer lastBuffer;

    private Buffer buffer() {
        lastBuffer = Buffer.empty(new Rect(0, 0, 90, 24));
        return lastBuffer;
    }

    private String frameText() {
        Rect area = new Rect(0, 0, 90, 24);
        StringBuilder sb = new StringBuilder();
        for (int y = 0; y < area.height(); y++) {
            for (int x = 0; x < area.width(); x++) {
                Cell c = lastBuffer.get(x, y);
                String sym = c.symbol();
                sb.append(sym == null || sym.isEmpty() ? " " : sym);
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    /// Every region a screen reads for itself, not just the one the first
    /// version happened to break. Which screen dies depends on which region is
    /// damaged, so covering one proves nothing about the rest.
    @Test
    void aDamagedColumnIndexIsReportedRatherThanFatal() throws Exception {
        damage(m -> m.chunk(0, 0).columnIndexOffset(), 24);
        app.stack().push(new ScreenState.ColumnIndexView(0, 0, 0, "", false, true, false));

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed", "damaged.parquet", "ColumnIndex");
    }

    /// The screen that reads a column's offset index once per row group, so the
    /// failure comes out of a loop rather than out of a single read.
    @Test
    void aDamagedOffsetIndexAcrossRowGroupsIsReportedRatherThanFatal() throws Exception {
        damage(m -> m.chunk(0, 0).offsetIndexOffset(), 24);
        app.stack().push(new ScreenState.ColumnAcrossRowGroups(0, 0, false));

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed", "damaged.parquet");
    }

    /// Data preview reads through the row pipeline rather than at the bytes, and
    /// it is entered by a key rather than by a render — so this is the guard on
    /// the dispatch side, on the one screen whose page is fitted inside the
    /// render guard as well.
    @Test
    void aDamagedDataPageIsReportedRatherThanFatalOnDataPreview() throws Exception {
        damageFirstDataPageHeader();
        app.renderOnce(buffer());
        for (int i = 0; i < MENU_ROWS_TO_DATA_PREVIEW; i++) {
            app.dispatchKey(new KeyEvent(KeyCode.DOWN, KeyModifiers.NONE, '\0'));
        }

        DiveApp.Action enter = app.dispatchKey(new KeyEvent(KeyCode.ENTER, KeyModifiers.NONE, '\0'));

        assertThat(enter).isEqualTo(DiveApp.Action.HANDLED);
        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed", "damaged.parquet");
    }

    /// `o` collapses to Overview, which reads nothing that could fail — so it
    /// has to take the failure with it. Left behind, it paints an error box over
    /// a screen that reads, and `Esc` at the root has no frame to pop.
    @Test
    void collapsingToTheRootLeavesNoFailureBehind() throws Exception {
        damageFirstDataPageHeader();
        toPagesScreen();
        app.renderOnce(buffer());
        assertThat(frameText()).contains("Read failed");

        app.dispatchKey(new KeyEvent(KeyCode.CHAR, KeyModifiers.NONE, 'o'));

        app.renderOnce(buffer());
        assertThat(frameText()).doesNotContain("Read failed");
    }

    /// A failure the root screen is left holding still has to be dismissable.
    /// Esc dismisses the overlay with no frame to pop.
    @Test
    void escapeDismissesAFailureAtTheRoot() throws Exception {
        damageFirstDataPageHeader();
        toPagesScreen();
        app.renderOnce(buffer());
        app.dispatchKey(new KeyEvent(KeyCode.ESCAPE, KeyModifiers.NONE, '\0'));
        assertThat(app.stack().depth()).isEqualTo(1);

        // Put the root back into a failure the way a carried-over one would look.
        app.stack().push(new ScreenState.Pages(0, 0, 0, false, true, 0));
        app.renderOnce(buffer());
        app.stack().pop();
        assertThat(frameText()).contains("Read failed");

        DiveApp.Action esc = app.dispatchKey(new KeyEvent(KeyCode.ESCAPE, KeyModifiers.NONE, '\0'));

        assertThat(esc).isEqualTo(DiveApp.Action.HANDLED);
        app.renderOnce(buffer());
        assertThat(frameText()).doesNotContain("Read failed");
    }

    /// Overview's menu cursor starts on the first entry; Data preview is the last.
    private static final int MENU_ROWS_TO_DATA_PREVIEW = 3;

    @Test
    void aDamagedOffsetIndexIsReportedRatherThanFatal() throws Exception {
        damage(m -> m.chunk(0, 0).offsetIndexOffset(), 24);
        app.stack().push(new ScreenState.OffsetIndexView(0, 0, 0));

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed", "damaged.parquet", "OffsetIndex");
    }

    @Test
    void aDamagedDictionaryPageIsReportedRatherThanFatal() throws Exception {
        damage(m -> {
            Long dict = m.chunk(0, 0).metaData().dictionaryPageOffset();
            return dict != null && dict > 0 ? dict : m.chunk(0, 0).metaData().dataPageOffset();
        }, 24);
        app.stack().push(new ScreenState.DictionaryView(0, 0, 0, false, "", false, true, true));

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed");
    }

    /// A decode failure in a page *body* rather than a header. The failure comes
    /// out of the decoders as an array index or a state check rather than as a
    /// parse error, which is a different set of types for the guard to hold.
    @Test
    void aDamagedPageBodyIsReportedRatherThanFatal() throws Exception {
        Path source = Path.of(getClass().getResource("/column_index_pushdown_dict.parquet").getPath());
        byte[] bytes = Files.readAllBytes(source);
        long bodyStart;
        try (ParquetModel intact = ParquetModel.open(InputFile.of(source), source.toString())) {
            ColumnMetaData meta = intact.chunk(0, 1).metaData();
            Long dict = meta.dictionaryPageOffset();
            // Past the header, into the body it describes.
            bodyStart = (dict != null && dict > 0 ? dict : meta.dataPageOffset()) + 40;
        }
        for (int i = 0; i < 24; i++) {
            bytes[(int) bodyStart + i] = (byte) 0xff;
        }
        Path damaged = tempDir.resolve("body.parquet");
        Files.write(damaged, bytes);

        model = ParquetModel.open(InputFile.of(damaged), "body.parquet");
        app = new DiveApp(model);
        app.stack().push(new ScreenState.DictionaryView(0, 1, 0, false, "", false, true, true));

        assertThatCode(() -> app.renderOnce(buffer())).doesNotThrowAnyException();
        assertThat(frameText()).contains("Read failed");
    }
}
