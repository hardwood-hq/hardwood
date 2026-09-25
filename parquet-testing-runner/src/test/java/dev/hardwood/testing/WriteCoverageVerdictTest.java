/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.testing.Coverage.Projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/// The verdict of the write-path coverage assertion described in
/// `_designs-legacy/WRITE_COVERAGE_ASSERTION.md`: every cell the writer can produce was produced by some
/// test, or is waived with a reason.
///
/// This runs in a Surefire execution of its own, after the one that produces the files, because
/// what it asserts spans every test class and outlives the JVM they ran in. Its input is what
/// [WriteCoverageListener] flushed — merged across forks, and joined with this JVM's own registry
/// so that running the whole module inside an IDE reaches the same conclusion.
class WriteCoverageVerdictTest {

    /// Where the human-readable breakdown is written, outside the directory the observations
    /// live in so that it is not mistaken for one of them.
    private static final Path REPORT = Paths.get("target", "write-coverage-report.txt");

    private static Set<String> required;
    private static Set<String> observed;
    private static Map<String, String> waived;

    @BeforeAll
    static void collect() throws IOException {
        required = CoverageDomain.required();
        observed = readObservations();
        waived = CoverageWaivers.waivers();
        writeReport();
    }

    /// The observations exist at all. An empty set would make every cell a gap, which is the
    /// right verdict but the wrong explanation: it means the execution that produces the files
    /// did not run, not that the writer is untested.
    @Test
    void theRunProducedObservations() {
        assertThat(observed)
                .as("no write-path coverage was recorded — did the recording Surefire execution run?")
                .isNotEmpty();
    }

    /// Every cell the writer can produce was produced.
    @Test
    void everyProducibleCellIsCovered() {
        Set<String> gaps = new TreeSet<>(required);
        gaps.removeAll(observed);
        gaps.removeAll(waived.keySet());
        if (gaps.isEmpty()) {
            return;
        }
        fail("""
                %d of %d producible cells were produced by no test.

                Each is a shape the writer can emit and nothing reads back. Cover it with a test, \
                or waive it in CoverageWaivers with the reason it cannot be covered.

                %s""".formatted(gaps.size(), required.size(), byProjection(gaps)));
    }

    /// No waiver outlives what justified it. A waived cell that some test did produce means the
    /// obstacle is gone, and the waiver has to come off rather than stay as a claim the run
    /// disproves.
    @Test
    void noWaiverOutlivesItsReason() {
        Set<String> stale = new TreeSet<>(waived.keySet());
        stale.retainAll(observed);
        if (stale.isEmpty()) {
            return;
        }
        List<String> lines = new ArrayList<>();
        for (String cell : stale) {
            lines.add("  " + cell + "\n      waived as: " + waived.get(cell));
        }
        fail("""
                A waiver claims a cell no test can produce. %d were produced anyway, so those \
                waivers no longer describe the world.

                Remove them from CoverageWaivers.

                %s""".formatted(stale.size(), String.join("\n", lines)));
    }

    /// Every member of the sealed [LogicalType] hierarchy is either required by the domain or
    /// refused by the writer.
    ///
    /// This is the claim the design rests on — that a member added to `LogicalType` extends the
    /// writer and fails the verdict in the same commit — and without it the claim is not true of
    /// this module: `CoverageDomain.annotations()` is a hand-written list, and a new member would
    /// force a spelling in [LogicalTypeKey] and a classification in core's
    /// `WriterAnnotationRangeTest` while producing no requirement at all here.
    ///
    /// The refused side is checked against the writer rather than asserted: a release that starts
    /// writing `VARIANT` fails here, rather than leaving it excluded by a stale list. The refusal
    /// is held to the reason it gives, not merely to its having happened — a group annotation on a
    /// primitive column is refused whatever the writer supports of it, so a probe satisfied by any
    /// exception would be satisfied by `LIST` and `MAP` too, and would stay green on exactly the
    /// day this exclusion has to end.
    @Test
    void everyAnnotationIsRequiredOrRefusedByTheWriter() {
        Set<Class<?>> accountedFor = new HashSet<>();
        for (CoverageDomain.Annotation annotation : CoverageDomain.annotations()) {
            accountedFor.add(annotation.logicalType().getClass());
        }
        for (LogicalType group : CoverageDomain.groupAnnotations()) {
            accountedFor.add(group.getClass());
        }
        for (CoverageDomain.Refusal refused : CoverageDomain.refusedAnnotations()) {
            RuntimeException refusal = CoverageDomain.refusalOf(
                    refused.logicalType(), PhysicalType.BYTE_ARRAY, null);

            assertThat(refusal).as("the writer still refuses %s", refused.logicalType()).isNotNull();
            assertThat(refusal.getMessage())
                    .as("the refusal of %s names the annotation, not the shape it was declared in",
                            refused.logicalType())
                    .contains(refused.reason());
            accountedFor.add(refused.logicalType().getClass());
        }

        assertThat(LogicalType.class.getPermittedSubclasses())
                .as("every LogicalType is required by the domain or refused by the writer")
                .allSatisfy(member -> assertThat(accountedFor).contains(member));
    }

    /// Every cell of every observation belongs to a projection the domain knows, so a cell
    /// spelled one way by the registry and another by the domain shows up here rather than as a
    /// gap whose cause is hidden one level down.
    @Test
    void everyObservationSpellsAKnownProjection() {
        Set<String> unknown = new TreeSet<>();
        for (String cell : observed) {
            try {
                Coverage.projectionOf(cell);
            }
            catch (RuntimeException e) {
                unknown.add(cell);
            }
        }
        assertThat(unknown).as("observations naming no known projection").isEmpty();
    }

    /// The cells recorded by this run: whatever the recording execution flushed, plus anything
    /// this JVM recorded itself.
    private static Set<String> readObservations() throws IOException {
        Set<String> cells = new HashSet<>(CoverageRegistry.cells());
        if (!Files.isDirectory(CoverageRegistry.OUTPUT_DIRECTORY)) {
            return cells;
        }
        try (Stream<Path> files = Files.list(CoverageRegistry.OUTPUT_DIRECTORY)) {
            for (Path file : files.toList()) {
                cells.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            }
        }
        cells.remove("");
        return cells;
    }

    /// The gaps, grouped under the projection each belongs to, so that a failure reads as which
    /// question is unanswered rather than as a list of strings.
    private static String byProjection(Set<String> cells) {
        Map<Projection, List<String>> grouped = new TreeMap<>();
        for (String cell : cells) {
            grouped.computeIfAbsent(Coverage.projectionOf(cell), key -> new ArrayList<>()).add(cell);
        }
        StringBuilder message = new StringBuilder();
        grouped.forEach((projection, members) -> {
            message.append(projection).append(" (").append(members.size()).append(")\n");
            for (String cell : members) {
                message.append("  ").append(cell.substring(cell.indexOf('|') + 1)).append('\n');
            }
        });
        return message.toString();
    }

    /// Writes what every projection required and how much of it was reached, which is what a
    /// burndown is read off.
    private static void writeReport() throws IOException {
        Map<Projection, int[]> counts = new TreeMap<>();
        for (String cell : required) {
            int[] tally = counts.computeIfAbsent(Coverage.projectionOf(cell), key -> new int[2]);
            tally[0]++;
            if (observed.contains(cell)) {
                tally[1]++;
            }
        }
        List<String> lines = new ArrayList<>();
        lines.add("Write-path coverage, " + observed.size() + " cells observed");
        lines.add("");
        counts.forEach((projection, tally) ->
                lines.add("%-24s %4d / %4d covered".formatted(projection, tally[1], tally[0])));
        lines.add("");
        lines.add("Uncovered and unwaived:");
        Set<String> gaps = new TreeSet<>(required);
        gaps.removeAll(observed);
        gaps.removeAll(waived.keySet());
        gaps.forEach(cell -> lines.add("  " + cell));
        Files.createDirectories(REPORT.getParent());
        Files.write(REPORT, lines, StandardCharsets.UTF_8);
    }
}
