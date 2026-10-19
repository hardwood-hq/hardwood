/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.internal.encoding.simd.VectorSupport;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks that reads which log nothing leave the JDK's `System.LoggerFinder` lookup undone.
/// The lookup searches every jar on the class path for a finder, which costs tens of
/// milliseconds on a large class path. The reads run in a fresh JVM whose class path names
/// [RecordingLoggerFinder] as its only finder, so the finder's construction marks the lookup.
///
/// The child JVM loads core's multi-release overlays, from the packaged JAR or, when core's
/// classes come from a directory, from its `META-INF/versions` directories placed ahead of it,
/// and resolves the Vector API module whenever this JVM does. That covers the overlays'
/// classes, and pins that SIMD engages in a JVM that has the module.
class LoggerLookupTest {

    private static final String VECTOR_MODULE = "jdk.incubator.vector";

    @Test
    void readsThatLogNothingSkipTheLoggerFinderLookup(@TempDir Path workDir) throws Exception {
        Path services = workDir.resolve("services");
        Path serviceFile = services.resolve("META-INF/services/java.lang.System$LoggerFinder");
        Files.createDirectories(serviceFile.getParent());
        Files.writeString(serviceFile, RecordingLoggerFinder.class.getName());

        List<String> classPath = new ArrayList<>();
        classPath.add(services.toString());
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            // log4j-jpl, the test logging binding, is a finder too; two finders fail the lookup.
            if (!entry.contains("log4j-jpl")) {
                classPath.addAll(versionOverlays(Path.of(entry)));
                classPath.add(entry);
            }
        }
        boolean vectorModule = ModuleLayer.boot().findModule(VECTOR_MODULE).isPresent();

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--enable-native-access=ALL-UNNAMED");
        if (vectorModule) {
            command.add("--add-modules");
            command.add(VECTOR_MODULE);
        }
        command.addAll(List.of("-cp", String.join(File.pathSeparator, classPath), Reads.class.getName()));

        Path stdout = workDir.resolve("stdout.txt");
        Path stderr = workDir.resolve("stderr.txt");
        Process process = new ProcessBuilder(command)
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile())
                .start();
        boolean exited = process.waitFor(2, TimeUnit.MINUTES);
        if (!exited) {
            process.destroyForcibly().waitFor();
        }
        String output = Files.readString(stdout) + Files.readString(stderr);

        assertThat(exited).as("the child JVM exited within two minutes; its output:%n%s", output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();
        // The last line shows that the finder is picked up once something obtains a logger.
        assertThat(Files.readAllLines(stdout)).as(output).isEqualTo(List.of(
                "simd: " + vectorModule,
                "lookup after the reads: false",
                "lookup after getLogger: true"));
    }

    /// The `META-INF/versions/<n>` directories under a class directory, for every `n` this
    /// runtime applies, highest first, as a multi-release JAR would resolve them.
    private static List<String> versionOverlays(Path entry) throws IOException {
        Path versions = entry.resolve("META-INF/versions");
        if (!Files.isDirectory(versions)) {
            return List.of();
        }
        int feature = Runtime.version().feature();
        try (Stream<Path> dirs = Files.list(versions)) {
            return dirs.filter(dir -> dir.getFileName().toString().matches("\\d+"))
                    .filter(dir -> Integer.parseInt(dir.getFileName().toString()) <= feature)
                    .sorted(Comparator.comparingInt((Path dir) -> Integer.parseInt(dir.getFileName().toString()))
                            .reversed())
                    .map(Path::toString)
                    .toList();
        }
    }

    /// Runs a row read of a nested file and a column read of a flat one, then reports whether
    /// SIMD is in use and whether the finder was looked up, before and after obtaining a logger.
    public static final class Reads {

        public static void main(String[] args) throws Exception {
            try (Hardwood hardwood = Hardwood.create()) {
                try (ParquetFileReader parquet = hardwood.open(
                        InputFile.of(Path.of("src/test/resources/dict_nested_repeats.parquet")));
                     RowReader rows = parquet.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                    }
                }
                try (ParquetFileReader parquet = hardwood.open(
                        InputFile.of(Path.of("src/test/resources/dict_compressed_page.parquet")));
                     ColumnReaders columns = parquet.columnReaders(ColumnProjection.all())) {
                    while (columns.nextBatch()) {
                        columns.getColumnReader("payload").getLongs();
                    }
                }
            }
            System.out.println("simd: " + VectorSupport.isAvailable());
            System.out.println("lookup after the reads: " + RecordingLoggerFinder.created);
            System.getLogger(Reads.class.getName());
            System.out.println("lookup after getLogger: " + RecordingLoggerFinder.created);
        }
    }

    /// A finder that records its construction and discards every message.
    public static final class RecordingLoggerFinder extends System.LoggerFinder {

        static volatile boolean created;

        public RecordingLoggerFinder() {
            created = true;
        }

        @Override
        public System.Logger getLogger(String name, Module module) {
            return new DiscardingLogger(name);
        }
    }

    private record DiscardingLogger(String name) implements System.Logger {

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        }
    }
}
