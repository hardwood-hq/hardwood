/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// The read path's packages depend only on the packages before them in [#LAYERS], the order the
/// "Read Path Packages" table in `ARCHITECTURE.md` lists them in. Code references count, doc links
/// do not.
class ReadPathLayersTest {

    private static final List<String> LAYERS = List.of(
            "internal.io",
            "internal.reader.select",
            "internal.reader.step",
            "internal.reader.batch",
            "internal.reader.memory",
            "internal.reader.decode",
            "internal.reader.fetch",
            "internal.reader.footer",
            "internal.reader.plan",
            "internal.reader.pipeline",
            "internal.reader.engine",
            "internal.reader.row");

    private static final Path SOURCES = Path.of("src/main/java/dev/hardwood");

    private static final Pattern REFERENCE = Pattern.compile("dev\\.hardwood\\.(internal\\.(?:io|reader\\.\\w+))\\.[A-Z]");

    private static final Pattern COMMENT = Pattern.compile("//[^\\n]*|/\\*.*?\\*/", Pattern.DOTALL);

    @Test
    void noPackageReferencesALaterLayer() throws IOException {
        List<String> violations = new ArrayList<>();
        for (int layer = 0; layer < LAYERS.size(); layer++) {
            Path dir = SOURCES.resolve(LAYERS.get(layer).replace('.', '/'));
            assertThat(dir).isDirectory();
            try (Stream<Path> files = Files.list(dir)) {
                for (Path file : files.filter(f -> f.toString().endsWith(".java")).sorted().toList()) {
                    String code = COMMENT.matcher(Files.readString(file)).replaceAll("");
                    Matcher reference = REFERENCE.matcher(code);
                    while (reference.find()) {
                        int target = LAYERS.indexOf(reference.group(1));
                        if (target > layer) {
                            violations.add(LAYERS.get(layer) + "." + file.getFileName() + " -> " + reference.group(1));
                        }
                    }
                }
            }
        }
        assertThat(violations).isEmpty();
    }
}
