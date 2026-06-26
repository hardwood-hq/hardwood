/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies that Hardwood's Variant decoder gracefully rejects the deliberately
/// malformed fixtures under `bad_data/variants/` (apache/parquet-testing#113).
///
/// Those fixtures do not carry a Parquet `VARIANT` logical-type annotation — they
/// store the variant `metadata` and `value` as two plain `required binary`
/// columns, because they originate from parquet-java's variant *binary decoder*
/// hardening tests. The unit under test is therefore Hardwood's variant decoder,
/// not the file-read path: this test reads the two byte buffers and feeds them
/// into [PqVariantImpl], then walks the entire value tree to force every lazy
/// decode. A robust decoder must throw rather than over-allocate, run off the end
/// of a buffer, or recurse without bound.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VariantHardeningTest {

    /// Fixtures Hardwood does **not** yet reject. `over_deep_nested_children`
    /// nests variant children ~500 levels deep; Hardwood has no recursion-depth
    /// guard and decodes it without complaint, whereas parquet-java / Spark cap
    /// the depth. This is a tripwire: once a depth limit is added, the file moves
    /// out of this set and into the rejected group.
    private static final Set<String> KNOWN_NOT_REJECTED = Set.of(
            "over_deep_nested_children.parquet");

    private Path variantsDir;

    @BeforeAll
    void setUp() throws IOException {
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        variantsDir = repoDir.resolve("bad_data/variants");
    }

    @TestFactory
    List<DynamicTest> eachMalformedVariantIsRejected() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(variantsDir, "*.parquet")) {
            stream.forEach(files::add);
        }
        Collections.sort(files);
        for (Path file : files) {
            String name = file.getFileName().toString();
            if (KNOWN_NOT_REJECTED.contains(name)) {
                tests.add(DynamicTest.dynamicTest(name + " [known gap: accepted]",
                        () -> assertCurrentlyAccepted(file)));
            }
            else {
                tests.add(DynamicTest.dynamicTest(name, () -> assertRejected(file)));
            }
        }
        return tests;
    }

    private void assertRejected(Path file) {
        assertThatThrownBy(() -> decodeAndWalk(file))
                .as("Hardwood should gracefully reject malformed variant %s", file.getFileName())
                .isInstanceOf(RuntimeException.class);
    }

    /// Documents the current (undesired) behaviour so the suite stays green while
    /// the depth-guard gap is open. Flips to a failure the moment a guard is added.
    private void assertCurrentlyAccepted(Path file) throws IOException {
        int rows = decodeAndWalk(file);
        assertThat(rows)
                .as("%s is expected to be (wrongly) accepted until a depth guard exists",
                        file.getFileName())
                .isGreaterThan(0);
    }

    /// Reads the `metadata`/`value` binary columns, decodes each row as a Variant,
    /// and recursively materialises the whole tree. Returns the row count.
    private int decodeAndWalk(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rr = reader.rowReader()) {
            int rows = 0;
            while (rr.hasNext()) {
                rr.next();
                rows++;
                if (rr.isNull("metadata") || rr.isNull("value")) {
                    continue;
                }
                byte[] metadata = rr.getBinary("metadata");
                byte[] value = rr.getBinary("value");
                walk(new PqVariantImpl(metadata, value));
            }
            return rows;
        }
    }

    private void walk(PqVariant v) {
        VariantType type = v.type();
        switch (type) {
            case OBJECT -> {
                PqVariantObject obj = v.asObject();
                int n = obj.getFieldCount();
                for (int i = 0; i < n; i++) {
                    String field = obj.getFieldName(i);
                    if (!obj.isNull(field)) {
                        PqVariant child = obj.getVariant(field);
                        if (child != null) {
                            walk(child);
                        }
                    }
                }
            }
            case ARRAY -> {
                PqVariantArray arr = v.asArray();
                int n = arr.size();
                for (int i = 0; i < n; i++) {
                    PqVariant element = arr.get(i);
                    if (element != null) {
                        walk(element);
                    }
                }
            }
            // Force full materialisation of the leaf payload and its byte extent.
            default -> {
                v.value();
                v.toString();
            }
        }
    }
}
