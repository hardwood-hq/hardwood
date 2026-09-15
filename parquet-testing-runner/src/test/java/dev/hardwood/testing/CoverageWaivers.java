/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.column.Encoding;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;

/// The cells no test produces, each with the reason it does not.
///
/// A waiver is a claim about the world, so it is checked in both directions: a waived cell that
/// is nonetheless observed fails the verdict just as an unwaived gap does. Without that a waiver
/// outlives what justified it — a parquet-java release that gained the `BROTLI` codec would
/// leave those cells waived by inertia, which is what the existing
/// `WriterInteropTest.parquetJavaHasNoBrotliCodec` exists to prevent for one codec and what this
/// generalizes to every cell.
final class CoverageWaivers {

    private CoverageWaivers() {
    }

    /// One waived cell and why it is waived.
    ///
    /// @param cell the cell, as [Coverage] spells it
    /// @param reason why no test produces it, and where the capability is covered instead
    record Waiver(String cell, String reason) {
    }

    /// Every waived cell, keyed by the cell itself.
    static Map<String, String> waivers() {
        Map<String, String> byCell = new LinkedHashMap<>();
        for (Waiver waiver : all()) {
            byCell.put(waiver.cell(), waiver.reason());
        }
        return byCell;
    }

    private static List<Waiver> all() {
        List<Waiver> waivers = new ArrayList<>();
        brotli(waivers);
        fixedTimestamps(waivers);
        return waivers;
    }

    /// A `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` reaches no cell of this module. The carrier
    /// comes from parquet-format after its 2.14.0 release, and no parquet-java release up to 1.18.1
    /// accepts the pairing: the pinned one refuses it while parsing the footer, so it opens no file
    /// holding one.
    ///
    /// The writer's output is covered by core's `WriterFlba12TimestampTest`, and compared with the
    /// bytes and bounds a parquet-java development build wrote into `flba12_timestamp.parquet` by
    /// `Flba12TimestampTest`, whose `parquetJavaRejectsTheAnnotation` pins the reason so that a
    /// parquet-java that reads the carrier fails there and these waivers come off.
    private static void fixedTimestamps(List<Waiver> waivers) {
        for (CoverageDomain.Annotation annotation : CoverageDomain.annotations()) {
            if (CoverageDomain.readByParquetJava(annotation)) {
                continue;
            }
            for (Coverage.StorageForm form : CoverageDomain.storageForms(annotation)) {
                waivers.add(new Waiver(Coverage.annotationStorage(annotation.key(), annotation.carrierKey(), form),
                        "no parquet-java release up to 1.18.1 reads TIMESTAMP on FIXED_LEN_BYTE_ARRAY(12); covered by"
                                + " WriterFlba12TimestampTest and Flba12TimestampTest, with the reason pinned by"
                                + " Flba12TimestampTest.parquetJavaRejectsTheAnnotation"));
            }
        }
    }

    /// `BROTLI` reaches no cell of this module. parquet-java resolves a codec by name through
    /// Hadoop's registry, and for `BROTLI` that name is `org.apache.hadoop.io.compress.BrotliCodec`
    /// — a class in neither parquet-java nor Hadoop, but in an unmaintained third-party artifact
    /// whose native binaries cover a few platforms only. Putting it on this module's classpath
    /// would make the gate's result depend on the architecture it runs on.
    ///
    /// The codec is covered against DuckDB by `WriterDifferentialTest` instead, and
    /// `WriterInteropTest.parquetJavaHasNoBrotliCodec` pins the reason so that a parquet-java
    /// that gains the codec fails there and these waivers come off.
    private static void brotli(List<Waiver> waivers) {
        Set<Encoding> producible = EnumSet.noneOf(Encoding.class);
        for (PhysicalType type : CoverageDomain.writableTypes()) {
            producible.addAll(CoverageDomain.pageEncodings(type));
        }
        for (Encoding encoding : producible) {
            waivers.add(new Waiver(Coverage.encodingCodec(encoding, CompressionCodec.BROTLI),
                    "parquet-java cannot resolve the BROTLI codec class; covered against DuckDB by"
                            + " WriterDifferentialTest, with the reason pinned by"
                            + " WriterInteropTest.parquetJavaHasNoBrotliCodec"));
        }
    }
}
