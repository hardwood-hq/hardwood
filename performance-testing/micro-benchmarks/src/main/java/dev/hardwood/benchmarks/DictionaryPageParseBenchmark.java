/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;
import dev.hardwood.internal.reader.decode.DataPage;

/// Setting up one dictionary page, through each of [DictionaryParser]'s two entry points.
///
/// A caller scanning a column chunk parses each page header to learn where the page ends, so by
/// the time it reaches the dictionary it holds the header, the body it delimits, and everything
/// needed to check the page over. `parsePage` takes all of that; `parse` takes the undivided
/// region and works it out again from the bytes. Both are on the read path — the scanning callers
/// take the first, callers holding only an offset take the second — and the gap between them is
/// what a caller pays for handing over a region it has already been through.
///
/// Allocation-light by construction: one page, no file, no read. Each call hands the page to the
/// context's pool and waits for it, as every dictionary parse does; every benchmark here pays that
/// round trip alike, so the gaps between them are what the entry points cost. The end-to-end
/// counterpart, [DictionaryPageSetupBenchmark], puts the same work inside a real scan, where
/// several hundred thousand decoded values sit on top of it.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 5, jvmArgsAppend = { "-Xms1g", "-Xmx1g", "--add-modules", "jdk.incubator.vector" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class DictionaryPageParseBenchmark {

    /// Rows written to build the page. Twice the widest dictionary, so every value repeats and
    /// the writer keeps the column dictionary-encoded rather than falling back to `PLAIN`.
    private static final int ROWS = 8192;

    /// Bytes per value, which is what makes the dictionary body large enough for the checksum
    /// over it to be worth anything.
    private static final int VALUE_BYTES = 256;

    /// Distinct values in the dictionary. The body, and the checksum over it, grow with this.
    @Param({ "1024", "4096" })
    private int dictionaryValues;

    private HardwoodContextImpl context;
    private ColumnSchema columnSchema;
    private ColumnMetaData metaData;
    /// The dictionary page whole: its header followed by its body.
    private ByteBuffer region;
    /// The same page's header, parsed, as a scanning caller would already hold it.
    private PageHeader header;
    /// Where the body sits inside [#region]. Held as bounds rather than as a buffer because
    /// decoding advances the position of the buffer it is handed, so each invocation needs its
    /// own slice — as it gets on the read path, where every page is read fresh.
    private int bodyOffset;
    private int bodyLength;

    @Setup
    public void setup() throws IOException {
        Path file = Files.createTempFile("dict-page-parse", ".parquet");
        file.toFile().deleteOnExit();
        write(file, dictionaryValues);

        ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(file));
        Files.deleteIfExists(file);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(bytes))) {
            metaData = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            columnSchema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema()).getColumn(0);
        }

        long dictionaryOffset = metaData.dictionaryPageOffset();
        int regionSize = Math.toIntExact(metaData.dataPageOffset() - dictionaryOffset);
        region = bytes.slice(Math.toIntExact(dictionaryOffset), regionSize);

        ThriftCompactReader headerReader = new ThriftCompactReader(region, 0);
        header = PageHeaderReader.read(headerReader);
        bodyOffset = headerReader.getBytesRead();
        bodyLength = header.compressedPageSize();

        context = HardwoodContextImpl.create();
    }

    @TearDown
    public void tearDown() {
        context.close();
    }

    /// The scanning caller's path: parse the header to find the page's end, then set the page up
    /// from that header and the body it delimits.
    @Benchmark
    public Dictionary fromParsedHeader() throws IOException {
        ThriftCompactReader headerReader = new ThriftCompactReader(region, 0);
        PageHeader parsed = PageHeaderReader.read(headerReader);
        ByteBuffer pageBody = region.slice(headerReader.getBytesRead(), parsed.compressedPageSize());
        return DictionaryParser.parsePage(parsed, pageBody, columnSchema, metaData, context);
    }

    /// The same caller handing the undivided region over instead. It has the header and the body
    /// in hand and checks the checksum itself, and then the parser opens the region, reads that
    /// same header back out of it, cuts the same body and checksums it a second time.
    @Benchmark
    public Dictionary fromWholeRegion() throws IOException {
        ThriftCompactReader headerReader = new ThriftCompactReader(region, 0);
        PageHeader parsed = PageHeaderReader.read(headerReader);
        ByteBuffer pageBody = region.slice(headerReader.getBytesRead(), parsed.compressedPageSize());
        if (parsed.crc() != null) {
            CRC32 crc = new CRC32();
            crc.update(pageBody.duplicate());
            if ((int) crc.getValue() != parsed.crc()) {
                throw new IOException("CRC mismatch");
            }
        }
        return DictionaryParser.parse(DataPage.parse(region), columnSchema, metaData, context);
    }

    /// The floor the two sit above: the header already in hand, leaving the one checksum a
    /// page is worth and the decode both entry points end in. It is not decode alone — the
    /// fixture's writer stamps a CRC on every page, and validating it once is work the read
    /// path has to do — so what the two gaps measure is the second pass over the body, not
    /// the first.
    @Benchmark
    public Dictionary decodeOnly() throws IOException {
        return DictionaryParser.parsePage(header, region.slice(bodyOffset, bodyLength),
                columnSchema, metaData, context);
    }

    private static void write(Path path, int dictionaryValues) throws IOException {
        FileSchema schema = FileSchema.builder("dict_page_parse")
                .addColumn("label", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        // Uncompressed so the body the checksum covers is the dictionary itself, not whatever a
        // codec squeezed it down to.
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(ROWS)
                .codec(CompressionCodec.UNCOMPRESSED)
                .build();

        byte[][] values = new byte[ROWS][];
        for (int row = 0; row < ROWS; row++) {
            values[row] = value(row % dictionaryValues);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes(0, values));
        }
    }

    private static byte[] value(int ordinal) {
        byte[] bytes = new byte[VALUE_BYTES];
        byte[] prefix = ("label-" + ordinal + "-").getBytes(StandardCharsets.UTF_8);
        System.arraycopy(prefix, 0, bytes, 0, Math.min(prefix.length, VALUE_BYTES));
        for (int i = prefix.length; i < VALUE_BYTES; i++) {
            bytes[i] = (byte) ('a' + (i % 26));
        }
        return bytes;
    }
}
