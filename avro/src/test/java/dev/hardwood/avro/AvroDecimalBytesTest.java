/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.avro.internal.AvroPlanNode;
import dev.hardwood.avro.internal.AvroSchemaConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// Decimals the Avro reader materializes as `bytes`: a `BYTE_ARRAY` decimal as its stored
/// two's-complement bytes, an `INT32` or `INT64` one as the bytes of its unscaled value.
class AvroDecimalBytesTest {

    /// A decimal over `physicalType` materializes as its unscaled two's-complement bytes, which
    /// Avro's conversion reads back to the written value, in a field, a list element and a map value.
    @ParameterizedTest
    @EnumSource(value = PhysicalType.class, names = { "BYTE_ARRAY", "INT32", "INT64" })
    void unscaledBytesDecodeToTheWrittenDecimalInEveryPosition(PhysicalType physicalType) throws Exception {
        int precision = switch (physicalType) {
            case INT32 -> 9;
            case INT64 -> 18;
            default -> 40;
        };
        String largest = "9".repeat(precision - 3) + ".999";
        List<BigDecimal> values = List.of(new BigDecimal("0.000"), new BigDecimal("-1.234"),
                new BigDecimal("128.000"), new BigDecimal("-128.000"), new BigDecimal(largest),
                new BigDecimal("-" + largest));
        LogicalType decimal = LogicalType.decimal(precision, 3);
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("d", physicalType, RepetitionType.REQUIRED, decimal)
                .list("ds", RepetitionType.REQUIRED, element -> element.primitive(physicalType,
                        RepetitionType.REQUIRED, decimal))
                .map("dm", RepetitionType.REQUIRED, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.primitive(physicalType, RepetitionType.REQUIRED, decimal))
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (BigDecimal value : values) {
                writer.rowWriter().writeRow(r -> r
                        .setDecimal("d", value)
                        .setList("ds", l -> l.addDecimal(value))
                        .setMap("dm", m -> m.addEntry(e -> e.setString("key", "k").setDecimal("value", value))));
            }
        }

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(out.buffer()));
             AvroRowReader reader = AvroReaders.rowReader(fileReader)) {
            Schema avroSchema = reader.getSchema();
            Schema decimalSchema = avroSchema.getField("d").schema();
            assertThat(decimalSchema.getType()).isEqualTo(Schema.Type.BYTES);
            Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();

            List<GenericRecord> records = new ArrayList<>();
            while (reader.hasNext()) {
                records.add(reader.next());
            }
            assertThat(records).hasSize(values.size());
            for (int i = 0; i < values.size(); i++) {
                GenericRecord record = records.get(i);
                ByteBuffer field = (ByteBuffer) record.get("d");
                ByteBuffer element = (ByteBuffer) ((List<?>) record.get("ds")).get(0);
                ByteBuffer mapValue = (ByteBuffer) ((Map<?, ?>) record.get("dm")).get("k");
                for (ByteBuffer bytes : List.of(field, element, mapValue)) {
                    assertThat(bytes.duplicate()).isEqualTo(ByteBuffer.wrap(values.get(i).unscaledValue().toByteArray()));
                    assertThat(conversion.fromBytes(bytes.duplicate(), decimalSchema, decimalSchema.getLogicalType()))
                            .isEqualTo(values.get(i));
                }
            }

            GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null);
            for (GenericRecord record : records) {
                datumWriter.write(record, encoder);
            }
            encoder.flush();
        }
    }

    /// An empty `BYTE_ARRAY` decimal, which the row reader reads as zero, materializes as the one
    /// byte of zero: Avro's conversion rejects empty bytes.
    @Test
    void anEmptyStoredValueMaterializesAsZero() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("d", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, LogicalType.decimal(10, 2))
                .build();
        AvroPlanNode plan = AvroSchemaConverter.plan(schema, ColumnProjection.all());
        RowReader rows = (RowReader) Proxy.newProxyInstance(RowReader.class.getClassLoader(),
                new Class<?>[]{ RowReader.class },
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isNull" -> false;
                    case "getRawValue" -> new byte[0];
                    case "next", "close" -> null;
                    default -> throw new AssertionError("Unexpected accessor: " + method.getName());
                });
        try (AvroRowReader reader = new AvroRowReader(rows, plan)) {
            GenericRecord record = reader.next();
            Schema decimalSchema = record.getSchema().getField("d").schema();
            ByteBuffer bytes = (ByteBuffer) record.get("d");
            assertThat(bytes).isEqualTo(ByteBuffer.wrap(new byte[1]));
            assertThat(new Conversions.DecimalConversion().fromBytes(bytes, decimalSchema,
                    decimalSchema.getLogicalType())).isEqualTo(new BigDecimal("0.00"));
        }
    }
}
