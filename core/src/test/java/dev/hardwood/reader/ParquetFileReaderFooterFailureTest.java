/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.CountingInputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Footer-derived schema failures are file errors on both reader paths.
class ParquetFileReaderFooterFailureTest {

    private static final Path VARIANT_FIXTURE = Path.of("src/test/resources/variant_test.parquet");
    private static final String MALFORMED_SCHEMA_MESSAGE =
            "[<memory>] Variant group 'var' expected child 'metadata', found: invalid_metadata";

    private static byte[] malformedVariantSchema() throws IOException {
        byte[] validFile = Files.readAllBytes(VARIANT_FIXTURE);
        return FooterRewriter.rewrite(validFile, metaData -> {
            List<SchemaElement> schema = new ArrayList<>(metaData.schema());
            int metadataIndex = -1;
            for (int i = 0; i < schema.size(); i++) {
                if (schema.get(i).name().equals("metadata")) {
                    metadataIndex = i;
                    break;
                }
            }
            if (metadataIndex < 0) {
                throw new AssertionError("Variant fixture has no metadata child");
            }

            SchemaElement metadata = schema.get(metadataIndex);
            schema.set(metadataIndex, new SchemaElement("invalid_metadata", metadata.type(),
                    metadata.typeLength(), metadata.repetitionType(), metadata.numChildren(),
                    metadata.convertedType(), metadata.scale(), metadata.precision(), metadata.fieldId(),
                    metadata.logicalType()));
            return new FileMetaData(metaData.version(), schema, metaData.numRows(), metaData.rowGroups(),
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        });
    }

    @Test
    void malformedVariantSchemaOnEagerOpenIsAReadFailure() throws IOException {
        byte[] malformed = malformedVariantSchema();

        assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(malformed))))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(MALFORMED_SCHEMA_MESSAGE);
    }

    @Test
    void malformedVariantSchemaOnLazyLoadIsCachedAsAReadFailure() throws IOException {
        byte[] malformed = malformedVariantSchema();
        CountingInputFile invalid = new CountingInputFile(ByteBuffer.wrap(malformed));

        try (ParquetFileReader reader = ParquetFileReader.openAll(
                List.of(InputFile.of(VARIANT_FIXTURE), invalid))) {
            assertThatThrownBy(() -> reader.getFileMetaData(1))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage(MALFORMED_SCHEMA_MESSAGE);
            int footerReadsAfterFailure = invalid.footerReadCount();

            assertThatThrownBy(() -> reader.getFileMetaData(1))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage(MALFORMED_SCHEMA_MESSAGE);
            assertThat(invalid.footerReadCount()).isEqualTo(footerReadsAfterFailure);
        }
    }
}
