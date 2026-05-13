/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.DecryptionKeyProvider;
import dev.hardwood.InputFile;
import dev.hardwood.ParquetEncryptionException;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EncryptionTest {

    private static final String KEY_BASE64 =
            java.util.Base64.getEncoder().encodeToString(
                    "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));

    private DecryptionKeyProvider keyProvider = keyMetadata -> {
        String json = new String(keyMetadata, StandardCharsets.UTF_8);
        // parquet-java fixtures store raw key identifier as metadata
        if (!json.startsWith("{")) {
            return java.util.Base64.getDecoder().decode(KEY_BASE64);
        }
        // pyarrow fixtures store a JSON blob with wrappedDEK
        int start = json.indexOf("\"wrappedDEK\":\"") + "\"wrappedDEK\":\"".length();
        int end = json.indexOf("\"", start);
        return java.util.Base64.getDecoder().decode(json.substring(start, end));
    };

    @Test
    void testEncryptColsPlaintextFooter() throws IOException, URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_plaintext_footer.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .open();
             ) {
            FileSchema schema = fileReader.getFileSchema();

            validateSchema(schema);
            validateData(fileReader);
        }
    }

    @Test
    void testEncryptColsAndFooterWithGcm() throws IOException, URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .open();
        ) {
            FileSchema schema = fileReader.getFileSchema();

            validateSchema(schema);
            validateData(fileReader);
        }
    }

    @Test
    void testEncryptColsAndFooterWithGcmCtrMode() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm_ctr.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .open();
        ) {
            FileSchema schema = fileReader.getFileSchema();

            validateSchema(schema);
            validateData(fileReader);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testEncryptColsAndFooterWithDiffKeys() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypted_columns_and_footer_but_with_diff_keys.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .open();
        ) {
            FileSchema schema = fileReader.getFileSchema();

            validateSchema(schema);
            validateData(fileReader);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testAadPrefixStoredInFile() throws IOException, URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_aad_prefix_stored.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .open()) {
            FileSchema schema = fileReader.getFileSchema();
            validateSchema(schema);
            validateData(fileReader);
        }
    }

    @Test
    void testAadPrefixSupplied() throws IOException, URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_aad_prefix_supplied.parquet")
                .toURI());

        try (ParquetFileReader fileReader = ParquetFileReader.builder(InputFile.of(file))
                .keyProvider(keyProvider)
                .aadPrefixProvider(() -> "tenant-123".getBytes(StandardCharsets.UTF_8))
                .open()) {
            FileSchema schema = fileReader.getFileSchema();
            validateSchema(schema);
            validateData(fileReader);
        }
    }

    @Test
    void testMixedFiles() throws IOException, URISyntaxException {
        Path encryptedFile = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());
        Path plaintextFile = Path.of(getClass().getClassLoader()
                .getResource("plain/plaintext.parquet")
                .toURI());

        List<InputFile> files = List.of(
                InputFile.of(encryptedFile),
                InputFile.of(plaintextFile));

        try (ParquetFileReader fileReader = ParquetFileReader.openAll(files, keyProvider)) {
            validateSchema(fileReader.getFileSchema());

            RowReader rowReader = fileReader.rowReader();
            int rowCount = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                rowCount++;
            }
            // 3 rows from encrypted file + 3 rows from plaintext file
            assertEquals(6, rowCount);
        }
    }

    // Tests to validate exception handling

    @Test
    void testNullAadProviderFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_aad_prefix_supplied.parquet")
                .toURI());

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(keyProvider)
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_aad_prefix_supplied.parquet] File decryption requires user-supplied AAD prefix but no AadPrefixProvider was supplied";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testNullAadPrefixFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_aad_prefix_supplied.parquet")
                .toURI());

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(keyProvider)
                    .aadPrefixProvider(() -> null)
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_aad_prefix_supplied.parquet] AadPrefixProvider returned null or empty AAD prefix";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testEmptyAadPrefixFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_aad_prefix_supplied.parquet")
                .toURI());

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(keyProvider)
                    .aadPrefixProvider(() -> new byte[0])
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_aad_prefix_supplied.parquet] AadPrefixProvider returned null or empty AAD prefix";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testWrongKeyFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        DecryptionKeyProvider wrongKeyProvider = keyMetadata -> new byte[32]; // all zeros

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(wrongKeyProvider)
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_gcm.parquet] Failed to decrypt data (GCM)";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testNullKeyProviderFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_gcm.parquet] File has encrypted footer but key provider not supplied";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testNullKeyFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        //DecryptionKeyProvider nullKeyProvider = keyMetadata -> null;
        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(keyMetadata -> null)
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_gcm.parquet] DecryptionKeyProvider returned null or empty key for footer";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testEmptyKeyFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        Exception exception = assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .keyProvider(keyMetadata -> null)
                    .open();
        });
        String expectedMessage = "[encrypt_columns_footer_gcm.parquet] DecryptionKeyProvider returned null or empty key for footer";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage, "Error message should match");
    }

    @Test
    void testMissingKeyProviderFails() throws URISyntaxException {
        Path file = Path.of(getClass().getClassLoader()
                .getResource("encryption/encrypt_columns_footer_gcm.parquet")
                .toURI());

        assertThrows(ParquetEncryptionException.class, () -> {
            ParquetFileReader.builder(InputFile.of(file))
                    .open(); // no keyProvider
        });
    }

    private void validateSchema(FileSchema schema) {
        // validate number of columns in schema
        assertEquals(schema.getColumnCount(), 3, "Column count wrong");

        // validate data types in schema
        assertEquals(schema.getColumn(0).name(), "id", "Wrong column name at index 0");
        assertEquals(schema.getColumn(0).type(), PhysicalType.INT64, "Wrong data type at index 0");
        assertEquals(schema.getColumn(1).name(), "name", "Wrong column name at index 1");
        assertEquals(schema.getColumn(1).type(), PhysicalType.BYTE_ARRAY, "Wrong data type at index 0");
        assertEquals(schema.getColumn(2).name(), "salary", "Wrong column name at index 2");
        assertEquals(schema.getColumn(2).type(), PhysicalType.INT64, "Wrong data type at index 0");
    }

    private void validateData(ParquetFileReader fileReader) {
        // validate values
        RowReader rowReader = fileReader.rowReader();
        int rowIndex = 0;
        while (rowReader.hasNext()) {
            rowReader.next();
            long id = rowReader.getLong("id");
            String name = rowReader.getString("name");
            long salary = rowReader.getLong("salary");

            if (rowIndex == 0) {
                assertEquals(1L, id);
                assertEquals("Mark", name);
                assertEquals(10001L, salary);
            } else if (rowIndex == 1) {
                assertEquals(2L, id);
                assertEquals("Mary", name);
                assertEquals(45345L, salary);
            } else if (rowIndex == 2) {
                assertEquals(3L, id);
                assertEquals("Mike", name);
                assertEquals(34345L, salary);
            }
            rowIndex++;
        }
    }
}
