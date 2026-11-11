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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.PlainInt64File;
import dev.hardwood.internal.thrift.PlainInt64File.Page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A column chunk without an offset index is read by scanning its page headers: a header whose
/// `compressed_page_size` takes the page past the end of its chunk fails the read, on a pool and
/// on a context of zero threads.
class ScannedPageSizeTest {

    private static final List<Page> TWO_PAGES = List.of(new Page(0, 10), new Page(10, 10));

    @ParameterizedTest
    @ValueSource(ints = { 0, 2 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void pagesWithinTheirChunkRead(int threads) throws IOException {
        assertThat(sum(PlainInt64File.build(TWO_PAGES), threads)).isEqualTo(190);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void pageDeclaredLargerThanTheRestOfItsChunkFails(int threads) {
        byte[] file = PlainInt64File.build(List.of(new Page(0, 10, 1000), new Page(10, 10)));

        assertThatThrownBy(() -> sum(file, threads))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'v', page 0] Page at offset 4 ends at offset 1029, "
                        + "past the end of the column chunk at offset 214");
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void pageDeclaringNearlyTwoGibibytesFails(int threads) {
        byte[] file = PlainInt64File.build(List.of(new Page(0, 10, Integer.MAX_VALUE - 1), new Page(10, 10)));

        assertThatThrownBy(() -> sum(file, threads))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'v', page 0] Page at offset 4 ends at offset 2147483678, "
                        + "past the end of the column chunk at offset 217");
    }

    private static long sum(byte[] file, int threads) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(threads);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            long sum = 0;
            while (rows.hasNext()) {
                rows.next();
                sum += rows.getLong("v");
            }
            return sum;
        }
    }
}
