/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/// Reads the `__row__` values a parquet-java 1.18.1 filter returns, with its row-group, page-index,
/// dictionary and Bloom filter pushdown at their defaults.
final class ParquetJavaReader {

    private ParquetJavaReader() {
    }

    static List<Long> rows(URI file, FilterCompat.Filter filter) throws IOException {
        List<Long> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(new Configuration())
                .withFilter(filter)
                .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                rows.add(group.getLong("__row__", 0));
            }
        }
        return rows;
    }
}
