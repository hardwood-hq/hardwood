/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;

/// The Hadoop [Configuration] instances the parquet-java reference paths read through.
///
/// Constructing one is not the cheap object allocation it looks like: it parses
/// `core-default.xml` out of the Hadoop jar, some four thousand lines of XML, and does so once
/// per instance. At the rate this module reads files back through parquet-java — five reads per
/// interop case, two per corpus fixture, hundreds of each — that parse costs more CPU than every
/// Parquet encode and decode in the module put together.
///
/// Nothing here writes to a configuration after it is built, so one instance per distinct
/// configuration is the same configuration each call site would otherwise have built for itself.
/// A site that does need to set something builds its own.
final class HadoopConf {

    /// Hadoop's defaults, as a bare `new Configuration()` produces them.
    static final Configuration DEFAULTS = new Configuration();

    /// The defaults, plus the setting that makes parquet-java's Avro reader surface an INT96
    /// column as a fixed-length byte array rather than refusing the type it cannot model.
    static final Configuration AVRO_INT96_AS_FIXED = avroInt96AsFixed();

    /// Read options over [#DEFAULTS], for `ParquetFileReader.open`, which without options builds a
    /// `Configuration` of its own for them. Built per reader, since the options carry the codec
    /// factory that a reader releases when it closes.
    static ParquetReadOptions readOptions() {
        return HadoopReadOptions.builder(DEFAULTS).build();
    }

    private static Configuration avroInt96AsFixed() {
        Configuration conf = new Configuration();
        conf.set("parquet.avro.readInt96AsFixed", "true");
        return conf;
    }

    private HadoopConf() {
    }
}
