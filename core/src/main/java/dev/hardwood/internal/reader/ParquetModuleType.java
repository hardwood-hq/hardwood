/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Module type constants for Parquet encryption AAD suffix construction.
///
/// In Parquet encryption, every encrypted module has an AAD (Additional Authenticated Data)
/// built from two parts:
///
/// <pre>
/// +----------------+--------------------------------------------------+
/// |   AAD Prefix   |                  AAD Suffix                      |
/// +----------------+--------------------------------------------------+
/// | optional,      | aadFileUnique | moduleType | rowGroupOrdinal     |
/// | caller-supplied|  (N bytes)    |  (1 byte)  | (2 bytes, optional) |
/// |                |               |            | columnOrdinal       |
/// |                |               |            | (2 bytes, optional) |
/// |                |               |            | pageOrdinal         |
/// |                |               |            | (2 bytes, optional) |
/// +----------------+--------------------------------------------------+
/// </pre>
///
/// The module type byte identifies which part of the file is being encrypted,
/// protecting against module swapping attacks (e.g. replacing a page for a column with page for another column).
///
/// Which ordinals are included depends on the module type:
///
/// <pre>
/// Module               | rowGroup | column | page
/// ---------------------|----------|--------|------
/// Footer               |    no    |   no   |  no
/// ColumnMetaData       |   yes    |  yes   |  no
/// Data Page            |   yes    |  yes   | yes
/// Dictionary Page      |   yes    |  yes   |  no
/// Data Page Header     |   yes    |  yes   | yes
/// Dict Page Header     |   yes    |  yes   |  no
/// Column Index         |   yes    |  yes   |  no
/// Offset Index         |   yes    |  yes   |  no
/// BloomFilter Header   |   yes    |  yes   |  no
/// BloomFilter Bitset   |   yes    |  yes   |  no
/// </pre>
///
/// @see <a href="https://github.com/apache/parquet-format/blob/master/Encryption.md#442-aad-suffix">Parquet Encryption Spec - AAD Suffix</a>
public final class ParquetModuleType {

    private ParquetModuleType() {
        // Constants class
    }

    /// Footer module (FileMetaData).
    public static final byte FOOTER                = 0x00;

    /// Column metadata module (ColumnMetaData).
    public static final byte COLUMN_META           = 0x01;

    /// Data page module (page data bytes).
    public static final byte DATA_PAGE             = 0x02;

    /// Dictionary page module (dictionary page data bytes).
    public static final byte DICT_PAGE             = 0x03;

    /// Data page header module (serialized Thrift PageHeader for data pages).
    public static final byte DATA_PAGE_HEADER      = 0x04;

    /// Dictionary page header module (serialized Thrift PageHeader for dictionary pages).
    public static final byte DICT_PAGE_HEADER      = 0x05;

    /// Column index module.
    public static final byte COLUMN_INDEX          = 0x06;

    /// Offset index module.
    public static final byte OFFSET_INDEX          = 0x07;

    /// Bloom filter header module.
    public static final byte BLOOM_FILTER_HEADER   = 0x08;

    /// Bloom filter bitset module.
    public static final byte BLOOM_FILTER_BITSET   = 0x09;
}