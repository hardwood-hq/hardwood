#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, time
from decimal import Decimal
import uuid

# Plain encoding with no compression (for Milestone 1)
# Create a simple table with NO nulls first, explicitly marking fields as non-nullable
schema = pa.schema([
    ('id', pa.int64(), False),  # False = not nullable (REQUIRED)
    ('value', pa.int64(), False)
])
simple_table = pa.table({
    'id': [1, 2, 3],
    'value': [100, 200, 300]
}, schema=schema)

pq.write_table(simple_table, 'src/test/resources/plain_uncompressed.parquet',
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("Generated plain_uncompressed.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: id=[1,2,3], value=[100,200,300] - NO NULLS")

# Also generate one with nulls
# Make id REQUIRED (no nulls) and name OPTIONAL (with nulls)
schema_with_nulls = pa.schema([
    ('id', pa.int64(), False),  # REQUIRED - no nulls
    ('name', pa.string(), True)  # OPTIONAL - can have nulls
])
table_with_nulls = pa.table({
    'id': [1, 2, 3],
    'name': ['alice', None, 'charlie']
}, schema=schema_with_nulls)
pq.write_table(table_with_nulls, 'src/test/resources/plain_uncompressed_with_nulls.parquet',
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("\nGenerated plain_uncompressed_with_nulls.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: id=[1,2,3], name=['alice', None, 'charlie']")

# Generate SNAPPY compressed file with same data as plain_uncompressed
pq.write_table(simple_table, 'src/test/resources/plain_snappy.parquet',
               use_dictionary=False,
               compression='snappy',
               data_page_version='1.0')

print("\nGenerated plain_snappy.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: SNAPPY (compression='snappy')")
print("  - Data: id=[1,2,3], value=[100,200,300] - NO NULLS")

# Generate dictionary encoded file with strings
schema_dict = pa.schema([
    ('id', pa.int64(), False),
    ('category', pa.string(), False)
])
table_dict = pa.table({
    'id': [1, 2, 3, 4, 5],
    'category': ['A', 'B', 'A', 'C', 'B']  # Repeated values - good for dictionary
}, schema=schema_dict)

# Only use dictionary for the category column (column 1), not id (column 0)
pq.write_table(table_dict, 'src/test/resources/dictionary_uncompressed.parquet',
               use_dictionary=['category'],  # Only dictionary encode the category column
               compression=None,
               data_page_version='1.0')

print("\nGenerated dictionary_uncompressed.parquet:")
print("  - Encoding: DICTIONARY (use_dictionary=True)")
print("  - Compression: UNCOMPRESSED")
print("  - Data: id=[1,2,3,4,5], category=['A','B','A','C','B']")

# Generate logical types test file
logical_types_schema = pa.schema([
    ('id', pa.int32(), False),  # Simple INT32 (no logical type)
    ('name', pa.string(), False),  # STRING logical type
    ('birth_date', pa.date32(), False),  # DATE logical type (INT32 days since epoch)
    ('created_at_millis', pa.timestamp('ms', tz='UTC'), False),  # TIMESTAMP(MILLIS, UTC)
    ('created_at_micros', pa.timestamp('us', tz='UTC'), False),  # TIMESTAMP(MICROS, UTC)
    ('created_at_nanos', pa.timestamp('ns', tz='UTC'), False),  # TIMESTAMP(NANOS, UTC)
    ('wake_time_millis', pa.time32('ms'), False),  # TIME(MILLIS)
    ('wake_time_micros', pa.time64('us'), False),  # TIME(MICROS)
    ('wake_time_nanos', pa.time64('ns'), False),  # TIME(NANOS)
    ('balance', pa.decimal128(10, 2), False),  # DECIMAL(scale=2, precision=10)
    ('tiny_int', pa.int8(), False),  # INT_8 logical type
    ('small_int', pa.int16(), False),  # INT_16 logical type
    ('medium_int', pa.int32(), False),  # INT_32 logical type
    ('big_int', pa.int64(), False),  # INT_64 logical type
    ('tiny_uint', pa.uint8(), False),  # UINT_8 logical type
    ('small_uint', pa.uint16(), False),  # UINT_16 logical type
    ('medium_uint', pa.uint32(), False),  # UINT_32 logical type
    ('big_uint', pa.uint64(), False),  # UINT_64 logical type
    ('account_id', pa.uuid(), False),  # UUID logical type (supported in PyArrow 21+)
])

logical_types_data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'birth_date': [
        date(1990, 1, 15),
        date(1985, 6, 30),
        date(2000, 12, 25)
    ],
    'created_at_millis': [
        datetime(2025, 1, 1, 10, 30, 0),
        datetime(2025, 1, 2, 14, 45, 30),
        datetime(2025, 1, 3, 9, 15, 45)
    ],
    'created_at_micros': [
        datetime(2025, 1, 1, 10, 30, 0, 123456),
        datetime(2025, 1, 2, 14, 45, 30, 654321),
        datetime(2025, 1, 3, 9, 15, 45, 111222)
    ],
    # NANOS columns use raw int64 values since Python datetime only supports microseconds
    # Values are nanoseconds since epoch with 9-digit precision (e.g., .123456789)
    'created_at_nanos': pa.array([
        1735727400123456789,  # 2025-01-01T10:30:00.123456789Z
        1735829130654321987,  # 2025-01-02T14:45:30.654321987Z
        1735895745111222333,  # 2025-01-03T09:15:45.111222333Z
    ], type=pa.timestamp('ns', tz='UTC')),
    'wake_time_millis': [
        time(7, 30, 0),
        time(8, 0, 0),
        time(6, 45, 0)
    ],
    'wake_time_micros': [
        time(7, 30, 0, 123456),
        time(8, 0, 0, 654321),
        time(6, 45, 0, 111222)
    ],
    # TIME NANOS uses raw int64 values (nanoseconds since midnight)
    'wake_time_nanos': pa.array([
        27000123456789,  # 7:30:00.123456789
        28800654321987,  # 8:00:00.654321987
        24300111222333,  # 6:45:00.111222333
    ], type=pa.time64('ns')),
    'balance': [
        Decimal('1234.56'),
        Decimal('9876.54'),
        Decimal('5555.55')
    ],
    'tiny_int': [10, 20, 30],
    'small_int': [1000, 2000, 3000],
    'medium_int': [100000, 200000, 300000],
    'big_int': [10000000000, 20000000000, 30000000000],
    'tiny_uint': [255, 128, 64],
    'small_uint': [65535, 32768, 16384],
    # For UINT_32, use values that fit in signed int32 for easier testing
    'medium_uint': [2147483647, 1000000, 500000],
    # Java's long is signed, so max is 2^63-1. Use values within signed long range for testing.
    'big_uint': [9223372036854775807, 5000000000000000000, 4611686018427387904],
    'account_id': [
        uuid.UUID('12345678-1234-5678-1234-567812345678').bytes,
        uuid.UUID('87654321-4321-8765-4321-876543218765').bytes,
        uuid.UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee').bytes
    ]
}

logical_types_table = pa.table(logical_types_data, schema=logical_types_schema)

pq.write_table(
    logical_types_table,
    'src/test/resources/logical_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated logical_types_test.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: 3 rows with various logical types (DATE, TIMESTAMP, TIME, DECIMAL, INT_8/16/32/64, UINT_8/16/32/64, UUID)")
