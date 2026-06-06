#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import base64
import numpy
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from datetime import datetime, date, time, timezone
from decimal import Decimal
import uuid

import shutil
from pathlib import Path

from parquet_annotators import (
    annotate_column_as_bson,
    annotate_group_as_variant,
    annotate_group_at_path_as_variant,
    annotate_column_as_interval,
    annotate_element_at_path_as_float16,
    annotate_element_at_path_as_interval,
    annotate_element_at_path_as_json,
    annotate_element_at_path_as_time,
    annotate_element_at_path_as_decimal,
    annotate_columns_as_legacy_converted_type,
    annotate_map_as_legacy_key_value,
    strip_converted_type,
    corrupt_data_page_offset_negative,
)


def _copy_if_exists(src: Path, dst: str, prereq_hint: str) -> bool:
    """Copy `src` to `dst` if `src` exists; otherwise print a warning and return False.

    Use this for fixtures sourced from another module's build output: the source
    file is only present after that module has been built, so a fresh checkout
    won't have it. Skipping (rather than failing) keeps the rest of the data
    generation usable in that case.
    """
    if not src.exists():
        print(f"\nSkipping {dst}:")
        print(f"  Source not found: {src}")
        print(f"  Hint: {prereq_hint}")
        return False
    shutil.copyfile(src, dst)
    return True

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

pq.write_table(simple_table, 'core/src/test/resources/plain_uncompressed.parquet',
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
pq.write_table(table_with_nulls, 'core/src/test/resources/plain_uncompressed_with_nulls.parquet',
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("\nGenerated plain_uncompressed_with_nulls.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: id=[1,2,3], name=['alice', None, 'charlie']")

# Generate SNAPPY compressed file with same data as plain_uncompressed
pq.write_table(simple_table, 'core/src/test/resources/plain_snappy.parquet',
               use_dictionary=False,
               compression='snappy',
               data_page_version='1.0')

print("\nGenerated plain_snappy.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: SNAPPY (compression='snappy')")
print("  - Data: id=[1,2,3], value=[100,200,300] - NO NULLS")

# hardwood-hq/hardwood#594: pins the ColumnReader batch-array non-aliasing
# contract. Four rows of one flat column per accessor family plus a list<int32>
# column. The null pattern (present / null / null / present) is chosen so that
# reading with batchSize=2 yields two batches that EACH carry a real (backed)
# validity bitmap — never the shared Validity.NO_NULLS singleton — so the test
# can assert the backing long[] is freshly allocated per batch, not just the
# value arrays. Every flat column is non-null in row 0 and row 3 so both batches
# also hold at least one real binary value.
batch_identity_schema = pa.schema([
    ('ints', pa.int32(), True),
    ('longs', pa.int64(), True),
    ('floats', pa.float32(), True),
    ('doubles', pa.float64(), True),
    ('flags', pa.bool_(), True),
    ('labels', pa.string(), True),
    ('nums', pa.list_(pa.int32()), True),
])
batch_identity_table = pa.table({
    'ints': [10, None, None, 40],
    'longs': [100, None, None, 400],
    'floats': [1.5, None, None, 4.5],
    'doubles': [2.5, None, None, 5.5],
    'flags': [True, None, None, False],
    'labels': ['alpha', None, None, 'delta'],
    'nums': [[1, 2], None, None, [3]],
}, schema=batch_identity_schema)
pq.write_table(batch_identity_table,
               'core/src/test/resources/batch_array_identity_test.parquet',
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("\nGenerated batch_array_identity_test.parquet:")
print("  - Rows: present / null / null / present per flat column")
print("  - nums: [1,2] / null / null / [3]")
print("  - With batchSize=2 both batches carry backed validity bitmaps")

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
pq.write_table(table_dict, 'core/src/test/resources/dictionary_uncompressed.parquet',
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
    ('profile_json', pa.json_(), False),  # JSON logical type (BYTE_ARRAY backed)
    ('bson_payload', pa.binary(), False),
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
    ],
    'profile_json': [
        '{"role":"admin","tags":["x","y"]}',
        '{"role":"user","active":true}',
        '{"nested":{"k":1,"v":[1,2,3]}}'
    ],
    # Three hand-crafted BSON documents. The first is a minimal empty doc;
    # the others embed non-UTF-8 bytes (0x80, 0xFF) that would be corrupted
    # if BSON were incorrectly decoded as UTF-8.
    'bson_payload': [
        bytes.fromhex('0500000000'),
        bytes.fromhex('12000000026b0006000000686921800000'),
        bytes.fromhex('0f00000005780003000000ff00fe0000'),
    ],
}

logical_types_table = pa.table(logical_types_data, schema=logical_types_schema)

pq.write_table(
    logical_types_table,
    'core/src/test/resources/logical_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)
# PyArrow writes `bson_payload` as a plain BYTE_ARRAY column; tag it as BSON
# in the footer so Hardwood's schema reader sees the right logical type.
annotate_column_as_bson('core/src/test/resources/logical_types_test.parquet', 'bson_payload')

print("\nGenerated logical_types_test.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: 3 rows with various logical types (DATE, TIMESTAMP, TIME, DECIMAL, INT_8/16/32/64, UINT_8/16/32/64, UUID, JSON, BSON)")

# ============================================================================
# Nested Data Test Files
# ============================================================================

# 1. Nested struct test
nested_struct_schema = pa.schema([
    ('id', pa.int32(), False),
    ('address', pa.struct([
        ('street', pa.string()),
        ('city', pa.string()),
        ('zip', pa.int32())
    ]))
])

nested_struct_data = {
    'id': [1, 2, 3],
    'address': [
        {'street': '123 Main St', 'city': 'New York', 'zip': 10001},
        {'street': '456 Oak Ave', 'city': 'Los Angeles', 'zip': 90001},
        None  # null struct
    ]
}

nested_struct_table = pa.table(nested_struct_data, schema=nested_struct_schema)
pq.write_table(
    nested_struct_table,
    'core/src/test/resources/nested_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated nested_struct_test.parquet:")
print("  - Data: id=[1,2,3], address=[{street,city,zip}, {street,city,zip}, null]")

# 1b. Nested dictionary-encoded string decoded across a batch boundary.
# Regression fixture for a nested-read bug: the drain reused one bytes buffer
# across batches and published each batch by aliasing it, so assembling the next
# batch overwrote the previous batch's still-unread values. It triggers whenever
# a single nested variable-length leaf spans more than one batch. The row reader
# auto-sizes its batch to 6 MB / row width (so a lone string column needs
# > ~393k rows to cross), but the ColumnReader path shares the same worker and
# takes an explicit batch size, so the test reads this fixture with a batch of 64
# and 300 rows suffice. Values are distinct so any corruption is obvious; the
# struct is null on every 7th row, the name on every 3rd. data_page_version
# '1.0' matches the real-world (PLAIN dictionary page) layout that surfaced this.
NESTED_BATCH_ROWS = 300

def _nested_batch_name(i):
    # name is null on every 3rd row (within a present struct)
    return None if i % 3 == 0 else f"value_{i:04d}"

nested_batch_table = pa.table(
    {
        # nested struct is null on every 7th row
        'nested': pa.array(
            [None if i % 7 == 0 else {'name': _nested_batch_name(i)}
             for i in range(NESTED_BATCH_ROWS)],
            type=pa.struct([('name', pa.string())])),
    },
    schema=pa.schema([('nested', pa.struct([('name', pa.string())]))]),
)
pq.write_table(
    nested_batch_table,
    'core/src/test/resources/nested_dict_batch_boundary.parquet',
    use_dictionary=True,
    compression='snappy',
    data_page_version='1.0',
)

print("\nGenerated nested_dict_batch_boundary.parquet:")
print(f"  - {NESTED_BATCH_ROWS} rows, nested.name (dictionary-encoded string; read across batches)")

# 2. List of basic types test
list_basic_schema = pa.schema([
    ('id', pa.int32(), False),
    ('tags', pa.list_(pa.string())),
    ('scores', pa.list_(pa.int32()))
])

list_basic_data = {
    'id': [1, 2, 3, 4],
    'tags': [
        ['a', 'b', 'c'],       # normal list
        [],                    # empty list
        None,                  # null list
        ['single']             # single element
    ],
    'scores': [
        [10, 20, 30],
        [100],
        [1, 2],
        None
    ]
}

list_basic_table = pa.table(list_basic_data, schema=list_basic_schema)
pq.write_table(
    list_basic_table,
    'core/src/test/resources/list_basic_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated list_basic_test.parquet:")
print("  - Data: id=[1,2,3,4], tags=[[a,b,c],[],null,[single]], scores=[[10,20,30],[100],[1,2],null]")

# 3. List of structs test
list_struct_schema = pa.schema([
    ('id', pa.int32(), False),
    ('items', pa.list_(pa.struct([
        ('name', pa.string()),
        ('quantity', pa.int32())
    ])))
])

list_struct_data = {
    'id': [1, 2, 3],
    'items': [
        [
            {'name': 'apple', 'quantity': 5},
            {'name': 'banana', 'quantity': 10}
        ],
        [
            {'name': 'orange', 'quantity': 3}
        ],
        []  # empty list
    ]
}

list_struct_table = pa.table(list_struct_data, schema=list_struct_schema)
pq.write_table(
    list_struct_table,
    'core/src/test/resources/list_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated list_struct_test.parquet:")
print("  - Data: id=[1,2,3], items=[[{apple,5},{banana,10}],[{orange,3}],[]]")

# 4. Nested list of structs test (list -> struct -> list -> struct)
# Schema: Book -> chapters (list) -> Chapter (struct) -> sections (list) -> Section (struct)
section_type = pa.struct([
    ('name', pa.string()),
    ('page_count', pa.int32())
])

chapter_type = pa.struct([
    ('name', pa.string()),
    ('sections', pa.list_(section_type))
])

nested_list_struct_schema = pa.schema([
    ('title', pa.string()),
    ('chapters', pa.list_(chapter_type))
])

nested_list_struct_data = [
    # Book 0: "Parquet Guide" with 2 chapters, each with sections
    {
        'title': 'Parquet Guide',
        'chapters': [
            {
                'name': 'Introduction',
                'sections': [
                    {'name': 'What is Parquet', 'page_count': 5},
                    {'name': 'History', 'page_count': 3}
                ]
            },
            {
                'name': 'Schema',
                'sections': [
                    {'name': 'Types', 'page_count': 10},
                    {'name': 'Nesting', 'page_count': 8},
                    {'name': 'Repetition', 'page_count': 12}
                ]
            }
        ]
    },
    # Book 1: "Empty Chapters" with 1 chapter that has no sections
    {
        'title': 'Empty Chapters',
        'chapters': [
            {
                'name': 'The Only Chapter',
                'sections': []
            }
        ]
    },
    # Book 2: "No Chapters" with empty chapters list
    {
        'title': 'No Chapters',
        'chapters': []
    }
]

nested_list_struct_table = pa.Table.from_pylist(nested_list_struct_data, schema=nested_list_struct_schema)
pq.write_table(
    nested_list_struct_table,
    'core/src/test/resources/nested_list_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated nested_list_struct_test.parquet:")
print("  - Schema: Book(title, chapters: list<Chapter(name, sections: list<Section(name, page_count)>)>)")
print("  - Data: 3 books with nested chapters and sections")

# 5. Multi-level nested struct test (Customer -> Account -> Organization -> Address)
address_type = pa.struct([
    ('street', pa.string()),
    ('city', pa.string()),
    ('zip', pa.int32())
])

organization_type = pa.struct([
    ('name', pa.string()),
    ('address', address_type)
])

account_type = pa.struct([
    ('id', pa.string()),
    ('organization', organization_type)
])

deep_nested_struct_schema = pa.schema([
    ('customer_id', pa.int32(), False),
    ('name', pa.string()),
    ('account', account_type)
])

deep_nested_struct_data = [
    {
        'customer_id': 1,
        'name': 'Alice',
        'account': {
            'id': 'ACC-001',
            'organization': {
                'name': 'Acme Corp',
                'address': {
                    'street': '123 Main St',
                    'city': 'New York',
                    'zip': 10001
                }
            }
        }
    },
    {
        'customer_id': 2,
        'name': 'Bob',
        'account': {
            'id': 'ACC-002',
            'organization': {
                'name': 'TechStart',
                'address': None  # null address
            }
        }
    },
    {
        'customer_id': 3,
        'name': 'Charlie',
        'account': {
            'id': 'ACC-003',
            'organization': None  # null organization
        }
    },
    {
        'customer_id': 4,
        'name': 'Diana',
        'account': None  # null account
    }
]

deep_nested_struct_table = pa.Table.from_pylist(deep_nested_struct_data, schema=deep_nested_struct_schema)
pq.write_table(
    deep_nested_struct_table,
    'core/src/test/resources/deep_nested_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated deep_nested_struct_test.parquet:")
print("  - Schema: Customer(id, name, account: Account(id, organization: Organization(name, address: Address(street, city, zip))))")
print("  - Data: 4 customers with varying levels of null nested structs")

# 6. Nested list test (list<list<int32>>)
nested_list_schema = pa.schema([
    ('id', pa.int32(), False),
    ('matrix', pa.list_(pa.list_(pa.int32()))),  # list of list of int
    ('string_matrix', pa.list_(pa.list_(pa.string()))),  # list of list of string
    ('timestamp_matrix', pa.list_(pa.list_(pa.timestamp('ms', tz='UTC'))))  # list of list of timestamp
])

nested_list_data = [
    {
        'id': 1,
        'matrix': [[1, 2], [3, 4, 5], [6]],  # 3 inner lists
        'string_matrix': [['a', 'b'], ['c']],
        'timestamp_matrix': [
            [datetime(2025, 1, 1, 10, 0, 0), datetime(2025, 1, 1, 11, 0, 0)],
            [datetime(2025, 1, 2, 12, 0, 0)]
        ]
    },
    {
        'id': 2,
        'matrix': [[10, 20]],  # single inner list
        'string_matrix': [['x', 'y', 'z']],
        'timestamp_matrix': [[datetime(2025, 6, 15, 8, 30, 0)]]
    },
    {
        'id': 3,
        'matrix': [[], [100], []],  # includes empty inner lists
        'string_matrix': [[]],
        'timestamp_matrix': [[], [datetime(2025, 12, 31, 23, 59, 59)], []]
    },
    {
        'id': 4,
        'matrix': [],  # empty outer list
        'string_matrix': [],
        'timestamp_matrix': []
    },
    {
        'id': 5,
        'matrix': None,  # null outer list
        'string_matrix': None,
        'timestamp_matrix': None
    }
]

nested_list_table = pa.Table.from_pylist(nested_list_data, schema=nested_list_schema)
pq.write_table(
    nested_list_table,
    'core/src/test/resources/nested_list_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated nested_list_test.parquet:")
print("  - Schema: id, matrix: list<list<int32>>, string_matrix: list<list<string>>, timestamp_matrix: list<list<timestamp>>")
print("  - Data: 5 rows with various nested list configurations")

# 7. AddressBook example from Dremel paper / Twitter blog post
# Schema:
#   message AddressBook {
#     required string owner;
#     repeated string ownerPhoneNumbers;
#     repeated group contacts {
#       required string name;
#       optional string phoneNumber;
#     }
#   }
contact_type = pa.struct([
    ('name', pa.string(), False),  # required
    ('phoneNumber', pa.string())   # optional
])

address_book_schema = pa.schema([
    ('owner', pa.string(), False),  # required
    ('ownerPhoneNumbers', pa.list_(pa.string())),
    ('contacts', pa.list_(contact_type))
])

address_book_data = [
    # Record 1: Julien Le Dem with phone numbers and contacts
    {
        'owner': 'Julien Le Dem',
        'ownerPhoneNumbers': ['555 123 4567', '555 666 1337'],
        'contacts': [
            {'name': 'Dmitriy Ryaboy', 'phoneNumber': '555 987 6543'},
            {'name': 'Chris Aniszczyk', 'phoneNumber': None}  # phoneNumber is null
        ]
    },
    # Record 2: A. Nonymous with no phone numbers and no contacts
    {
        'owner': 'A. Nonymous',
        'ownerPhoneNumbers': [],
        'contacts': []
    }
]

address_book_table = pa.Table.from_pylist(address_book_data, schema=address_book_schema)
pq.write_table(
    address_book_table,
    'core/src/test/resources/address_book_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated address_book_test.parquet:")
print("  - Schema: AddressBook(owner, ownerPhoneNumbers: list<string>, contacts: list<Contact(name, phoneNumber)>)")
print("  - Data: Classic Dremel paper example - 2 records with varying nesting")

# 8. Triple nested list test (list<list<list<int32>>>)
triple_nested_schema = pa.schema([
    ('id', pa.int32(), False),
    ('cube', pa.list_(pa.list_(pa.list_(pa.int32()))))  # 3D array
])

triple_nested_data = [
    {
        'id': 1,
        # 2x2x2 cube: [[[1,2],[3,4]], [[5,6],[7,8]]]
        'cube': [
            [[1, 2], [3, 4]],
            [[5, 6], [7, 8]]
        ]
    },
    {
        'id': 2,
        # Irregular: [[[10]], [[20,21],[22]]]
        'cube': [
            [[10]],
            [[20, 21], [22]]
        ]
    },
    {
        'id': 3,
        # With empty inner lists: [[[]], [[100]]]
        'cube': [
            [[]],
            [[100]]
        ]
    },
    {
        'id': 4,
        # Empty outer list
        'cube': []
    },
    {
        'id': 5,
        # Null
        'cube': None
    }
]

triple_nested_table = pa.Table.from_pylist(triple_nested_data, schema=triple_nested_schema)
pq.write_table(
    triple_nested_table,
    'core/src/test/resources/triple_nested_list_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated triple_nested_list_test.parquet:")
print("  - Schema: id, cube: list<list<list<int32>>>")
print("  - Data: 5 rows with 3-level nested lists")

# 8b. List of list of struct (hardwood-hq/hardwood#283 regression fixture)
row_struct_type = pa.struct([
    ('name', pa.string()),
    ('score', pa.int32())
])

list_of_list_of_struct_schema = pa.schema([
    ('id', pa.int32(), False),
    ('matrix', pa.list_(pa.list_(row_struct_type)))
])

list_of_list_of_struct_data = [
    # Row 0: 2 outer rows x variable inner cells
    {
        'id': 1,
        'matrix': [
            [
                {'name': 'a00', 'score': 10},
                {'name': 'a01', 'score': 11}
            ],
            [
                {'name': 'a10', 'score': 20},
                {'name': 'a11', 'score': 21},
                {'name': 'a12', 'score': 22}
            ]
        ]
    },
    # Row 1: single outer row, inner list mixing struct, null-struct, and struct with
    # null field (exercises struct-null vs field-null distinction)
    {
        'id': 2,
        'matrix': [
            [
                {'name': 'b00', 'score': 30},
                None,
                {'name': None, 'score': 31}
            ]
        ]
    },
    # Row 2: outer row containing an empty inner list
    {
        'id': 3,
        'matrix': [
            []
        ]
    },
    # Row 3: empty outer list
    {
        'id': 4,
        'matrix': []
    },
    # Row 4: null outer list
    {
        'id': 5,
        'matrix': None
    }
]

list_of_list_of_struct_table = pa.Table.from_pylist(
    list_of_list_of_struct_data, schema=list_of_list_of_struct_schema)
pq.write_table(
    list_of_list_of_struct_table,
    'core/src/test/resources/list_of_list_of_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated list_of_list_of_struct_test.parquet:")
print("  - Schema: id, matrix: list<list<struct<name, score>>>")
print("  - Data: 5 rows covering populated, empty-inner, empty-outer, null-outer")

# ============================================================================
# Delta Encoding Test Files
# ============================================================================

# 9. DELTA_BINARY_PACKED encoding for INT32/INT64
delta_int_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value_i32', pa.int32(), False),
    ('value_i64', pa.int64(), False),
])

# Use sequential and patterned values to test delta encoding well
delta_int_data = {
    'id': list(range(1, 201)),  # 1 to 200 (200 values - enough to span multiple miniblocks)
    'value_i32': [i * 10 for i in range(1, 201)],  # 10, 20, 30, ... (constant delta = 10)
    'value_i64': [i * i for i in range(1, 201)],  # 1, 4, 9, 16, ... (varying deltas)
}

delta_int_table = pa.table(delta_int_data, schema=delta_int_schema)

# Force DELTA_BINARY_PACKED encoding for all integer columns
pq.write_table(
    delta_int_table,
    'core/src/test/resources/delta_binary_packed_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
    column_encoding={'id': 'DELTA_BINARY_PACKED', 'value_i32': 'DELTA_BINARY_PACKED', 'value_i64': 'DELTA_BINARY_PACKED'}
)

print("\nGenerated delta_binary_packed_test.parquet:")
print("  - Encoding: DELTA_BINARY_PACKED")
print("  - Data: 200 rows with id, value_i32, value_i64")

# 10. DELTA_BINARY_PACKED with optional columns (nulls)
delta_optional_schema = pa.schema([
    ('id', pa.int32(), False),
    ('optional_value', pa.int32(), True),  # nullable
])

delta_optional_data = {
    'id': list(range(1, 101)),  # 1 to 100
    'optional_value': [i * 5 if i % 3 != 0 else None for i in range(1, 101)],  # every 3rd value is null
}

delta_optional_table = pa.table(delta_optional_data, schema=delta_optional_schema)

pq.write_table(
    delta_optional_table,
    'core/src/test/resources/delta_binary_packed_optional_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
    column_encoding={'id': 'DELTA_BINARY_PACKED', 'optional_value': 'DELTA_BINARY_PACKED'}
)

print("\nGenerated delta_binary_packed_optional_test.parquet:")
print("  - Encoding: DELTA_BINARY_PACKED with nullable column")
print("  - Data: 100 rows, every 3rd optional_value is null")

# 11. DELTA_LENGTH_BYTE_ARRAY encoding for strings
delta_string_schema = pa.schema([
    ('id', pa.int64(), False),
    ('name', pa.string(), False),
    ('description', pa.string(), False),
])

delta_string_data = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Hello', 'World', 'Foobar', 'Test', 'Delta'],
    'description': ['Short', 'A bit longer text', 'Medium length', 'Tiny', 'Another string value'],
}

delta_string_table = pa.table(delta_string_data, schema=delta_string_schema)

pq.write_table(
    delta_string_table,
    'core/src/test/resources/delta_length_byte_array_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
    column_encoding={'name': 'DELTA_LENGTH_BYTE_ARRAY', 'description': 'DELTA_LENGTH_BYTE_ARRAY'}
)

print("\nGenerated delta_length_byte_array_test.parquet:")
print("  - Encoding: DELTA_LENGTH_BYTE_ARRAY for string columns")
print("  - Data: 5 rows with id, name, description")

# 12. DELTA_BYTE_ARRAY encoding for strings with common prefixes
delta_byte_array_schema = pa.schema([
    ('id', pa.int32(), False),
    ('prefix_strings', pa.string(), False),
    ('varying_strings', pa.string(), False),
])

# Strings with common prefixes - ideal for DELTA_BYTE_ARRAY encoding
delta_byte_array_data = {
    'id': [1, 2, 3, 4, 5, 6, 7, 8],
    # Strings that share common prefixes with previous values
    'prefix_strings': [
        'apple',
        'application',  # shares 'appl' with 'apple'
        'apply',        # shares 'appl' with 'application'
        'banana',       # no prefix shared with 'apply'
        'bandana',      # shares 'ban' with 'banana'
        'band',         # shares 'band' with 'bandana'
        'bandwidth',    # shares 'band' with 'band'
        'ban'           # shares 'ban' with 'bandwidth'
    ],
    # Strings with some common prefixes
    'varying_strings': [
        'hello',
        'world',        # no common prefix
        'wonderful',    # shares 'wo' with 'world'
        'wonder',       # shares 'wonder' with 'wonderful'
        'wander',       # shares 'w' with 'wonder'
        'wandering',    # shares 'wander' with 'wander'
        'test',         # no common prefix
        'testing'       # shares 'test' with 'test'
    ],
}

delta_byte_array_table = pa.table(delta_byte_array_data, schema=delta_byte_array_schema)

pq.write_table(
    delta_byte_array_table,
    'core/src/test/resources/delta_byte_array_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
    column_encoding={'prefix_strings': 'DELTA_BYTE_ARRAY', 'varying_strings': 'DELTA_BYTE_ARRAY'}
)

print("\nGenerated delta_byte_array_test.parquet:")
print("  - Encoding: DELTA_BYTE_ARRAY for string columns")
print("  - Data: 8 rows with id, prefix_strings, varying_strings")

# ============================================================================
# Map Test Files
# ============================================================================

# 13. Simple map test (map<string, int32>)
simple_map_schema = pa.schema([
    ('id', pa.int32(), False),
    ('name', pa.string(), False),
    ('attributes', pa.map_(pa.string(), pa.int32())),  # map<string, int32>
])

simple_map_data = [
    {
        'id': 1,
        'name': 'Alice',
        'attributes': [('age', 30), ('score', 95), ('level', 5)]
    },
    {
        'id': 2,
        'name': 'Bob',
        'attributes': [('age', 25), ('score', 88)]  # different number of entries
    },
    {
        'id': 3,
        'name': 'Charlie',
        'attributes': []  # empty map
    },
    {
        'id': 4,
        'name': 'Diana',
        'attributes': None  # null map
    },
    {
        'id': 5,
        'name': 'Eve',
        'attributes': [('single_key', 42)]  # single entry
    },
    {
        'id': 6,
        'name': 'Frank',
        # Duplicate 'dup' key. The Parquet spec mandates last-value-wins for
        # duplicate keys, so a lookup of 'dup' must surface 40, not 10.
        'attributes': [('dup', 10), ('other', 20), ('dup', 30), ('dup', 40)]
    }
]

simple_map_table = pa.Table.from_pylist(simple_map_data, schema=simple_map_schema)
pq.write_table(
    simple_map_table,
    'core/src/test/resources/simple_map_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated simple_map_test.parquet:")
print("  - Schema: id, name, attributes: map<string, int32>")
print("  - Data: 6 rows with varying map sizes including empty, null, and duplicate keys")

# 14. Map with different value types
map_types_schema = pa.schema([
    ('id', pa.int32(), False),
    ('string_map', pa.map_(pa.string(), pa.string())),    # map<string, string>
    ('int_map', pa.map_(pa.int32(), pa.int64())),         # map<int32, int64>
    ('bool_map', pa.map_(pa.string(), pa.bool_())),       # map<string, bool>
])

map_types_data = [
    {
        'id': 1,
        'string_map': [('greeting', 'hello'), ('farewell', 'goodbye')],
        'int_map': [(1, 100), (2, 200), (3, 300)],
        'bool_map': [('active', True), ('verified', False)]
    },
    {
        'id': 2,
        'string_map': [('color', 'blue')],
        'int_map': [(10, 1000)],
        'bool_map': [('enabled', True)]
    },
    {
        'id': 3,
        'string_map': [],
        'int_map': [],
        'bool_map': []
    }
]

map_types_table = pa.Table.from_pylist(map_types_data, schema=map_types_schema)
pq.write_table(
    map_types_table,
    'core/src/test/resources/map_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated map_types_test.parquet:")
print("  - Schema: id, string_map: map<string,string>, int_map: map<int32,int64>, bool_map: map<string,bool>")
print("  - Data: 3 rows with different map value types")

# 15. Map of maps test (map<string, map<string, int32>>)
map_of_maps_schema = pa.schema([
    ('id', pa.int32(), False),
    ('name', pa.string(), False),
    ('nested_map', pa.map_(pa.string(), pa.map_(pa.string(), pa.int32()))),  # map<string, map<string, int32>>
])

map_of_maps_data = [
    {
        'id': 1,
        'name': 'Department A',
        'nested_map': [
            ('team1', [('alice', 100), ('bob', 95)]),
            ('team2', [('charlie', 88), ('diana', 92), ('eve', 90)])
        ]
    },
    {
        'id': 2,
        'name': 'Department B',
        'nested_map': [
            ('solo_team', [('frank', 75)])
        ]
    },
    {
        'id': 3,
        'name': 'Department C',
        'nested_map': [
            ('empty_team', [])  # inner map is empty
        ]
    },
    {
        'id': 4,
        'name': 'Department D',
        'nested_map': []  # outer map is empty
    },
    {
        'id': 5,
        'name': 'Department E',
        'nested_map': None  # null map
    }
]

map_of_maps_table = pa.Table.from_pylist(map_of_maps_data, schema=map_of_maps_schema)
pq.write_table(
    map_of_maps_table,
    'core/src/test/resources/map_of_maps_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated map_of_maps_test.parquet:")
print("  - Schema: id, name, nested_map: map<string, map<string, int32>>")
print("  - Data: 5 rows with nested maps including empty and null cases")

# 16. List of maps test (list<map<string, int32>>)
list_of_maps_schema = pa.schema([
    ('id', pa.int32(), False),
    ('map_list', pa.list_(pa.map_(pa.string(), pa.int32()))),  # list<map<string, int32>>
])

list_of_maps_data = [
    {
        'id': 1,
        'map_list': [
            [('a', 1), ('b', 2)],
            [('c', 3)],
            [('d', 4), ('e', 5), ('f', 6)]
        ]
    },
    {
        'id': 2,
        'map_list': [
            [('single', 100)]
        ]
    },
    {
        'id': 3,
        'map_list': [
            []  # empty map in list
        ]
    },
    {
        'id': 4,
        'map_list': []  # empty list
    },
    {
        'id': 5,
        'map_list': None  # null list
    }
]

list_of_maps_table = pa.Table.from_pylist(list_of_maps_data, schema=list_of_maps_schema)
pq.write_table(
    list_of_maps_table,
    'core/src/test/resources/list_of_maps_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated list_of_maps_test.parquet:")
print("  - Schema: id, map_list: list<map<string, int32>>")
print("  - Data: 5 rows with lists of maps")

# 17. Map with struct values (map<string, struct>)
person_type = pa.struct([
    ('name', pa.string()),
    ('age', pa.int32())
])

map_struct_value_schema = pa.schema([
    ('id', pa.int32(), False),
    ('people', pa.map_(pa.string(), person_type)),  # map<string, Person>
])

map_struct_value_data = [
    {
        'id': 1,
        'people': [
            ('employee1', {'name': 'Alice', 'age': 30}),
            ('employee2', {'name': 'Bob', 'age': 25})
        ]
    },
    {
        'id': 2,
        'people': [
            ('manager', {'name': 'Charlie', 'age': 45})
        ]
    },
    {
        'id': 3,
        'people': []
    }
]

map_struct_value_table = pa.Table.from_pylist(map_struct_value_data, schema=map_struct_value_schema)
pq.write_table(
    map_struct_value_table,
    'core/src/test/resources/map_struct_value_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated map_struct_value_test.parquet:")
print("  - Schema: id, people: map<string, Person(name, age)>")
print("  - Data: 3 rows with maps containing struct values")

# 17b. Map value containing a list (hardwood-hq/hardwood#293 regression fixture).
# Exercises the key/value row alignment when the value column has more rep levels
# than the key column (value column emits one record per inner list element plus
# one marker for empty/null inner lists).
scored_value_type = pa.struct([
    ('scores', pa.list_(pa.int32()))
])

map_with_list_value_schema = pa.schema([
    ('id', pa.int32(), False),
    ('entries', pa.map_(pa.string(), scored_value_type))
])

map_with_list_value_data = [
    # Row 0: single entry, non-empty list
    {
        'id': 1,
        'entries': [
            ('a', {'scores': [10, 20]})
        ]
    },
    # Row 1: four entries covering non-empty, empty, null-struct, null-list
    {
        'id': 2,
        'entries': [
            ('b', {'scores': [30, 40, 50]}),
            ('c', {'scores': []}),
            ('d', None),
            ('e', {'scores': None})
        ]
    },
    # Row 2: empty map
    {
        'id': 3,
        'entries': []
    }
]

map_with_list_value_table = pa.Table.from_pylist(
    map_with_list_value_data, schema=map_with_list_value_schema)
pq.write_table(
    map_with_list_value_table,
    'core/src/test/resources/map_with_list_value_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated map_with_list_value_test.parquet:")
print("  - Schema: id, entries: map<string, struct<scores: list<int32>>>")
print("  - Data: 3 rows covering non-empty/empty/null value-struct/null-inner-list")

# ============================================================================
# Primitive Types Test Files (for index-based accessor testing)
# ============================================================================

# 18. All primitive types in one file (for testing index-based accessors)
primitive_types_schema = pa.schema([
    ('int_col', pa.int32(), False),
    ('long_col', pa.int64(), False),
    ('float_col', pa.float32(), False),
    ('double_col', pa.float64(), False),
    ('bool_col', pa.bool_(), False),
    ('string_col', pa.string(), False),
    ('binary_col', pa.binary(), False),
])

primitive_types_data = {
    'int_col': [1, 2, 3],
    'long_col': [100, 200, 300],
    'float_col': [1.5, 2.5, 3.5],
    'double_col': [10.5, 20.5, 30.5],
    'bool_col': [True, False, True],
    'string_col': ['hello', 'world', 'test'],
    'binary_col': [b'\x00\x01\x02', b'\x03\x04\x05', b'\x06\x07\x08'],
}

primitive_types_table = pa.table(primitive_types_data, schema=primitive_types_schema)
pq.write_table(
    primitive_types_table,
    'core/src/test/resources/primitive_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated primitive_types_test.parquet:")
print("  - Schema: int_col, long_col, float_col, double_col, bool_col, string_col, binary_col")
print("  - Data: 3 rows with all primitive types for index-based accessor testing")

# 19. Lists of primitive types (int, long, double) for testing getListOfLongs/Doubles
primitive_lists_schema = pa.schema([
    ('id', pa.int32(), False),
    ('int_list', pa.list_(pa.int32())),
    ('long_list', pa.list_(pa.int64())),
    ('double_list', pa.list_(pa.float64())),
])

primitive_lists_data = [
    {
        'id': 1,
        'int_list': [1, 2, 3],
        'long_list': [100, 200, 300],
        'double_list': [1.1, 2.2, 3.3],
    },
    {
        'id': 2,
        'int_list': [10, 20],
        'long_list': [1000],
        'double_list': [10.5, 20.5],
    },
    {
        'id': 3,
        'int_list': [],
        'long_list': [1, 2, 3, 4, 5],
        'double_list': [],
    },
    {
        'id': 4,
        'int_list': None,
        'long_list': None,
        'double_list': None,
    }
]

primitive_lists_table = pa.Table.from_pylist(primitive_lists_data, schema=primitive_lists_schema)
pq.write_table(
    primitive_lists_table,
    'core/src/test/resources/primitive_lists_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated primitive_lists_test.parquet:")
print("  - Schema: id, int_list: list<int32>, long_list: list<int64>, double_list: list<float64>")
print("  - Data: 4 rows with primitive lists including empty and null cases")

# ============================================================================
# GZIP Compressed Test File (for libdeflate integration testing)
# ============================================================================

# 20. GZIP compressed file for testing libdeflate decompression
gzip_test_schema = pa.schema([
    ('id', pa.int32(), False),
    ('name', pa.string(), False),
    ('value', pa.int64(), False),
])

gzip_test_data = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'value': [100, 200, 300, 400, 500],
}

gzip_test_table = pa.table(gzip_test_data, schema=gzip_test_schema)
pq.write_table(
    gzip_test_table,
    'integration-test/src/test/resources/gzip_compressed.parquet',
    use_dictionary=False,
    compression='gzip',
    data_page_version='1.0'
)

print("\nGenerated gzip_compressed.parquet (in integration-test):")
print("  - Encoding: PLAIN")
print("  - Compression: GZIP")
print("  - Data: 5 rows with id, name, value")

# ============================================================================
# Page Index Test File (for Offset Index support testing)
# ============================================================================

# 21. File with page index enabled and small page size to produce multiple data pages
page_index_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), False),
    ('category', pa.string(), False),
])

# Generate enough rows so that with a small page size we get multiple data pages
num_rows = 10000
page_index_data = {
    'id': list(range(1, num_rows + 1)),
    'value': [i * 7 for i in range(1, num_rows + 1)],
    'category': [f'cat_{i % 50}' for i in range(1, num_rows + 1)],
}

page_index_table = pa.table(page_index_data, schema=page_index_schema)

pq.write_table(
    page_index_table,
    'core/src/test/resources/page_index_test.parquet',
    use_dictionary=['category'],
    compression=None,
    data_page_version='1.0',
    data_page_size=4096,  # Small page size to produce many pages
    write_page_index=True
)

print("\nGenerated page_index_test.parquet:")
print("  - Encoding: PLAIN for id/value, DICTIONARY for category")
print("  - Compression: UNCOMPRESSED")
print(f"  - Data: {num_rows} rows with small page size (4096) and page index enabled")

# ============================================================================
# CRC Checksum Test Files
# ============================================================================

# 22. Plain encoding with CRC page checksums enabled
pq.write_table(simple_table, 'core/src/test/resources/plain_with_crc.parquet',
               write_page_checksum=True,
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("\nGenerated plain_with_crc.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - CRC: Enabled (write_page_checksum=True)")
print("  - Data: id=[1,2,3], value=[100,200,300] - NO NULLS")

# 23. Dictionary encoding with CRC page checksums enabled
pq.write_table(table_dict, 'core/src/test/resources/dictionary_with_crc.parquet',
               write_page_checksum=True,
               use_dictionary=['category'],
               compression=None,
               data_page_version='1.0')

print("\nGenerated dictionary_with_crc.parquet:")
print("  - Encoding: DICTIONARY for category column")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - CRC: Enabled (write_page_checksum=True)")
print("  - Data: id=[1,2,3,4,5], category=['A','B','A','C','B']")

# ============================================================================
# Predicate Push-Down Test Files (multi-row-group with known statistics)
# ============================================================================

# 22. Multi-row-group file with INT64 column for predicate push-down testing
# RG1: id 1-100, value 1-100
# RG2: id 101-200, value 101-200
# RG3: id 201-300, value 201-300
filter_int_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), False),
    ('label', pa.string(), False),
])

rg1 = pa.table({
    'id': list(range(1, 101)),
    'value': list(range(1, 101)),
    'label': [f'rg1_{i}' for i in range(1, 101)],
}, schema=filter_int_schema)

rg2 = pa.table({
    'id': list(range(101, 201)),
    'value': list(range(101, 201)),
    'label': [f'rg2_{i}' for i in range(101, 201)],
}, schema=filter_int_schema)

rg3 = pa.table({
    'id': list(range(201, 301)),
    'value': list(range(201, 301)),
    'label': [f'rg3_{i}' for i in range(201, 301)],
}, schema=filter_int_schema)

combined = pa.concat_tables([rg1, rg2, rg3])

# Write with max_rows_per_row_group=100 to create 3 row groups
writer = pq.ParquetWriter(
    'core/src/test/resources/filter_pushdown_int.parquet',
    schema=filter_int_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    write_statistics=True,
)
writer.write_table(rg1)
writer.write_table(rg2)
writer.write_table(rg3)
writer.close()

print("\nGenerated filter_pushdown_int.parquet:")
print("  - 3 row groups: RG1 id/value 1-100, RG2 101-200, RG3 201-300")
print("  - Statistics enabled for predicate push-down testing")

# 23. Multi-row-group file with mixed types for predicate push-down testing
filter_mixed_schema = pa.schema([
    ('id', pa.int32(), False),
    ('price', pa.float64(), False),
    ('rating', pa.float32(), False),
    ('name', pa.string(), False),
    ('active', pa.bool_(), False),
])

mixed_rg1 = pa.table({
    'id': [1, 2, 3, 4, 5],
    'price': [10.0, 20.0, 30.0, 40.0, 50.0],
    'rating': [1.0, 2.0, 3.0, 4.0, 5.0],
    'name': ['apple', 'banana', 'cherry', 'date', 'elderberry'],
    'active': [True, True, True, True, True],
}, schema=filter_mixed_schema)

mixed_rg2 = pa.table({
    'id': [6, 7, 8, 9, 10],
    'price': [60.0, 70.0, 80.0, 90.0, 100.0],
    'rating': [6.0, 7.0, 8.0, 9.0, 10.0],
    'name': ['fig', 'grape', 'honeydew', 'imbe', 'jackfruit'],
    'active': [False, False, False, False, False],
}, schema=filter_mixed_schema)

mixed_rg3 = pa.table({
    'id': [11, 12, 13, 14, 15],
    'price': [110.0, 120.0, 130.0, 140.0, 150.0],
    'rating': [1.5, 2.5, 3.5, 4.5, 5.5],
    'name': ['kiwi', 'lemon', 'mango', 'nectarine', 'orange'],
    'active': [True, False, True, False, True],
}, schema=filter_mixed_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/filter_pushdown_mixed.parquet',
    schema=filter_mixed_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    write_statistics=True,
)
writer.write_table(mixed_rg1)
writer.write_table(mixed_rg2)
writer.write_table(mixed_rg3)
writer.close()

print("\nGenerated filter_pushdown_mixed.parquet:")
print("  - 3 row groups with mixed types (int32, float64, float32, string, bool)")
print("  - RG1: id 1-5, RG2: id 6-10, RG3: id 11-15")

# 24. Multi-row-group file with repeated (list) columns for predicate push-down testing
filter_list_schema = pa.schema([
    ('id', pa.int32(), False),
    ('scores', pa.list_(pa.int32())),
])

list_rg1 = pa.table({
    'id': [1, 2, 3],
    'scores': [[10, 20, 30], [5, 15], [25]],
}, schema=filter_list_schema)
# scores leaf values: 5..30

list_rg2 = pa.table({
    'id': [4, 5, 6],
    'scores': [[100, 200], [150], [110, 190]],
}, schema=filter_list_schema)
# scores leaf values: 100..200

list_rg3 = pa.table({
    'id': [7, 8, 9],
    'scores': [[300], [400, 500], [350, 450]],
}, schema=filter_list_schema)
# scores leaf values: 300..500

writer = pq.ParquetWriter(
    'core/src/test/resources/filter_pushdown_list.parquet',
    schema=filter_list_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    write_statistics=True,
)
writer.write_table(list_rg1)
writer.write_table(list_rg2)
writer.write_table(list_rg3)
writer.close()

print("\nGenerated filter_pushdown_list.parquet:")
print("  - 3 row groups with list<int32> column")
print("  - RG0: scores 5-30, RG1: scores 100-200, RG2: scores 300-500")

# 25. Multi-row-group file with nested struct columns for predicate push-down testing
filter_nested_schema = pa.schema([
    ('id', pa.int32(), False),
    ('address', pa.struct([
        ('city', pa.string()),
        ('zip', pa.int32()),
    ])),
])

nested_rg1 = pa.table({
    'id': [1, 2, 3],
    'address': [
        {'city': 'Austin', 'zip': 70000},
        {'city': 'Boston', 'zip': 71000},
        {'city': 'Chicago', 'zip': 72000},
    ],
}, schema=filter_nested_schema)
# city: Austin..Chicago, zip: 70000..72000

nested_rg2 = pa.table({
    'id': [4, 5, 6],
    'address': [
        {'city': 'Denver', 'zip': 80000},
        {'city': 'Eugene', 'zip': 81000},
        {'city': 'Fresno', 'zip': 82000},
    ],
}, schema=filter_nested_schema)
# city: Denver..Fresno, zip: 80000..82000

nested_rg3 = pa.table({
    'id': [7, 8, 9],
    'address': [
        {'city': 'Gary', 'zip': 90000},
        {'city': 'Houston', 'zip': 91000},
        {'city': 'Irvine', 'zip': 92000},
    ],
}, schema=filter_nested_schema)
# city: Gary..Irvine, zip: 90000..92000

writer = pq.ParquetWriter(
    'core/src/test/resources/filter_pushdown_nested.parquet',
    schema=filter_nested_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    write_statistics=True,
)
writer.write_table(nested_rg1)
writer.write_table(nested_rg2)
writer.write_table(nested_rg3)
writer.close()

print("\nGenerated filter_pushdown_nested.parquet:")
print("  - 3 row groups with struct column (address: {city, zip})")
print("  - RG0: zip 70000-72000, RG1: zip 80000-82000, RG2: zip 90000-92000")

# ===== Key-value metadata test file =====

kv_schema = pa.schema([
    ('id', pa.int64(), False),
    ('name', pa.string(), True)
])

kv_table = pa.table({
    'id': [1, 2, 3],
    'name': ['alice', 'bob', 'charlie']
}, schema=kv_schema)

kv_metadata = {
    b'app.version': b'1.2.3',
    b'writer.tool': b'hardwood-test',
    b'empty.value': b'',
}
kv_schema_with_meta = kv_schema.with_metadata(kv_metadata)
kv_table = kv_table.cast(kv_schema_with_meta)

pq.write_table(kv_table, 'core/src/test/resources/kv_metadata_test.parquet',
               use_dictionary=False,
               compression=None,
               data_page_version='1.0')

print("\nGenerated kv_metadata_test.parquet:")
print("  - Custom key-value metadata: app.version=1.2.3, writer.tool=hardwood-test, empty.value=''")
print("  - Also contains ARROW:schema metadata from pyarrow")

# ===== Column-level key-value metadata test file =====
# pyarrow doesn't write Thrift-level ColumnMetaData key_value_metadata (field 8),
# so we generate a base file and patch the Thrift footer manually.

import struct

def _write_varint(val):
    val = val & 0xFFFFFFFFFFFFFFFF
    buf = bytearray()
    while val > 0x7F:
        buf.append((val & 0x7F) | 0x80)
        val >>= 7
    buf.append(val & 0x7F)
    return bytes(buf)

def _write_zigzag(val):
    return _write_varint((val << 1) ^ (val >> 63))

_STOP = b'\x00'
_T_I32, _T_I64, _T_BIN, _T_LIST, _T_STRUCT, _T_DOUBLE = 0x05, 0x06, 0x08, 0x09, 0x0C, 0x07

class _ThriftWriter:
    def __init__(self):
        self.buf = bytearray()
        self.lf = 0
    def f(self, fid, tid):
        d = fid - self.lf
        self.buf += bytes([(d << 4) | tid]) if 0 < d <= 15 else (bytes([tid]) + _write_zigzag(fid))
        self.lf = fid
        return self
    def i32(self, v):  self.buf += _write_zigzag(v); return self
    def i64(self, v):  self.buf += _write_zigzag(v); return self
    def double(self, v):
        self.buf += struct.pack('<d', v)
        return self
    def s(self, v):    b = v.encode(); self.buf += _write_varint(len(b)) + b; return self
    def lst(self, t, n):
        self.buf += bytes([(n << 4) | t]) if n < 15 else (bytes([0xF0 | t]) + _write_varint(n))
        return self
    def raw(self, d):  self.buf += d; return self
    def end(self):     self.buf += _STOP; return self
    def out(self):     return bytes(self.buf)

def _kv_struct(k, v):
    w = _ThriftWriter(); w.f(1,_T_BIN).s(k).f(2,_T_BIN).s(v).end(); return w.out()

# SchemaElement fields: 1=type, 3=repetition_type, 4=name, 5=num_children, 6=converted_type, 10=logical_type
def _schema_elem(name, type_val=None, rep=None, num_children=None, ct=None, logical_type=None):
    w = _ThriftWriter()
    if type_val is not None: w.f(1, _T_I32).i32(type_val)
    if rep is not None:      w.f(3, _T_I32).i32(rep)
    w.f(4, _T_BIN).s(name)
    if num_children is not None: w.f(5, _T_I32).i32(num_children)
    if ct is not None:       w.f(6, _T_I32).i32(ct)
    if logical_type is not None: w.f(10, _T_STRUCT).raw(logical_type)
    w.end()
    return w.out()

# Generate base file to get data pages
col_kv_schema = pa.schema([pa.field('id', pa.int64(), False), pa.field('name', pa.string(), True)])
col_kv_table = pa.table({'id': [1, 2, 3], 'name': ['alice', 'bob', 'charlie']}, schema=col_kv_schema)
base_path = '/tmp/_col_kv_base.parquet'
pq.write_table(col_kv_table, base_path, use_dictionary=False, compression=None, data_page_version='1.0')

with open(base_path, 'rb') as f:
    base_data = f.read()
base_footer_len = struct.unpack('<I', base_data[-8:-4])[0]
pre_footer = base_data[:len(base_data) - 8 - base_footer_len]
base_pf = pq.ParquetFile(base_path)
base_rg = base_pf.metadata.row_group(0)

# Build FileMetaData with column-level kv metadata
fm = _ThriftWriter()
fm.f(1, _T_I32).i32(2)
fm.f(2, _T_LIST).lst(_T_STRUCT, 3)
fm.raw(_schema_elem("schema", num_children=2))
fm.raw(_schema_elem("id", type_val=2, rep=0))
fm.raw(_schema_elem("name", type_val=6, rep=1, ct=0))
fm.f(3, _T_I64).i64(3)
fm.f(4, _T_LIST).lst(_T_STRUCT, 1)

rw = _ThriftWriter()
rw.f(1, _T_LIST).lst(_T_STRUCT, 2)

column_kv_metadata = [
    [("col.origin", "primary-key")],
    [("col.encoding", "utf-8"), ("col.source", "user-input")],
]
enc_map = {'PLAIN': 0, 'RLE': 3, 'RLE_DICTIONARY': 8}

for ci in range(2):
    col = base_rg.column(ci)
    pt = 2 if ci == 0 else 6
    encs = [enc_map.get(str(e), 0) for e in col.encodings]

    cc = _ThriftWriter()
    cc.f(2, _T_I64).i64(col.file_offset)
    cc.f(3, _T_STRUCT)

    md = _ThriftWriter()
    md.f(1, _T_I32).i32(pt)
    md.f(2, _T_LIST).lst(_T_I32, len(encs))
    for e in encs: md.i32(e)
    md.f(3, _T_LIST).lst(_T_BIN, 1).s(col.path_in_schema)
    md.f(4, _T_I32).i32(0)
    md.f(5, _T_I64).i64(col.num_values)
    md.f(6, _T_I64).i64(col.total_uncompressed_size)
    md.f(7, _T_I64).i64(col.total_compressed_size)
    md.f(8, _T_LIST).lst(_T_STRUCT, len(column_kv_metadata[ci]))
    for k, v in column_kv_metadata[ci]: md.raw(_kv_struct(k, v))
    md.f(9, _T_I64).i64(col.data_page_offset)
    md.end()

    cc.raw(md.out()).end()
    rw.raw(cc.out())

rw.f(2, _T_I64).i64(base_rg.total_byte_size)
rw.f(3, _T_I64).i64(base_rg.num_rows)
rw.end()
fm.raw(rw.out())
fm.f(6, _T_BIN).s("hardwood-test-datagen")
fm.end()

footer_bytes = fm.out()
output = pre_footer + footer_bytes + struct.pack('<I', len(footer_bytes)) + b'PAR1'
with open('core/src/test/resources/column_kv_metadata_test.parquet', 'wb') as f:
    f.write(output)

print("\nGenerated column_kv_metadata_test.parquet:")
print("  - Column-level kv metadata on ColumnMetaData (Thrift field 8)")
print("  - id column: col.origin=primary-key")
print("  - name column: col.encoding=utf-8, col.source=user-input")

# ============================================================================
# Page-Level Column Index Push-Down Test File (Parquet v2)
# ============================================================================

# 27. Parquet v2 file with Column Index for page-level predicate pushdown testing
# Single row group, 10000 rows sorted by id, tiny pages to create ~10 pages with
# non-overlapping min/max ranges. Uses data_page_version='2.0' so PyArrow writes
# Column Index and Offset Index.
column_index_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), False),
])

column_index_table = pa.table({
    'id': list(range(0, 10000)),
    'value': list(range(1000, 11000)),
}, schema=column_index_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/column_index_pushdown.parquet',
    schema=column_index_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='2.0',
    data_page_size=128,
    write_statistics=True,
    write_page_index=True,
)
writer.write_table(column_index_table)
writer.close()

print("\nGenerated column_index_pushdown.parquet:")
print("  - 1 row group, 10000 rows, sorted id [0,9999] and value [1000,10999]")
print("  - Parquet v2 with Column Index and Offset Index")
print("  - ~10 pages of 1024 values each")

# 28. Same as above but with dictionary encoding, to test page-range I/O with dictionary pages
column_index_dict_schema = pa.schema([
    ('id', pa.int64(), False),
    ('category', pa.string(), False),
])

column_index_dict_table = pa.table({
    'id': list(range(0, 10000)),
    'category': [f'cat_{i % 10}' for i in range(10000)],
}, schema=column_index_dict_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/column_index_pushdown_dict.parquet',
    schema=column_index_dict_schema,
    use_dictionary=True,
    compression='NONE',
    data_page_version='2.0',
    data_page_size=128,
    write_statistics=True,
    write_page_index=True,
)
writer.write_table(column_index_dict_table)
writer.close()

print("\nGenerated column_index_pushdown_dict.parquet:")
print("  - 1 row group, 10000 rows, sorted id [0,9999], category with 10 distinct values")
print("  - Dictionary encoding enabled for both columns")
print("  - Parquet v2 with Column Index and Offset Index")

# Multi-row-group file with nested struct containing timestamp for predicate push-down testing
filter_nested_ts_schema = pa.schema([
    ('id', pa.int32(), False),
    ('event', pa.struct([
        ('ts', pa.timestamp('us', tz='UTC')),
        ('label', pa.string()),
    ])),
])

nested_ts_rg1 = pa.table({
    'id': [1, 2, 3],
    'event': [
        {'ts': datetime(2024, 1, 10, tzinfo=timezone.utc), 'label': 'a'},
        {'ts': datetime(2024, 1, 15, tzinfo=timezone.utc), 'label': 'b'},
        {'ts': datetime(2024, 1, 20, tzinfo=timezone.utc), 'label': 'c'},
    ],
}, schema=filter_nested_ts_schema)
# ts: 2024-01-10 .. 2024-01-20

nested_ts_rg2 = pa.table({
    'id': [4, 5, 6],
    'event': [
        {'ts': datetime(2024, 6, 1, tzinfo=timezone.utc), 'label': 'd'},
        {'ts': datetime(2024, 6, 15, tzinfo=timezone.utc), 'label': 'e'},
        {'ts': datetime(2024, 6, 30, tzinfo=timezone.utc), 'label': 'f'},
    ],
}, schema=filter_nested_ts_schema)
# ts: 2024-06-01 .. 2024-06-30

nested_ts_rg3 = pa.table({
    'id': [7, 8, 9],
    'event': [
        {'ts': datetime(2024, 12, 1, tzinfo=timezone.utc), 'label': 'g'},
        {'ts': datetime(2024, 12, 15, tzinfo=timezone.utc), 'label': 'h'},
        {'ts': datetime(2024, 12, 25, tzinfo=timezone.utc), 'label': 'i'},
    ],
}, schema=filter_nested_ts_schema)
# ts: 2024-12-01 .. 2024-12-25

writer = pq.ParquetWriter(
    'core/src/test/resources/filter_pushdown_nested_ts.parquet',
    schema=filter_nested_ts_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    write_statistics=True,
)
writer.write_table(nested_ts_rg1)
writer.write_table(nested_ts_rg2)
writer.write_table(nested_ts_rg3)
writer.close()

print("\nGenerated filter_pushdown_nested_ts.parquet:")
print("  - 3 row groups with struct column (event: {ts: timestamp[us, UTC], label: string})")
print("  - RG0: Jan 2024, RG1: Jun 2024, RG2: Dec 2024")

# =====================================================================
# Schema compatibility test files for issue #202
# These files share column names and physical types but differ in
# logical types, repetition types, or decimal parameters.
# =====================================================================

# --- Timestamp with MICROS ---
ts_micros_schema = pa.schema([
    ('id', pa.int64(), False),
    ('ts', pa.timestamp('us', tz='UTC'), False),
])
ts_micros_table = pa.table({
    'id': [1, 2],
    'ts': pa.array([
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 6, 1, tzinfo=timezone.utc),
    ], type=pa.timestamp('us', tz='UTC')),
}, schema=ts_micros_schema)
pq.write_table(ts_micros_table, 'core/src/test/resources/compat_ts_micros.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("\nGenerated compat_ts_micros.parquet (timestamp micros UTC)")

# --- Timestamp with MILLIS ---
ts_millis_schema = pa.schema([
    ('id', pa.int64(), False),
    ('ts', pa.timestamp('ms', tz='UTC'), False),
])
ts_millis_table = pa.table({
    'id': [3, 4],
    'ts': pa.array([
        datetime(2024, 7, 1, tzinfo=timezone.utc),
        datetime(2024, 12, 1, tzinfo=timezone.utc),
    ], type=pa.timestamp('ms', tz='UTC')),
}, schema=ts_millis_schema)
pq.write_table(ts_millis_table, 'core/src/test/resources/compat_ts_millis.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_ts_millis.parquet (timestamp millis UTC)")

# --- Decimal(10, 2) stored as FIXED_LEN_BYTE_ARRAY ---
dec_10_2_schema = pa.schema([
    ('id', pa.int64(), False),
    ('amount', pa.decimal128(10, 2), False),
])
dec_10_2_table = pa.table({
    'id': [1, 2],
    'amount': pa.array([Decimal('123.45'), Decimal('678.90')], type=pa.decimal128(10, 2)),
}, schema=dec_10_2_schema)
pq.write_table(dec_10_2_table, 'core/src/test/resources/compat_decimal_10_2.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_decimal_10_2.parquet (decimal(10,2))")

# --- Decimal(10, 4) stored as FIXED_LEN_BYTE_ARRAY (same precision, different scale) ---
dec_10_4_schema = pa.schema([
    ('id', pa.int64(), False),
    ('amount', pa.decimal128(10, 4), False),
])
dec_10_4_table = pa.table({
    'id': [3, 4],
    'amount': pa.array([Decimal('123.4500'), Decimal('678.9000')], type=pa.decimal128(10, 4)),
}, schema=dec_10_4_schema)
pq.write_table(dec_10_4_table, 'core/src/test/resources/compat_decimal_10_4.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_decimal_10_4.parquet (decimal(10,4))")

# --- REQUIRED id + REQUIRED value ---
required_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), False),
])
required_table = pa.table({
    'id': [1, 2],
    'value': [100, 200],
}, schema=required_schema)
pq.write_table(required_table, 'core/src/test/resources/compat_required.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_required.parquet (all REQUIRED)")

# --- REQUIRED id + OPTIONAL value ---
optional_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), True),
])
optional_table = pa.table({
    'id': [3, 4],
    'value': [300, None],
}, schema=optional_schema)
pq.write_table(optional_table, 'core/src/test/resources/compat_optional_value.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_optional_value.parquet (value is OPTIONAL)")

# --- INT64 with no logical type (plain int) ---
plain_int_schema = pa.schema([
    ('id', pa.int64(), False),
    ('ts', pa.int64(), False),
])
plain_int_table = pa.table({
    'id': [1, 2],
    'ts': [1000, 2000],
}, schema=plain_int_schema)
pq.write_table(plain_int_table, 'core/src/test/resources/compat_plain_int64.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("Generated compat_plain_int64.parquet (ts is plain INT64, no logical type)")

# Nullable primitives test: all primitive types with null values
nullable_primitives_schema = pa.schema([
    ('id', pa.int32(), False),           # REQUIRED - row identifier
    ('nullable_int', pa.int32(), True),   # OPTIONAL
    ('nullable_long', pa.int64(), True),  # OPTIONAL
    ('nullable_float', pa.float32(), True),  # OPTIONAL
    ('nullable_double', pa.float64(), True), # OPTIONAL
    ('nullable_bool', pa.bool_(), True),     # OPTIONAL
])

nullable_primitives_table = pa.table({
    'id': [1, 2, 3, 4],
    'nullable_int': [10, None, 30, None],
    'nullable_long': [100, None, 300, None],
    'nullable_float': [1.5, None, 3.5, None],
    'nullable_double': [10.5, None, 30.5, None],
    'nullable_bool': [True, None, False, None],
}, schema=nullable_primitives_schema)

pq.write_table(nullable_primitives_table, 'core/src/test/resources/nullable_primitives_test.parquet',
               use_dictionary=False, compression=None, data_page_version='1.0')
print("\nGenerated nullable_primitives_test.parquet:")
print("  - Data: 4 rows, rows 2 and 4 have all nullable columns as null")
print("  - Types: int32, int64, float32, float64, bool")

# Unsigned int test file
unsigned_int_schema = pa.schema([
    ('id', pa.uint32(), False),
    ('uint32_val', pa.uint32(), False),
    ('uint64_val', pa.uint64(), False),
])

unsigned_int_data = {
    'id': [1, 2, 3],
    'uint32_val': [0, 2147483647, 4294967295],
    'uint64_val': [0, 9223372036854775807, 18446744073709551615],
}

unsigned_int_table = pa.table(unsigned_int_data, schema=unsigned_int_schema)
pq.write_table(
    unsigned_int_table,
    'core/src/test/resources/unsigned_int_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated unsigned_int_test.parquet:")
print("  - Data: uint32_val=[0, 2147483647, 4294967295], uint64_val=[0, 9223372036854775807, 18446744073709551615]")

# ============================================================================
# Inline page statistics (no ColumnIndex) — models parquet-cpp-arrow defaults
# ============================================================================

# Files produced by parquet-cpp-arrow (and older pyarrow defaults) emit
# per-page Statistics inline in DataPageHeader but omit the out-of-band
# ColumnIndex / OffsetIndex. Used as a fixture for the inline-stats
# pushdown fallback.
inline_stats_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), True),   # nullable → optional, maxDefLevel=1 (exercises null-placeholder skip)
])

inline_stats_table = pa.table({
    'id': list(range(0, 10000)),
    'value': list(range(1000, 11000)),
}, schema=inline_stats_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/inline_page_stats.parquet',
    schema=inline_stats_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    data_page_size=1024,
    write_statistics=True,
    write_page_index=False,
)
writer.write_table(inline_stats_table)
writer.close()

print("\nGenerated inline_page_stats.parquet:")
print("  - 1 row group, 10000 rows, sorted id [0,9999] and value [1000,10999]")
print("  - Parquet v1 with inline DataPageHeader.statistics (no ColumnIndex)")


# ============================================================================
# Variant logical type
# ============================================================================
#
# ============================================================================
# Variant logical type — unshredded
# ============================================================================
#
# PyArrow 23.0.1 lacks a public `pa.variant()` writer, but the Variant column
# is just a group of two binary fields `{metadata, value}` — we write that
# structure with PyArrow, then post-process the footer to stamp the
# VARIANT(specification_version=1) annotation on the outer group. Same
# footer-rewrite pattern as parquet_bson_annotation.py.

# A minimal unshredded Variant column with a few rows covering the distinct
# cases: BOOLEAN_TRUE, BOOLEAN_FALSE, INT32, and STRING. Every row has
# `metadata` = empty dictionary (`01 00 00`) and `value` = the variant payload.

_empty_metadata = bytes([0x01, 0x00, 0x00])  # v1, unsorted, offset_size=1, dict_size=0, offset[0]=0

variant_unshredded_schema = pa.schema([
    ('id', pa.int32(), False),
    ('var', pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), False),
    ]), True),
])

variant_unshredded_table = pa.table({
    'id': [1, 2, 3, 4],
    'var': [
        {'metadata': _empty_metadata, 'value': bytes([0x04])},                            # BOOLEAN_TRUE
        {'metadata': _empty_metadata, 'value': bytes([0x08])},                            # BOOLEAN_FALSE
        {'metadata': _empty_metadata, 'value': bytes([0x14, 42, 0, 0, 0])},               # INT32 = 42
        {'metadata': _empty_metadata, 'value': bytes([0x09, ord('h'), ord('i')])},         # short string "hi" (basic_type=1, length=2)
    ],
}, schema=variant_unshredded_schema)

pq.write_table(
    variant_unshredded_table,
    'core/src/test/resources/variant_test.parquet',
    compression='NONE',
    use_dictionary=False,
    data_page_version='1.0',
)
annotate_group_as_variant('core/src/test/resources/variant_test.parquet', 'var')

print("\nGenerated variant_test.parquet:")
print("  - 4 rows: BOOLEAN_TRUE, BOOLEAN_FALSE, INT32(42), short string 'hi'")
print("  - `var` is a VARIANT-annotated group of {metadata, value} binaries")

# ============================================================================
# Variant logical type — shredded (typed_value: int64)
# ============================================================================
#
# Exercises the reassembly path: typed_value non-null → encode as Variant
# INT64; typed_value null and value non-null → pass through; both null at
# non-null group → emit Variant NULL per the parquet-java convention.

variant_shredded_schema = pa.schema([
    ('id', pa.int32(), False),
    ('var', pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), True),
        pa.field('typed_value', pa.int64(), True),
    ]), True),
])

variant_shredded_table = pa.table({
    'id': [1, 2, 3, 4],
    'var': [
        # Row 1: shredded — value null, typed_value = 42 → reassemble as INT64(42)
        {'metadata': _empty_metadata, 'value': None, 'typed_value': 42},
        # Row 2: unshredded — value carries the Variant bytes, typed_value null
        {'metadata': _empty_metadata, 'value': bytes([0x04]), 'typed_value': None},
        # Row 3: both null at non-null group → Variant NULL (0x00) per spec
        {'metadata': _empty_metadata, 'value': None, 'typed_value': None},
        # Row 4: shredded — typed_value = 1_000_000_000_000
        {'metadata': _empty_metadata, 'value': None, 'typed_value': 1_000_000_000_000},
    ],
}, schema=variant_shredded_schema)

pq.write_table(
    variant_shredded_table,
    'core/src/test/resources/variant_shredded_test.parquet',
    compression='NONE',
    use_dictionary=False,
    data_page_version='1.0',
)
annotate_group_as_variant('core/src/test/resources/variant_shredded_test.parquet', 'var')

print("\nGenerated variant_shredded_test.parquet:")
print("  - 4 rows exercising the shredded reassembly paths")
print("  - typed_value: int64 — shredded (rows 1, 4), unshredded (row 2), Variant NULL (row 3)")

# ============================================================================
# hardwood-hq/hardwood#464: VARIANT in repeated contexts (map values, list
# elements). Exercises the unshredded path through PqMap.Entry.getVariantValue,
# PqList.variants, and RowReader.getVariant(int). Shredded variants in
# repeated contexts remain deferred (tracked in #467).
# ============================================================================

variant_in_repeated_schema = pa.schema([
    ('id', pa.int32(), False),
    # Top-level variant — exercises RowReader.getVariant(int).
    ('top_var', pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), False),
    ]), True),
    # Map<String, Variant> — exercises PqMap.Entry.getVariantValue.
    ('var_map', pa.map_(pa.string(), pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), False),
    ]))),
    # List<Variant> — exercises PqList.variants().
    ('var_list', pa.list_(pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), False),
    ]))),
])

# Variant payloads reused across columns: BOOLEAN_TRUE, INT32(7), short string "hi".
_v_true = (_empty_metadata, bytes([0x04]))
_v_int7 = (_empty_metadata, bytes([0x14, 7, 0, 0, 0]))
_v_hi = (_empty_metadata, bytes([0x09, ord('h'), ord('i')]))

variant_in_repeated_table = pa.table({
    'id': [1, 2],
    'top_var': [
        {'metadata': _v_true[0], 'value': _v_true[1]},
        {'metadata': _v_int7[0], 'value': _v_int7[1]},
    ],
    'var_map': [
        [('a', {'metadata': _v_true[0], 'value': _v_true[1]}),
         ('b', {'metadata': _v_int7[0], 'value': _v_int7[1]})],
        [('c', {'metadata': _v_hi[0], 'value': _v_hi[1]})],
    ],
    'var_list': [
        [{'metadata': _v_true[0], 'value': _v_true[1]},
         {'metadata': _v_int7[0], 'value': _v_int7[1]},
         {'metadata': _v_hi[0], 'value': _v_hi[1]}],
        [{'metadata': _v_int7[0], 'value': _v_int7[1]}],
    ],
}, schema=variant_in_repeated_schema)

pq.write_table(
    variant_in_repeated_table,
    'core/src/test/resources/variant_in_repeated_test.parquet',
    compression='NONE',
    use_dictionary=False,
    data_page_version='1.0',
)
annotate_group_as_variant(
    'core/src/test/resources/variant_in_repeated_test.parquet', 'top_var')
annotate_group_at_path_as_variant(
    'core/src/test/resources/variant_in_repeated_test.parquet',
    ['var_map', 'key_value', 'value'])
annotate_group_at_path_as_variant(
    'core/src/test/resources/variant_in_repeated_test.parquet',
    ['var_list', 'list', 'element'])

print("\nGenerated variant_in_repeated_test.parquet:")
print("  - 2 rows covering top-level Variant, Map<String, Variant>, List<Variant>")
print("  - All unshredded; payloads: BOOLEAN_TRUE, INT32(7), short string 'hi'")

# ============================================================================
# Variant attributes example — EAV table backing the docs/tweet snippet
# ============================================================================
#
# Three rows of an entity-attribute-value table where the VARIANT column
# `value` carries a different shape per row: INT64, STRING, OBJECT. Backs
# VariantAttributesExampleTest (which mirrors the published code snippet).

# Metadata with empty dict (reused by rows whose value carries no object keys).
_attr_empty_metadata = bytes([0x01, 0x00, 0x00])  # v1, unsorted, offset_size=1, dict_size=0

# Metadata with dictionary ["opt_in", "theme"] (sorted by name).
#   header 0x11 = v1 + sorted + offset_size=1
#   dict_size=2, offsets=[0, 6, 11], keys="opt_in" + "theme"
_attr_object_metadata = bytes([0x11, 0x02, 0x00, 0x06, 0x0B]) + b'opt_in' + b'theme'

# INT64(42): primitive header (PRIM_INT64=6) → 0x18, then 8-byte LE payload.
_attr_value_age = bytes([0x18, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])

# Short string "ada@example.com" (15 bytes): header=(15<<2)|1=0x3D, then bytes.
_attr_value_email = bytes([0x3D]) + b'ada@example.com'

# OBJECT {"opt_in": true, "theme": "dark"}:
#   header 0x02 = BASIC_TYPE_OBJECT with value_header=0 (offset_size=1,
#   id_size=1, is_large=0); n=2; field ids [0,1] (asc by field name);
#   offsets [0,1,6]; values = BOOL_TRUE (0x04) + short-string "dark" (0x11 + 'dark')
_attr_value_prefs = bytes([
    0x02,                   # object header
    0x02,                   # num_elements = 2
    0x00, 0x01,             # field ids: opt_in=0, theme=1
    0x00, 0x01, 0x06,       # offsets: 0, 1, 6
    0x04,                   # value[0]: BOOLEAN_TRUE
    0x11, ord('d'), ord('a'), ord('r'), ord('k'),  # value[1]: short-string "dark"
])

variant_attributes_schema = pa.schema([
    ('id', pa.int64(), False),
    ('name', pa.string(), False),
    ('value', pa.struct([
        pa.field('metadata', pa.binary(), False),
        pa.field('value', pa.binary(), False),
    ]), True),
])

variant_attributes_table = pa.table({
    'id':   [1, 1, 1],
    'name': ['age', 'email', 'preferences'],
    'value': [
        {'metadata': _attr_empty_metadata,  'value': _attr_value_age},
        {'metadata': _attr_empty_metadata,  'value': _attr_value_email},
        {'metadata': _attr_object_metadata, 'value': _attr_value_prefs},
    ],
}, schema=variant_attributes_schema)

pq.write_table(
    variant_attributes_table,
    'core/src/test/resources/variant_attributes_example.parquet',
    compression='NONE',
    use_dictionary=False,
    data_page_version='1.0',
)
annotate_group_as_variant('core/src/test/resources/variant_attributes_example.parquet', 'value')

print("\nGenerated variant_attributes_example.parquet:")
print("  - EAV table (id, name, value) backing the docs/tweet Variant snippet")
print("  - 3 rows: INT64(age=42), STRING(email), OBJECT(preferences)")

# INTERVAL logical type test
# PyArrow writes pa.binary(12) as FIXED_LEN_BYTE_ARRAY(12); the annotation script then
# writes LogicalType.IntervalType (field 9 in the LogicalType union) into the footer.
def _interval_bytes(months, days, millis):
    return struct.pack('<III', months, days, millis)

interval_schema = pa.schema([
    ('id', pa.int32(), False),
    ('duration', pa.binary(12), True),
])

interval_table = pa.table({
    'id': [1, 2, 3],
    'duration': [
        _interval_bytes(1, 15, 3_600_000),   # 1 month, 15 days, 1 hour in millis
        _interval_bytes(0, 30, 0),            # 30 days
        None,                                 # null
    ],
}, schema=interval_schema)

pq.write_table(
    interval_table,
    'core/src/test/resources/interval_logical_type_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)
annotate_column_as_interval('core/src/test/resources/interval_logical_type_test.parquet', 'duration')

print("\nGenerated interval_logical_type_test.parquet:")
print("  - Schema: id INT32, duration FIXED_LEN_BYTE_ARRAY(12) annotated INTERVAL")
print("  - 3 rows: (1mo,15d,1h), (0mo,30d,0ms), null")

# Same shape as above, but annotated with the legacy `converted_type=INTERVAL` only
# (no modern LogicalType union member). Mirrors files from parquet-mr / Spark / Hive
# predating the LogicalType union; verifies the schema-builder fallback.
pq.write_table(
    interval_table,
    'core/src/test/resources/interval_legacy_converted_type_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)
annotate_column_as_interval(
    'core/src/test/resources/interval_legacy_converted_type_test.parquet',
    'duration',
    legacy_only=True)

print("\nGenerated interval_legacy_converted_type_test.parquet:")
print("  - Same data, only the legacy converted_type=INTERVAL annotation set")

# Legacy converted-type coverage (hardwood-hq/hardwood#529). Every primitive
# `converted_type` that predates the LogicalType union, written with no modern
# `logicalType` — the shape produced by older parquet-mr / Hive / Impala / Spark.
# PyArrow emits the bare physical columns (BYTE_ARRAY / INT32 / INT64); the
# annotator then stamps `converted_type` alone so the schema builder's fallback
# is exercised end-to-end. Two non-null rows per column.
legacy_ct_schema = pa.schema([
    ('utf8_col', pa.binary(), True),
    ('enum_col', pa.binary(), True),
    ('json_col', pa.binary(), True),
    ('bson_col', pa.binary(), True),
    ('date_col', pa.int32(), True),
    ('decimal_col', pa.int64(), True),
    ('time_millis_col', pa.int32(), True),
    ('time_micros_col', pa.int64(), True),
    ('timestamp_millis_col', pa.int64(), True),
    ('timestamp_micros_col', pa.int64(), True),
    ('int8_col', pa.int32(), True),
    ('int16_col', pa.int32(), True),
    ('int32_col', pa.int32(), True),
    ('int64_col', pa.int64(), True),
    ('uint8_col', pa.int32(), True),
    ('uint16_col', pa.int32(), True),
    ('uint32_col', pa.int32(), True),
    ('uint64_col', pa.int64(), True),
])

# Row 1 chosen to exercise edge values: signed-type minima, unsigned-type maxima
# stored as the raw two's-complement bit pattern (e.g. uint32 max = -1 as int32).
legacy_ct_table = pa.table({
    'utf8_col': [b'alpha', b'bravo'],
    'enum_col': [b'RED', b'GREEN'],
    'json_col': [b'{"k":1}', b'{"k":2}'],
    'bson_col': [b'\x05\x00\x00\x00\x00', b'\x05\x00\x00\x00\x00'],  # empty BSON doc
    'date_col': pa.array([0, 19723], type=pa.int32()),              # epoch, 2024-01-01
    'decimal_col': pa.array([12345, -100], type=pa.int64()),        # 123.45, -1.00 at scale 2
    'time_millis_col': pa.array([0, 3661000], type=pa.int32()),     # 00:00:00, 01:01:01
    'time_micros_col': pa.array([0, 3661000000], type=pa.int64()),
    'timestamp_millis_col': pa.array([0, 1704067200000], type=pa.int64()),       # 2024-01-01T00:00:00Z
    'timestamp_micros_col': pa.array([0, 1704067200000000], type=pa.int64()),
    'int8_col': pa.array([-128, 127], type=pa.int32()),
    'int16_col': pa.array([-32768, 32767], type=pa.int32()),
    'int32_col': pa.array([-2147483648, 2147483647], type=pa.int32()),
    'int64_col': pa.array([-100, 100], type=pa.int64()),
    'uint8_col': pa.array([0, 255], type=pa.int32()),
    'uint16_col': pa.array([0, 65535], type=pa.int32()),
    'uint32_col': pa.array([0, -1], type=pa.int32()),               # 0, 4294967295 (max uint32)
    'uint64_col': pa.array([0, -1], type=pa.int64()),               # 0, 18446744073709551615 (max uint64)
}, schema=legacy_ct_schema)

pq.write_table(
    legacy_ct_table,
    'core/src/test/resources/legacy_converted_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)
annotate_columns_as_legacy_converted_type(
    'core/src/test/resources/legacy_converted_types_test.parquet',
    {
        'utf8_col': 'UTF8',
        'enum_col': 'ENUM',
        'json_col': 'JSON',
        'bson_col': 'BSON',
        'date_col': 'DATE',
        'decimal_col': {'ct': 'DECIMAL', 'scale': 2, 'precision': 18},
        'time_millis_col': 'TIME_MILLIS',
        'time_micros_col': 'TIME_MICROS',
        'timestamp_millis_col': 'TIMESTAMP_MILLIS',
        'timestamp_micros_col': 'TIMESTAMP_MICROS',
        'int8_col': 'INT_8',
        'int16_col': 'INT_16',
        'int32_col': 'INT_32',
        'int64_col': 'INT_64',
        'uint8_col': 'UINT_8',
        'uint16_col': 'UINT_16',
        'uint32_col': 'UINT_32',
        'uint64_col': 'UINT_64',
    },
)

print("\nGenerated legacy_converted_types_test.parquet:")
print("  - 18 columns, each carrying only a legacy converted_type (no logicalType)")
print("  - Covers UTF8/ENUM/JSON/BSON, DATE, DECIMAL, TIME_*, TIMESTAMP_*, INT_*/UINT_*")

# FLOAT16 logical type test
# IEEE 754 half-precision (binary16): sign | 5-bit exponent | 10-bit mantissa,
# stored as 2-byte FIXED_LEN_BYTE_ARRAY in little-endian. PyArrow's pa.float16()
# emits the column with the FLOAT16 LogicalType union member already set, so no
# post-hoc footer annotation is needed.
float16_schema = pa.schema([
    ('id', pa.int32(), False),
    ('half', pa.float16(), True),
])

float16_table = pa.table({
    'id': pa.array([1, 2, 3, 4, 5, 6, 7], type=pa.int32()),
    'half': pa.array(
        [
            numpy.float16(0.0),
            numpy.float16(1.0),
            numpy.float16(-1.5),
            numpy.float16(65504.0),               # max finite binary16
            numpy.float16('inf'),
            numpy.float16('nan'),
            None,                                 # null
        ],
        type=pa.float16(),
    ),
}, schema=float16_schema)

pq.write_table(
    float16_table,
    'core/src/test/resources/float16_logical_type_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)

print("\nGenerated float16_logical_type_test.parquet:")
print("  - Schema: id INT32, half FIXED_LEN_BYTE_ARRAY(2) annotated FLOAT16")
print("  - 7 rows: 0, 1, -1.5, 65504 (max), +Inf, NaN, null")

# NULL logical type test (hardwood-hq/hardwood#444). PyArrow's pa.null() column
# is written as an INT32 column annotated with the NullType union member
# (LogicalType field id 11); every row is null.
null_schema = pa.schema([
    ('id', pa.int32(), False),
    ('nothing', pa.null(), True),
])
null_table = pa.table({
    'id': pa.array([1, 2, 3], type=pa.int32()),
    'nothing': pa.array([None, None, None], type=pa.null()),
}, schema=null_schema)
pq.write_table(
    null_table,
    'core/src/test/resources/null_logical_type_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)

print("\nGenerated null_logical_type_test.parquet:")
print("  - Schema: id INT32, nothing INT32 annotated NULL (all rows null)")

# hardwood-hq/hardwood#445: typed accessors for INTERVAL inside list/map and
# TIME / DECIMAL keyed maps. Each column carries data plus a logical-type
# annotation written into the inner SchemaElement (list element or map key /
# value), which PyArrow cannot emit directly.
issue_445_schema = pa.schema([
    ('id', pa.int32(), False),
    ('intervals', pa.list_(pa.binary(12))),
    ('interval_map', pa.map_(pa.string(), pa.binary(12))),
    ('time_keyed', pa.map_(pa.int32(), pa.int32())),
    ('decimal_keyed', pa.map_(pa.binary(8), pa.int32())),
    # Map<String, JSON> — exercises decoded vs raw on PqMap.Entry.getValue()
    # for a BYTE_ARRAY column carrying the JSON logical-type annotation.
    ('json_map', pa.map_(pa.string(), pa.binary())),
    # Map<String, FLOAT16> — exercises PqMap.Entry.getFloatValue() on a
    # FLBA(2) value column annotated as FLOAT16 (parity with PqStruct /
    # PqList / FlatRowReader, all of which already handle this).
    ('f16_map', pa.map_(pa.string(), pa.binary(2))),
    # Struct{ts: TIMESTAMP(MICROS), payload: JSON, i8: INT_8} — exercises
    # decoded vs raw on PqStruct.getValue() for INT64-backed, BYTE_ARRAY-backed,
    # and narrowed-integer logical types (INT_8 surfaces as `Byte` decoded but
    # `Integer` raw, matching the flat-reader path).
    ('nested_struct', pa.struct([
        ('ts', pa.timestamp('us', tz='UTC')),
        ('payload', pa.binary()),
        ('i8', pa.int8()),
    ])),
])

issue_445_table = pa.table({
    'id': [1, 2, 3],
    'intervals': [
        [_interval_bytes(1, 0, 0), _interval_bytes(0, 7, 0)],
        [_interval_bytes(2, 30, 1000)],
        # Row 2 exercises a null INTERVAL element inside a non-null list.
        [None, _interval_bytes(5, 0, 0)],
    ],
    'interval_map': [
        [('a', _interval_bytes(0, 1, 0))],
        [('b', _interval_bytes(3, 0, 0)), ('c', _interval_bytes(0, 0, 5000))],
        # Row 2 exercises a null INTERVAL value (non-null key).
        [('d', None)],
    ],
    'time_keyed': [
        [(12345, 100)],
        [(67890, 200), (11111, 300)],
        [],
    ],
    'decimal_keyed': [
        [(struct.pack('>q', 12345), 1)],
        [(struct.pack('>q', 99999), 2)],
        [],
    ],
    'json_map': [
        [('k1', b'{"a":1}')],
        [('k2', b'[true,false]'), ('k3', b'"hello"')],
        [],
    ],
    # struct.pack('<e', ...) writes IEEE 754 binary16 little-endian — the
    # encoding Parquet's FLOAT16 logical type expects.
    'f16_map': [
        [('a', struct.pack('<e', 1.5))],
        [('b', struct.pack('<e', -1.0)), ('c', struct.pack('<e', 2.0))],
        [],
    ],
    'nested_struct': [
        {'ts': pa.scalar(1, type=pa.timestamp('us', tz='UTC')).as_py(),
         'payload': b'{"answer":42}',
         'i8': 7},
        {'ts': pa.scalar(2_000_000, type=pa.timestamp('us', tz='UTC')).as_py(),
         'payload': b'[1,2,3]',
         'i8': -1},
        {'ts': pa.scalar(3_000_000, type=pa.timestamp('us', tz='UTC')).as_py(),
         'payload': b'null',
         'i8': 0},
    ],
}, schema=issue_445_schema)

pq.write_table(
    issue_445_table,
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)
annotate_element_at_path_as_interval(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['intervals', 'list', 'element'])
annotate_element_at_path_as_interval(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['interval_map', 'key_value', 'value'])
annotate_element_at_path_as_time(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['time_keyed', 'key_value', 'key'],
    unit='MILLIS')
annotate_element_at_path_as_decimal(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['decimal_keyed', 'key_value', 'key'],
    precision=18, scale=2)
annotate_element_at_path_as_json(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['json_map', 'key_value', 'value'])
annotate_element_at_path_as_json(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['nested_struct', 'payload'])
annotate_element_at_path_as_float16(
    'core/src/test/resources/typed_accessors_issue_445.parquet',
    ['f16_map', 'key_value', 'value'])

print("\nGenerated typed_accessors_issue_445.parquet:")
print("  - Fixture for hardwood#445: INTERVAL list, INTERVAL-value map,")
print("    TIME-keyed map, DECIMAL-keyed map, JSON-value map, FLOAT16-value map,")
print("    struct{ts, JSON, INT_8}.")

# old_list_structure_test.parquet
# Tests reading the pre-standard 2-level LIST encoding (see hardwood-hq/hardwood#282).
# PyArrow only writes the standard 3-level encoding, so this fixture cannot be
# generated synthetically — it is copied from the upstream apache/parquet-testing
# corpus, which the parquet-testing-runner module clones into its target/ at build
# time. Build that module first (e.g. `./mvnw -pl parquet-testing-runner package`).
_copy_if_exists(
    Path('parquet-testing-runner/target/parquet-testing/data/old_list_structure.parquet'),
    'core/src/test/resources/old_list_structure_test.parquet',
    "build the parquet-testing-runner module to populate target/parquet-testing/")

# filter_all_pages_match.parquet
# Regression for hardwood-hq/hardwood#254: when a filter matches all pages in a row
# group (the Column Index reports every page overlaps the predicate), the reader
# must still return all rows. Requires a Column Index so the filter resolves to
# a concrete page-spanning RowRanges rather than RowRanges.ALL.
filter_all_pages_table = pa.table({
    'id': pa.array(list(range(100)), type=pa.int64()),
    'value': pa.array([i * 1.5 for i in range(100)], type=pa.float64()),
})
pq.write_table(
    filter_all_pages_table,
    'core/src/test/resources/filter_all_pages_match.parquet',
    write_page_index=True,
)

print("\nGenerated filter_all_pages_match.parquet:")
print("  - Schema: id INT64, value DOUBLE; 100 rows in one row group")
print("  - Column Index enabled")

# yellow_tripdata_sample.parquet
# Five-row slice of the real NYC TLC Yellow Taxi 2025-01 release. The sample is
# *not* synthetic: YellowTripDataTest asserts the exact published values
# (VendorID, pickup/dropoff timestamps, fares, ...). The full month is
# downloaded by the performance-testing/test-data-setup module's TaxiDataDownloader
# (run via Maven on that module first); we just copy out the first 5 rows so the
# core test suite has a small fixture without a network dependency.
yellow_source = Path('performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2025-01.parquet')
yellow_destinations = [
    'core/src/test/resources/yellow_tripdata_sample.parquet',
    'integration-test/src/test/resources/yellow_tripdata_sample.parquet',
]
if yellow_source.exists():
    yellow_full = pq.read_table(yellow_source)
    yellow_sample = yellow_full.slice(0, 5)
    primary, *mirrors = yellow_destinations
    pq.write_table(yellow_sample, primary)
    for mirror in mirrors:
        shutil.copyfile(primary, mirror)
    print("\nGenerated yellow_tripdata_sample.parquet:")
    print(f"  - First 5 rows of {yellow_source.name}")
    print(f"  - Schema: {yellow_full.num_columns} columns from the upstream TLC release")
    print(f"  - Written to: {', '.join(yellow_destinations)}")
else:
    print("\nSkipping yellow_tripdata_sample.parquet:")
    print(f"  Source not found: {yellow_source}")
    print("  Hint: build performance-testing/test-data-setup so TaxiDataDownloader")
    print("        populates target/tlc-trip-record-data/yellow_tripdata_2025-01.parquet")

# LIST annotation recognition test files.
# Two variants of the same data: both annotations (PyArrow default) and modern-only
# (logicalType only, no converted_type). The modern-only variant is the regression
# case for LogicalTypeReader field ID 3.
_list_ann_base = 'core/src/test/resources/list_annotation_both_test.parquet'
_list_schema = pa.schema([
    ('id', pa.int32(), False),
    ('tags', pa.list_(pa.field('element', pa.string()))),
])
_list_data = pa.table({
    'id': pa.array([1, 2], type=pa.int32()),
    'tags': pa.array([['hello', 'world'], None], type=pa.list_(pa.field('element', pa.string()))),
}, schema=_list_schema)
pq.write_table(_list_data, _list_ann_base, use_dictionary=False, compression=None, data_page_version='1.0')
strip_converted_type(_list_ann_base, 'core/src/test/resources/list_annotation_modern_only_test.parquet', 'tags')

print("\nGenerated list_annotation_both/modern_only_test.parquet:")
print("  - Schema: id INT32, tags LIST<STRING>")

# MAP annotation recognition test files.
# Two variants: both annotations (PyArrow default) and modern-only (logicalType only).
# The modern-only variant is the regression case for LogicalTypeReader field ID 2.
_map_ann_base = 'core/src/test/resources/map_annotation_both_test.parquet'
_map_schema = pa.schema([
    ('id', pa.int32(), False),
    ('attrs', pa.map_(pa.string(), pa.int32())),
])
_map_data = pa.Table.from_pylist([
    {'id': 1, 'attrs': [('a', 1), ('b', 2)]},
    {'id': 2, 'attrs': None},
], schema=_map_schema)
pq.write_table(_map_data, _map_ann_base, use_dictionary=False, compression=None, data_page_version='1.0')
strip_converted_type(_map_ann_base, 'core/src/test/resources/map_annotation_modern_only_test.parquet', 'attrs')
annotate_map_as_legacy_key_value(_map_ann_base, 'core/src/test/resources/map_annotation_legacy_key_value_test.parquet', 'attrs')

print("\nGenerated map_annotation_both/modern_only/legacy_key_value_test.parquet:")
print("  - Schema: id INT32, attrs MAP<STRING, INT32>")

# =====================================================================
# Misaligned per-column page boundaries for issue #277
# Two columns whose pages flush at different row positions within the
# same row group, exercising cross-column row alignment under filter
# pushdown.
# =====================================================================

misaligned_schema = pa.schema([
    ('narrow', pa.int32(), False),
    ('wide', pa.binary(), False),
])

misaligned_table = pa.table({
    'narrow': list(range(10000)),
    'wide': [f"row={i:08d}-{'x' * 83}".encode() for i in range(10000)],
}, schema=misaligned_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/misaligned_pages.parquet',
    schema=misaligned_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='2.0',
    data_page_size=4096,
    # write_batch_size=17 so narrow's page period (61×17 = 1037 values) is not
    # a multiple of wide's page period (3×17 = 51 values). Narrow page
    # boundaries therefore never coincide with wide page boundaries, forcing
    # the first kept page of the two columns to start at different global rows
    # under any non-trivial filter.
    write_batch_size=17,
    write_statistics=True,
    write_page_index=True,
)
writer.write_table(misaligned_table)
writer.close()

print("\nGenerated misaligned_pages.parquet:")
print("  - 1 row group, 10000 rows, Parquet v2 with ColumnIndex + OffsetIndex")
print("  - narrow INT32 (4 B/value, ~10 pages) vs wide BYTE_ARRAY (~96 B/value, ~197 pages)")
print("  - Per-column page boundaries do not line up — exercises cross-column row alignment")

# test data for geospatial metadata reading
geospatial_schema = pa.schema([pa.field('city_name', pa.string(), False), pa.field('city_geom', pa.binary(), True)])
geospatial_table = pa.table({'city_name': ['London', 'Paris', 'Tokyo'], 'city_geom': [b'\x01', b'\x02', b'\x03']}, schema=geospatial_schema)
geospatial_base_path = '/tmp/_geospatial_base.parquet'
pq.write_table(geospatial_table, geospatial_base_path, use_dictionary=False, compression=None, data_page_version='1.0')

with open(geospatial_base_path, 'rb') as f:
    base_data = f.read()
base_footer_len = struct.unpack('<I', base_data[-8:-4])[0]
pre_footer = base_data[:len(base_data) - 8 - base_footer_len]
base_pf = pq.ParquetFile(geospatial_base_path)
base_rg = base_pf.metadata.row_group(0)

# Build FileMetaData with column-level kv metadata
fm = _ThriftWriter()
fm.f(1, _T_I32).i32(2)
fm.f(2, _T_LIST).lst(_T_STRUCT, 3)
fm.raw(_schema_elem("schema", num_children=2))
fm.raw(_schema_elem("city_name", type_val=6, rep=0))
fm.raw(_schema_elem("city_geom", type_val=6, rep=1))
fm.f(3, _T_I64).i64(3)
fm.f(4, _T_LIST).lst(_T_STRUCT, 1)

rw = _ThriftWriter()
rw.f(1, _T_LIST).lst(_T_STRUCT, 2)

for ci in range(2):
    col = base_rg.column(ci)
    pt = 6
    encs = [enc_map.get(str(e), 0) for e in col.encodings]

    # column chunk
    cc = _ThriftWriter()
    cc.f(2, _T_I64).i64(col.file_offset)
    cc.f(3, _T_STRUCT)

    md = _ThriftWriter()
    md.f(1, _T_I32).i32(pt)
    md.f(2, _T_LIST).lst(_T_I32, len(encs))
    for e in encs: md.i32(e)
    md.f(3, _T_LIST).lst(_T_BIN, 1).s(col.path_in_schema)
    md.f(4, _T_I32).i32(0)
    md.f(5, _T_I64).i64(col.num_values)
    md.f(6, _T_I64).i64(col.total_uncompressed_size)
    md.f(7, _T_I64).i64(col.total_compressed_size)
    md.f(9, _T_I64).i64(col.data_page_offset)
    if ci == 1:
        bbox = _ThriftWriter()
        bbox.f(1, _T_DOUBLE).double(-4.0)  # xmin
        bbox.f(2, _T_DOUBLE).double(7.5)   # xmax
        bbox.f(3, _T_DOUBLE).double(20.96)  # ymin
        bbox.f(4, _T_DOUBLE).double(77.08)  # ymax
        bbox.f(5, _T_DOUBLE).double(10.5)   # zmin
        bbox.f(6, _T_DOUBLE).double(90.0) # zmax
        bbox.end()

        geospatial = _ThriftWriter()
        geospatial.f(1, _T_STRUCT).raw(bbox.out())
        geospatial.f(2, _T_LIST).lst(_T_I32, 2).i32(1).i32(6)
        geospatial.end()

        md.f(17, _T_STRUCT).raw(geospatial.out())
    md.end()

    cc.raw(md.out()).end()
    rw.raw(cc.out())

rw.f(2, _T_I64).i64(base_rg.total_byte_size)
rw.f(3, _T_I64).i64(base_rg.num_rows)
rw.end()
fm.raw(rw.out())
fm.f(6, _T_BIN).s("hardwood-test-datagen")
fm.end()

footer_bytes = fm.out()
output = pre_footer + footer_bytes + struct.pack('<I', len(footer_bytes)) + b'PAR1'
with open('core/src/test/resources/geospatial_stats_test.parquet', 'wb') as f:
    f.write(output)

print("\nGenerated geospatial_stats_test.parquet:")
print("  - city_geom column has GeospatialStatistics at field 17")
print("  - BoundingBox: xmin=-4.0, xmax=7.5, ymin=20.96, ymax=77.08, zmin=10.5, zmax=90.0")
print("  - geospatial_types: [1, 6]")

# ============================================================================
# Geospatial fixtures: variants that exercise optional-field handling
# ============================================================================

def _geo_variant_path(name):
    return 'core/src/test/resources/' + name

def _build_geospatial_variant(out_path, *, with_bbox, with_types):
    """Generate a one-row-group, two-column Parquet file. The second column carries
    a GeospatialStatistics struct whose bbox / types presence is controlled by the
    flags. Used by GeospatialStatisticsTest to cover bbox-absent / types-absent."""
    schema = pa.schema([pa.field('city_name', pa.string(), False),
                        pa.field('city_geom', pa.binary(), True)])
    table = pa.table({'city_name': ['London', 'Paris', 'Tokyo'],
                      'city_geom': [b'\x01', b'\x02', b'\x03']}, schema=schema)
    base_path = '/tmp/_geospatial_variant_base.parquet'
    pq.write_table(table, base_path, use_dictionary=False, compression=None,
                   data_page_version='1.0')

    with open(base_path, 'rb') as f:
        base_data = f.read()
    base_footer_len = struct.unpack('<I', base_data[-8:-4])[0]
    pre_footer = base_data[:len(base_data) - 8 - base_footer_len]
    base_pf = pq.ParquetFile(base_path)
    base_rg = base_pf.metadata.row_group(0)

    fm = _ThriftWriter()
    fm.f(1, _T_I32).i32(2)
    fm.f(2, _T_LIST).lst(_T_STRUCT, 3)
    fm.raw(_schema_elem("schema", num_children=2))
    fm.raw(_schema_elem("city_name", type_val=6, rep=0))
    fm.raw(_schema_elem("city_geom", type_val=6, rep=1))
    fm.f(3, _T_I64).i64(3)
    fm.f(4, _T_LIST).lst(_T_STRUCT, 1)

    rw = _ThriftWriter()
    rw.f(1, _T_LIST).lst(_T_STRUCT, 2)

    for ci in range(2):
        col = base_rg.column(ci)
        encs = [enc_map.get(str(e), 0) for e in col.encodings]
        cc = _ThriftWriter()
        cc.f(2, _T_I64).i64(col.file_offset)
        cc.f(3, _T_STRUCT)

        md = _ThriftWriter()
        md.f(1, _T_I32).i32(6)
        md.f(2, _T_LIST).lst(_T_I32, len(encs))
        for e in encs: md.i32(e)
        md.f(3, _T_LIST).lst(_T_BIN, 1).s(col.path_in_schema)
        md.f(4, _T_I32).i32(0)
        md.f(5, _T_I64).i64(col.num_values)
        md.f(6, _T_I64).i64(col.total_uncompressed_size)
        md.f(7, _T_I64).i64(col.total_compressed_size)
        md.f(9, _T_I64).i64(col.data_page_offset)
        if ci == 1:
            geospatial = _ThriftWriter()
            if with_bbox:
                bbox = _ThriftWriter()
                bbox.f(1, _T_DOUBLE).double(-4.0)
                bbox.f(2, _T_DOUBLE).double(7.5)
                bbox.f(3, _T_DOUBLE).double(20.96)
                bbox.f(4, _T_DOUBLE).double(77.08)
                bbox.end()
                geospatial.f(1, _T_STRUCT).raw(bbox.out())
            if with_types:
                geospatial.f(2, _T_LIST).lst(_T_I32, 1).i32(1)
            geospatial.end()
            md.f(17, _T_STRUCT).raw(geospatial.out())
        md.end()

        cc.raw(md.out()).end()
        rw.raw(cc.out())

    rw.f(2, _T_I64).i64(base_rg.total_byte_size)
    rw.f(3, _T_I64).i64(base_rg.num_rows)
    rw.end()
    fm.raw(rw.out())
    fm.f(6, _T_BIN).s("hardwood-test-datagen")
    fm.end()

    footer_bytes = fm.out()
    output = pre_footer + footer_bytes + struct.pack('<I', len(footer_bytes)) + b'PAR1'
    with open(out_path, 'wb') as f:
        f.write(output)

_build_geospatial_variant(_geo_variant_path('geospatial_stats_types_only.parquet'),
                          with_bbox=False, with_types=True)
print("\nGenerated geospatial_stats_types_only.parquet:")
print("  - GeospatialStatistics with geospatial_types=[1] and no bounding box")

_build_geospatial_variant(_geo_variant_path('geospatial_stats_bbox_only.parquet'),
                          with_bbox=True, with_types=False)
print("\nGenerated geospatial_stats_bbox_only.parquet:")
print("  - GeospatialStatistics with bounding box and no geospatial_types list")

# ============================================================================
# Geospatial end-to-end fixture: 3 row groups (Europe, Asia, Americas)
# Used by GeospatialEndToEndTest to verify spatial filter pushdown.
# ============================================================================

def wkb_point(x, y):
    # WKB Point: byte order (1=little endian) + type (1=Point) + x + y
    return struct.pack('<BIdd', 1, 1, x, y)

def _geometry_logical_type():
    # LogicalType union field 17 = GeometryType (empty struct: defaults to OGC:CRS84)
    w = _ThriftWriter()
    w.f(17, _T_STRUCT).end()
    w.end()
    return w.out()

europe_names = ['London', 'Paris', 'Berlin']
europe_points = [wkb_point(-0.12, 51.50), wkb_point(2.35, 48.85), wkb_point(13.40, 52.52)]
asia_names = ['Tokyo', 'Beijing', 'Mumbai']
asia_points = [wkb_point(139.69, 35.68), wkb_point(116.39, 39.91), wkb_point(72.87, 19.07)]
america_names = ['New York', 'Chicago', 'LA']
america_points = [wkb_point(-74.00, 40.71), wkb_point(-87.62, 41.87), wkb_point(-118.24, 34.05)]

geo_schema = pa.schema([pa.field('city_name', pa.string(), False),
                        pa.field('location', pa.binary(), False)])
geo_base_path = '/tmp/_geo_e2e_base.parquet'
writer = pq.ParquetWriter(geo_base_path, geo_schema, use_dictionary=False,
                          compression=None, data_page_version='1.0')
writer.write_table(pa.table({'city_name': europe_names, 'location': europe_points}, schema=geo_schema))
writer.write_table(pa.table({'city_name': asia_names, 'location': asia_points}, schema=geo_schema))
writer.write_table(pa.table({'city_name': america_names, 'location': america_points}, schema=geo_schema))
writer.close()

base_pf = pq.ParquetFile(geo_base_path)
with open(geo_base_path, 'rb') as f:
    base_data = f.read()
base_footer_len = struct.unpack('<I', base_data[-8:-4])[0]
pre_footer = base_data[:len(base_data) - 8 - base_footer_len]

fm = _ThriftWriter()
fm.f(1, _T_I32).i32(2)
fm.f(2, _T_LIST).lst(_T_STRUCT, 3)
fm.raw(_schema_elem("schema", num_children=2))
fm.raw(_schema_elem("city_name", type_val=6, rep=0))
fm.raw(_schema_elem("location", type_val=6, rep=0, logical_type=_geometry_logical_type()))
fm.f(3, _T_I64).i64(9)
fm.f(4, _T_LIST).lst(_T_STRUCT, 3)

continent_bbox = {
    0: [-0.12, 13.40, 48.85, 52.52],   # Europe
    1: [72.87, 139.69, 19.07, 39.91],  # Asia
    2: [-118.24, -74.00, 34.05, 41.87],# Americas
}
for rgi in range(3):
    rw = _ThriftWriter()
    rw.f(1, _T_LIST).lst(_T_STRUCT, 2)
    base_rg = base_pf.metadata.row_group(rgi)
    for ci in range(2):
        col = base_rg.column(ci)
        encs = [enc_map.get(str(e), 0) for e in col.encodings]

        cc = _ThriftWriter()
        cc.f(2, _T_I64).i64(col.file_offset)
        cc.f(3, _T_STRUCT)

        md = _ThriftWriter()
        md.f(1, _T_I32).i32(6)
        md.f(2, _T_LIST).lst(_T_I32, len(encs))
        for e in encs: md.i32(e)
        md.f(3, _T_LIST).lst(_T_BIN, 1).s(col.path_in_schema)
        md.f(4, _T_I32).i32(0)
        md.f(5, _T_I64).i64(col.num_values)
        md.f(6, _T_I64).i64(col.total_uncompressed_size)
        md.f(7, _T_I64).i64(col.total_compressed_size)
        md.f(9, _T_I64).i64(col.data_page_offset)
        if ci == 1:
            bbox = _ThriftWriter()
            bbox.f(1, _T_DOUBLE).double(continent_bbox[rgi][0])  # xmin
            bbox.f(2, _T_DOUBLE).double(continent_bbox[rgi][1])  # xmax
            bbox.f(3, _T_DOUBLE).double(continent_bbox[rgi][2])  # ymin
            bbox.f(4, _T_DOUBLE).double(continent_bbox[rgi][3])  # ymax
            bbox.f(5, _T_DOUBLE).double(10.5)
            bbox.f(6, _T_DOUBLE).double(90.0)
            bbox.end()

            geospatial = _ThriftWriter()
            geospatial.f(1, _T_STRUCT).raw(bbox.out())
            geospatial.f(2, _T_LIST).lst(_T_I32, 2).i32(1).i32(6)
            geospatial.end()

            md.f(17, _T_STRUCT).raw(geospatial.out())
        md.end()

        cc.raw(md.out()).end()
        rw.raw(cc.out())

    rw.f(2, _T_I64).i64(base_rg.total_byte_size)
    rw.f(3, _T_I64).i64(base_rg.num_rows)
    rw.end()
    fm.raw(rw.out())

fm.f(6, _T_BIN).s("hardwood-test-datagen")
fm.end()

footer_bytes = fm.out()
output = pre_footer + footer_bytes + struct.pack('<I', len(footer_bytes)) + b'PAR1'
with open('core/src/test/resources/geospatial_e2e_test.parquet', 'wb') as f:
    f.write(output)

print("\nGenerated geospatial_e2e_test.parquet:")
print("  - 3 row groups (Europe, Asia, Americas) with WKB Point geometries")
print("  - location column has GEOMETRY logical type and per-chunk GeospatialStatistics")

# =====================================================================
# Misaligned per-column page boundaries WITHOUT a Page Index. Exercises
# the SequentialFetchPlan mask path: filter pushdown and tail-mode fast
# path must keep columns row-aligned even though there's no OffsetIndex
# to carry per-page row counts. Independently sized from
# misaligned_pages.parquet so the file stays small (~80 KiB).
# =====================================================================

NO_INDEX_ROWS = 2000
no_index_misaligned_schema = pa.schema([
    ('narrow', pa.int32(), False),
    ('wide', pa.binary(), False),
])
no_index_misaligned_table = pa.table({
    'narrow': list(range(NO_INDEX_ROWS)),
    # 4-byte ASCII row prefix so the wide value self-identifies its row.
    # Total ~32 B/value (24-byte payload + 4-byte length prefix + framing).
    'wide': [f"row={i:05d}-{'x' * 19}".encode() for i in range(NO_INDEX_ROWS)],
}, schema=no_index_misaligned_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/misaligned_pages_no_index.parquet',
    schema=no_index_misaligned_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='2.0',
    data_page_size=512,
    # write_batch_size=7 keeps narrow's page period (~19×7 values) coprime
    # with wide's (~3×7 values) so per-column page boundaries don't line up.
    write_batch_size=7,
    write_statistics=True,
    write_page_index=False,
)
writer.write_table(no_index_misaligned_table)
writer.close()

print("\nGenerated misaligned_pages_no_index.parquet:")
print(f"  - 1 row group, {NO_INDEX_ROWS} rows, Parquet v2, NO ColumnIndex/OffsetIndex")
print("  - narrow INT32 vs wide ~32 B/value with coprime page periods")
print("  - Drives the SequentialFetchPlan per-page mask path for flat columns")

# =====================================================================
# Misaligned page boundaries with a nested LIST<STRING> column on a v2
# fixture without a Page Index. Drives the rep-level walk path in
# SequentialFetchPlan when the row-group-wide mask gate admits nested-v2
# columns.
# =====================================================================

NESTED_V2_ROWS = 2000
nested_v2_schema = pa.schema([
    ('narrow', pa.int32(), False),
    # Two strings per row: a row-self-identifying tag plus a label.
    ('tags', pa.list_(pa.field('element', pa.string()))),
])
nested_v2_table = pa.table({
    'narrow': list(range(NESTED_V2_ROWS)),
    'tags': [[f"row={i:05d}", f"tag-{i % 7}"] for i in range(NESTED_V2_ROWS)],
}, schema=nested_v2_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/misaligned_pages_nested_v2.parquet',
    schema=nested_v2_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='2.0',
    data_page_size=512,
    write_batch_size=7,
    write_statistics=True,
    write_page_index=False,
)
writer.write_table(nested_v2_table)
writer.close()

print("\nGenerated misaligned_pages_nested_v2.parquet:")
print(f"  - 1 row group, {NESTED_V2_ROWS} rows, Parquet v2, NO ColumnIndex/OffsetIndex")
print("  - narrow INT32 (flat) + tags LIST<STRING> (nested, 2 elements per row)")
print("  - Drives SequentialFetchPlan rep-level walk on a v2 nested column")

# =====================================================================
# Nested LIST<STRING> column with DATA_PAGE (v1) pages and no Page Index.
# Closes the row-group-wide mask gate: the rep-level prefix lives inside
# the compressed area on a v1 page, so counting top-level records would
# require decompression — which defeats the skip-without-decompress
# optimisation. Drives the gate-closed branch in
# RowGroupIterator.computeFetchPlans (matchingRows promoted to ALL) and
# the tail-read fallback path that decode-and-discards leading rows
# instead of using per-page masks.
# =====================================================================

NESTED_V1_ROWS = 2000
nested_v1_schema = pa.schema([
    ('narrow', pa.int32(), False),
    ('tags', pa.list_(pa.field('element', pa.string()))),
])
nested_v1_table = pa.table({
    'narrow': list(range(NESTED_V1_ROWS)),
    'tags': [[f"row={i:05d}", f"tag-{i % 7}"] for i in range(NESTED_V1_ROWS)],
}, schema=nested_v1_schema)

writer = pq.ParquetWriter(
    'core/src/test/resources/nested_v1_no_index.parquet',
    schema=nested_v1_schema,
    use_dictionary=False,
    compression='NONE',
    data_page_version='1.0',
    data_page_size=512,
    write_batch_size=7,
    write_statistics=True,
    write_page_index=False,
)
writer.write_table(nested_v1_table)
writer.close()

print("\nGenerated nested_v1_no_index.parquet:")
print(f"  - 1 row group, {NESTED_V1_ROWS} rows, Parquet v1, NO ColumnIndex/OffsetIndex")
print("  - narrow INT32 (flat) + tags LIST<STRING> (nested, v1 pages)")
print("  - Closes the row-group-wide mask gate; drives the fallback path")

# ============================================================================
# PR #471 Review Fixtures (#463 / #470 / #473)
# ============================================================================

import struct as _binstruct  # local alias to avoid clashing with pa.struct

# 1. List<FLOAT16> — closes the test gap on PqListImpl.floats()'s FLBA(2)
#    + Float16Type branch (#470).
list_f16_schema = pa.schema([
    ('id', pa.int32(), False),
    ('scores', pa.list_(pa.binary(2))),
])
list_f16_table = pa.table({
    'id': [1, 2],
    'scores': [
        [_binstruct.pack('<e', 1.5), _binstruct.pack('<e', -2.0), _binstruct.pack('<e', 0.0)],
        [_binstruct.pack('<e', 100.0)],
    ],
}, schema=list_f16_schema)
pq.write_table(
    list_f16_table,
    'core/src/test/resources/list_float16_test.parquet',
    use_dictionary=False, compression=None, data_page_version='1.0',
)
annotate_element_at_path_as_float16(
    'core/src/test/resources/list_float16_test.parquet',
    ['scores', 'list', 'element'])

print("\nGenerated list_float16_test.parquet (#470): List<FLOAT16>")

# 2. Map<int64,*> and Map<binary,*> — closes #463's long-key and byte[]-key
#    coverage gaps in PqMapLookupTest.
map_typed_keys_schema = pa.schema([
    ('id', pa.int32(), False),
    ('long_keyed', pa.map_(pa.int64(), pa.string())),
    ('binary_keyed', pa.map_(pa.binary(), pa.int32())),
])
map_typed_keys_table = pa.table({
    'id': [1],
    'long_keyed': [[(100, 'one-hundred'), (200, 'two-hundred'), (300, 'three-hundred')]],
    'binary_keyed': [[(b'\x01\x02', 10), (b'\x03\x04', 20)]],
}, schema=map_typed_keys_schema)
pq.write_table(
    map_typed_keys_table,
    'core/src/test/resources/map_typed_keys_test.parquet',
    use_dictionary=False, compression=None, data_page_version='1.0',
)

print("Generated map_typed_keys_test.parquet (#463): Map<int64,*> + Map<binary,*>")

# 3. Wide-schema struct (one field per primitive logical type) — backs the
#    parametric by-index test in #473. UUID, INTERVAL, and VARIANT by-index
#    are covered separately by their dedicated fixtures
#    (logical_types_test / typed_accessors_issue_445 /
#    variant_in_repeated_test); inlining them here trips
#    `pa.table(..., schema=struct_with_extension_type)` on PyArrow 24.
wide_struct_schema = pa.schema([
    ('id', pa.int32(), False),
    ('fields', pa.struct([
        ('a_int', pa.int32()),
        ('b_long', pa.int64()),
        ('c_float', pa.float32()),
        ('d_double', pa.float64()),
        ('e_bool', pa.bool_()),
        ('f_string', pa.string()),
        ('g_binary', pa.binary()),
        ('h_date', pa.date32()),
        ('i_time', pa.time32('ms')),
        ('j_timestamp', pa.timestamp('us', tz='UTC')),
        ('k_decimal', pa.decimal128(10, 2)),
        ('m_list', pa.list_(pa.int32())),
        ('n_map', pa.map_(pa.string(), pa.int32())),
    ])),
])
wide_struct_table = pa.table({
    'id': [1],
    'fields': [{
        'a_int': 42,
        'b_long': 12345678901234,
        'c_float': 1.5,
        'd_double': 2.5,
        'e_bool': True,
        'f_string': 'hello',
        'g_binary': b'\xde\xad\xbe\xef',
        'h_date': date(2026, 1, 15),
        'i_time': time(12, 30, 45),
        'j_timestamp': pa.scalar(1_000_000, type=pa.timestamp('us', tz='UTC')).as_py(),
        'k_decimal': Decimal('123.45'),
        'm_list': [1, 2, 3],
        'n_map': [('alpha', 10), ('beta', 20)],
    }],
}, schema=wide_struct_schema)
pq.write_table(
    wide_struct_table,
    'core/src/test/resources/wide_struct_test.parquet',
    use_dictionary=False, compression=None, data_page_version='1.0',
)

print("Generated wide_struct_test.parquet (#473): 13-field struct (primitive logical types + list + map; UUID/INTERVAL/VARIANT covered elsewhere)")

# =====================================================================
# Depth-1 optional struct around an optional leaf — the canonical #436
# shape. Three rows pin all three states the layer model must
# disambiguate: struct null, struct present with leaf null, both
# present.
# =====================================================================

optional_struct_optional_leaf_schema = pa.schema([
    ('id', pa.int32(), False),
    ('point', pa.struct([
        ('x', pa.int32()),
    ])),
])

optional_struct_optional_leaf_data = [
    {'id': 1, 'point': None},        # struct null
    {'id': 2, 'point': {'x': None}}, # struct present, leaf null
    {'id': 3, 'point': {'x': 42}},   # both present
]

optional_struct_optional_leaf_table = pa.Table.from_pylist(
    optional_struct_optional_leaf_data,
    schema=optional_struct_optional_leaf_schema,
)
pq.write_table(
    optional_struct_optional_leaf_table,
    'core/src/test/resources/optional_struct_optional_leaf_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)

print("\nGenerated optional_struct_optional_leaf_test.parquet:")
print("  - Schema: id, point: optional struct { optional int32 x }")
print("  - Rows: struct=null / struct=present,x=null / struct=present,x=42")

# =====================================================================
# list<optional struct { optional int32 age }> — the element-wrapper
# correctness gate. One record contains both a struct-null entry and a
# struct-present-leaf-null entry inside the same list, so a consumer
# must read getLayerValidity(structLayer) AND getLeafValidity()
# independently to recover the difference. An implementation that
# re-folded list-element struct nullability into the leaf would only
# fail here.
# =====================================================================

list_of_optional_struct_schema = pa.schema([
    ('id', pa.int32(), False),
    ('items', pa.list_(pa.struct([
        ('age', pa.int32()),
    ]))),
])

list_of_optional_struct_data = [
    # Row 0: 3 entries — leaf-present, leaf-null, struct-null
    {'id': 1, 'items': [{'age': 10}, {'age': None}, None]},
    # Row 1: empty list
    {'id': 2, 'items': []},
    # Row 2: null list
    {'id': 3, 'items': None},
    # Row 3: single leaf-present entry
    {'id': 4, 'items': [{'age': 99}]},
]

list_of_optional_struct_table = pa.Table.from_pylist(
    list_of_optional_struct_data,
    schema=list_of_optional_struct_schema,
)
pq.write_table(
    list_of_optional_struct_table,
    'core/src/test/resources/list_of_optional_struct_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)

print("\nGenerated list_of_optional_struct_test.parquet:")
print("  - Schema: id, items: list<optional struct { optional int32 age }>")
print("  - Rows: [{age=10},{age=null},null] / [] / null / [{age=99}]")

# =====================================================================
# Two-column file where only one column carries a bloom filter, so a
# reader can verify both the populated and the absent shape of the
# `bloom_filter_offset` / `bloom_filter_length` fields on the same
# footer parse.
# =====================================================================

bloom_schema = pa.schema([
    ('id', pa.int64(), False),
    ('value', pa.int64(), False),
])
bloom_table = pa.table({
    'id': list(range(64)),
    'value': [v * 10 for v in range(64)],
}, schema=bloom_schema)

pq.write_table(
    bloom_table,
    'core/src/test/resources/bloom_filter_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
    bloom_filter_options={'id': {'ndv': 64, 'fpp': 0.05}},
)

print("\nGenerated bloom_filter_test.parquet:")
print("  - 1 row group, 64 rows, INT64 id + INT64 value")
print("  - Bloom filter on 'id' only; 'value' has no bloom filter")

# =====================================================================
# Local-wall-clock TIMESTAMP fixture (#568). PyArrow's `pa.timestamp(unit)`
# with no `tz=` argument emits TIMESTAMP(isAdjustedToUTC=false), the
# "naive" / wall-clock case. The companion UTC-adjusted column lets the
# read-side test exercise both kinds against the same fixture.
# =====================================================================

local_timestamp_schema = pa.schema([
    ('id', pa.int32(), False),
    ('local_millis', pa.timestamp('ms'), False),         # NTZ MILLIS
    ('local_micros', pa.timestamp('us'), False),         # NTZ MICROS
    ('utc_micros',   pa.timestamp('us', tz='UTC'), False),  # adjusted MICROS
    # Nested coverage so the per-impl `require` dispatch on PqStruct, PqList,
    # and PqMap.Entry is exercised end-to-end.
    ('nested', pa.struct([
        ('local_ts', pa.timestamp('us')),
        ('utc_ts',   pa.timestamp('us', tz='UTC')),
    ]), False),
    ('local_ts_list', pa.list_(pa.timestamp('us')), False),
    ('local_ts_map',  pa.map_(pa.string(), pa.timestamp('us')), False),
])
local_timestamp_table = pa.table({
    'id': [1, 2],
    'local_millis': [
        datetime(2026, 3, 5, 9, 30, 0),
        datetime(2026, 3, 5, 17, 45, 30),
    ],
    'local_micros': [
        datetime(2026, 3, 5, 9, 30, 0, 123456),
        datetime(2026, 3, 5, 17, 45, 30, 654321),
    ],
    'utc_micros': [
        datetime(2026, 3, 5, 9, 30, 0, 123456),
        datetime(2026, 3, 5, 17, 45, 30, 654321),
    ],
    'nested': [
        {'local_ts': datetime(2026, 3, 5, 9, 30, 0, 123456),
         'utc_ts':   datetime(2026, 3, 5, 9, 30, 0, 123456)},
        {'local_ts': datetime(2026, 3, 5, 17, 45, 30, 654321),
         'utc_ts':   datetime(2026, 3, 5, 17, 45, 30, 654321)},
    ],
    'local_ts_list': [
        [datetime(2026, 3, 5, 9, 30, 0), datetime(2026, 3, 5, 10, 0, 0)],
        [datetime(2026, 3, 5, 17, 45, 30)],
    ],
    'local_ts_map': [
        [('start', datetime(2026, 3, 5, 9, 30, 0)),
         ('end',   datetime(2026, 3, 5, 10, 0, 0))],
        [('start', datetime(2026, 3, 5, 17, 45, 30))],
    ],
}, schema=local_timestamp_schema)

pq.write_table(
    local_timestamp_table,
    'core/src/test/resources/local_timestamp_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)

print("\nGenerated local_timestamp_test.parquet (#568):")
print("  - 2 rows, NTZ MILLIS + NTZ MICROS + UTC MICROS columns,")
print("    plus nested struct/list/map with NTZ children")

# Companion fixture: INT96 TIMESTAMP. PyArrow only writes INT96 under the
# `use_deprecated_int96_timestamps=True` flag (it is a legacy Spark/Hive
# convention and has no isAdjustedToUTC field). One row, one column — just
# enough to pin the INT96 fallthrough behavior of both timestamp accessors.
int96_schema = pa.schema([
    ('ts', pa.timestamp('us', tz='UTC'), False),
])
int96_table = pa.table({
    'ts': [datetime(2026, 3, 5, 9, 30, 0, 123456)],
}, schema=int96_schema)
pq.write_table(
    int96_table,
    'core/src/test/resources/int96_timestamp_test.parquet',
    use_dictionary=False,
    compression=None,
    use_deprecated_int96_timestamps=True,
    data_page_version='1.0',
)
print("\nGenerated int96_timestamp_test.parquet (#568):")
print("  - 1 row, single INT96 TIMESTAMP column")

# =====================================================================
# Dive screenshots fixture (docs/content/assets/cli/*.svg)
# Not a test corpus file: this realistic, wide, nested/list/map dataset
# with multiple row groups and dictionary columns is the input the
# `screenshots` Maven profile dives into when regenerating the docs
# screenshots. See docs/screenshots/README.md.
# =====================================================================

dive_screenshots_schema = pa.schema([
    ('id', pa.string(), False),
    ('version', pa.int32(), False),
    ('confidence', pa.float64(), True),
    ('bbox', pa.struct([
        ('xmin', pa.float64()),
        ('xmax', pa.float64()),
        ('ymin', pa.float64()),
        ('ymax', pa.float64()),
    ])),
    ('names', pa.struct([
        ('primary', pa.string()),
        ('common', pa.map_(pa.string(), pa.string())),
    ])),
    ('sources', pa.list_(pa.struct([
        ('dataset', pa.string()),
        ('record_id', pa.string()),
        ('license', pa.string()),
    ]))),
    ('addresses', pa.list_(pa.struct([
        ('freeform', pa.string()),
        ('locality', pa.string()),
        ('region', pa.string()),
        ('country', pa.string()),
    ]))),
    ('websites', pa.list_(pa.string())),
    ('category', pa.string(), False),
    ('status', pa.string(), False),
    ('city', pa.string(), True),
    ('country_code', pa.string(), True),
    ('metric_a', pa.int64(), False),
    ('metric_b', pa.int64(), False),
    ('metric_c', pa.int64(), False),
    ('metric_d', pa.int64(), False),
    ('metric_e', pa.int64(), False),
    ('metric_f', pa.int64(), False),
    ('metric_g', pa.int64(), False),
    ('metric_h', pa.int64(), False),
])


def _build_dive_screenshots_chunk(start, count):
    rows = []
    categories = ['restaurant', 'cafe', 'grocery', 'pharmacy', 'hotel', 'museum']
    statuses = ['active', 'active', 'active', 'inactive']
    cities = ['New York', 'Boston', 'Chicago', 'Austin', 'Seattle', 'Denver']

    for i in range(start, start + count):
        cat = categories[i % len(categories)]
        rows.append({
            'id': f'place-{i:06d}',
            'version': 1000 + i,
            'confidence': round(0.35 + ((i % 60) / 100.0), 2),
            'bbox': {
                'xmin': -123.0 + (i % 50) * 0.01,
                'xmax': -122.5 + (i % 50) * 0.01,
                'ymin': 37.0 + (i % 50) * 0.005,
                'ymax': 37.4 + (i % 50) * 0.005,
            },
            'names': {
                'primary': f'{cat.title()} #{i}',
                'common': {
                    'en': f'{cat.title()} {i}',
                    'es': f'{cat.title()} {i} ES',
                },
            },
            'sources': [
                {'dataset': 'synthetic', 'record_id': f'syn-{i}', 'license': 'CC-BY'},
                {'dataset': 'seed', 'record_id': f'seed-{i}', 'license': 'ODbL'},
            ],
            'addresses': [
                {
                    'freeform': f'{100 + (i % 900)} Main St',
                    'locality': cities[i % len(cities)],
                    'region': 'NA',
                    'country': 'United States',
                }
            ],
            'websites': [f'https://example.org/{cat}/{i}', f'https://m.example.org/{i}'],
            'category': cat,
            'status': statuses[i % len(statuses)],
            'city': cities[i % len(cities)],
            'country_code': 'US',
            'metric_a': i * 3 + 1,
            'metric_b': i * 3 + 2,
            'metric_c': i * 3 + 3,
            'metric_d': i * 3 + 4,
            'metric_e': i * 3 + 5,
            'metric_f': i * 3 + 6,
            'metric_g': i * 3 + 7,
            'metric_h': i * 3 + 8,
        })
    return pa.Table.from_pylist(rows, schema=dive_screenshots_schema)


dive_screenshots_writer = pq.ParquetWriter(
    'cli/src/test/resources/dive_screenshots_fixture.parquet',
    schema=dive_screenshots_schema,
    compression='zstd',
    use_dictionary=['category', 'status', 'city', 'country_code', 'names.primary'],
    data_page_version='1.0',
)
for dive_rg_start in (0, 150, 300, 450):
    dive_screenshots_writer.write_table(_build_dive_screenshots_chunk(dive_rg_start, 150))
dive_screenshots_writer.close()

print("\nGenerated dive_screenshots_fixture.parquet (in cli):")
print("  - 4 row groups, 600 rows, nested/list/map fields + dictionary columns")
print("  - Wide metric_* tail columns for horizontal scrolling in data preview")


# Struct followed by a top-level primitive (guard fixture for #525).
# Schema: id INT, point STRUCT{x, y}, tag STRING, score INT
#
# Leaf-column indices:    id=0, point.x=1, point.y=2, tag=3, score=4
# Top-level field indices: id=0, point=1,            tag=2, score=3
#
# `score` is a top-level primitive whose LEAF-column index (4) differs from its
# top-level FIELD index (3): the `point` struct contributes two extra leaf
# columns in front of it. `tag` is a string column; a predicate on a BYTE_ARRAY
# column is not drain-side eligible, so AND-ing it with an int predicate on
# `score` forces the whole query onto the record-side (FilteredRowReader)
# filter path — the path that must keep field-index and leaf-column-index
# distinct when struct-of-primitives files move to the flat reader.
struct_then_primitive_schema = pa.schema([
    ('id', pa.int32(), False),
    ('point', pa.struct([
        ('x', pa.int32()),
        ('y', pa.int32()),
    ])),
    ('tag', pa.string(), False),
    ('score', pa.int32(), False),
])
struct_then_primitive_table = pa.table({
    'id': [1, 2, 3, 4],
    'point': [
        {'x': 10, 'y': 11},
        {'x': 20, 'y': 21},
        {'x': 30, 'y': 31},
        {'x': 40, 'y': 41},
    ],
    'tag': ['keep', 'keep', 'keep', 'keep'],
    'score': [50, 60, 70, 80],
}, schema=struct_then_primitive_schema)
pq.write_table(
    struct_then_primitive_table,
    'core/src/test/resources/struct_then_primitive_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0',
)
print("\nGenerated struct_then_primitive_test.parquet:")
print("  - id=[1,2,3,4], point={x,y}, tag='keep', score=[50,60,70,80]")
print("  - score: field index 3 but leaf-column index 4")

# ---------------------------------------------------------------------------
# Differential-testing corpus (DuckDB oracle harness, #548).
#
# Flat, deterministic fixtures carrying a synthetic `__row__` column equal to the
# physical row position, so that file order is recoverable as ORDER BY __row__ and
# hardwood's file-order head/tail/skip map one-to-one onto SQL LIMIT/OFFSET. Two
# files share one schema and differ only in row-group layout (single vs. multi).
# Values are computed deterministically (no RNG) so the bytes are stable.
# ---------------------------------------------------------------------------
diff_dir = Path('core/src/test/resources/differential')
diff_dir.mkdir(parents=True, exist_ok=True)

DIFF_ROWS = 250
diff_schema = pa.schema([
    ('__row__', pa.int64(), False),   # physical row position 0..N-1
    ('id', pa.int64(), False),        # = __row__ + 1
    ('score', pa.float64(), False),
    ('category', pa.string(), False),
    ('note', pa.string(), True),      # OPTIONAL — every 7th row is null
])
diff_table = pa.table({
    '__row__': list(range(DIFF_ROWS)),
    'id': [r + 1 for r in range(DIFF_ROWS)],
    'score': [float(r % 100) + 0.25 for r in range(DIFF_ROWS)],
    'category': ['ABCDE'[r % 5] for r in range(DIFF_ROWS)],
    'note': [None if r % 7 == 0 else f'note-{r}' for r in range(DIFF_ROWS)],
}, schema=diff_schema)

for diff_name, diff_rg_size in [('diff_numbers_single', DIFF_ROWS), ('diff_numbers_multi', 50)]:
    pq.write_table(
        diff_table,
        str(diff_dir / f'{diff_name}.parquet'),
        use_dictionary=False,
        compression=None,
        data_page_version='2.0',
        row_group_size=diff_rg_size,
    )
print("\nGenerated differential corpus:")
print(f"  - diff_numbers_single.parquet: {DIFF_ROWS} rows, 1 row group")
print(f"  - diff_numbers_multi.parquet:  {DIFF_ROWS} rows, row groups of 50")

# Typed differential corpus (value-comparison harness, #548 P2). One row group,
# every physical/logical type the value comparator covers, all REQUIRED so the
# values themselves — not null handling — are what's compared.
DIFF_TYPES_ROWS = 200
diff_types_schema = pa.schema([
    ('__row__', pa.int64(), False),
    ('i32', pa.int32(), False),
    ('i64', pa.int64(), False),
    ('f32', pa.float32(), False),
    ('f64', pa.float64(), False),
    ('flag', pa.bool_(), False),
    ('s', pa.string(), False),
    ('d', pa.date32(), False),
    ('ts', pa.timestamp('us', tz='UTC'), False),
    ('dec', pa.decimal128(10, 2), False),
    ('bin', pa.binary(), False),
])
_date_base = date(2000, 1, 1).toordinal()
diff_types_table = pa.table({
    '__row__': list(range(DIFF_TYPES_ROWS)),
    'i32': [r - 100 for r in range(DIFF_TYPES_ROWS)],
    'i64': [r * 1000 - 50 for r in range(DIFF_TYPES_ROWS)],
    'f32': [r * 0.5 for r in range(DIFF_TYPES_ROWS)],
    'f64': [r / 8.0 for r in range(DIFF_TYPES_ROWS)],
    'flag': [r % 2 == 0 for r in range(DIFF_TYPES_ROWS)],
    's': [f'row-{r}' for r in range(DIFF_TYPES_ROWS)],
    'd': [date.fromordinal(_date_base + r) for r in range(DIFF_TYPES_ROWS)],
    'ts': [datetime.fromtimestamp(1577836800 + r, tz=timezone.utc).replace(microsecond=(r * 137) % 1000000)
           for r in range(DIFF_TYPES_ROWS)],
    'dec': [Decimal(f'{r}.25') for r in range(DIFF_TYPES_ROWS)],
    'bin': [bytes([r % 256, (r + 1) % 256, (r + 2) % 256]) for r in range(DIFF_TYPES_ROWS)],
}, schema=diff_types_schema)
pq.write_table(
    diff_types_table,
    str(diff_dir / 'diff_types.parquet'),
    use_dictionary=False,
    compression=None,
    data_page_version='2.0',
)
print(f"  - diff_types.parquet:          {DIFF_TYPES_ROWS} rows, typed value-comparison corpus")

# Nulls corpus (filter null-semantics harness, #548 P2). A nullable column with
# scattered nulls, so a comparison predicate must drop the null rows (SQL
# three-valued logic) to agree with the oracle.
DIFF_NULLS_ROWS = 150
diff_nulls_schema = pa.schema([
    ('__row__', pa.int64(), False),
    ('val', pa.int64(), True),  # OPTIONAL — null every 3rd row
])
diff_nulls_table = pa.table({
    '__row__': list(range(DIFF_NULLS_ROWS)),
    'val': [None if r % 3 == 0 else r * 2 for r in range(DIFF_NULLS_ROWS)],
}, schema=diff_nulls_schema)
pq.write_table(
    diff_nulls_table,
    str(diff_dir / 'diff_nulls.parquet'),
    use_dictionary=False,
    compression=None,
    data_page_version='2.0',
)
print(f"  - diff_nulls.parquet:          {DIFF_NULLS_ROWS} rows, nullable column for filter null-semantics")

# ============================================================================
# Encrypted Test Files (Parquet Modular Encryption)
# ============================================================================
#
# Hardwood does not support decryption (hardwood-hq/hardwood#600); these fixtures
# exist purely so the reader can be asserted to fail *gracefully* — with a clear
# "encrypted Parquet files are not supported" error rather than a misleading
# "invalid magic number" or an unattributable page-scan crash.
#
# Unlike every other fixture here, these are NOT byte-stable across reruns: AES-GCM
# draws a fresh random nonce per write, so regenerating produces a different (but
# equivalent) file. That is fine — the graceful-failure tests assert on the error,
# not on exact bytes — but do not be alarmed by a non-empty git diff after a rerun.


class _InMemoryKmsClient(pe.KmsClient):
    """Toy KMS that 'wraps' a data key by base64-encoding it. Sufficient for
    PyArrow to emit a syntactically valid encrypted file; provides no real
    protection, which is irrelevant for fixtures that are never decrypted."""

    def __init__(self, config):
        super().__init__()
        self.master_keys = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_id):
        return base64.b64encode(key_bytes)

    def unwrap_key(self, wrapped_key, master_key_id):
        return base64.b64decode(wrapped_key)


_encryption_table = pa.table({
    'id': [1, 2, 3],
    'value': [100, 200, 300],
}, schema=pa.schema([('id', pa.int64(), False), ('value', pa.int64(), False)]))

_kms_conf = pe.KmsConnectionConfig(custom_kms_conf={
    'footer_key': base64.b64encode(b'0123456789012345').decode(),
    'column_key': base64.b64encode(b'1234567890123450').decode(),
})


def _write_encrypted(path, plaintext_footer):
    crypto_factory = pe.CryptoFactory(lambda conf: _InMemoryKmsClient(conf))
    enc_config = pe.EncryptionConfiguration(
        footer_key='footer_key',
        column_keys={'column_key': ['value']},
        plaintext_footer=plaintext_footer,
    )
    props = crypto_factory.file_encryption_properties(_kms_conf, enc_config)
    with pq.ParquetWriter(path, _encryption_table.schema, encryption_properties=props) as writer:
        writer.write_table(_encryption_table)


# Encrypted-footer mode: the footer itself is encrypted and the magic bytes are
# 'PARE' instead of 'PAR1'.
_write_encrypted('core/src/test/resources/encrypted_footer.parquet', plaintext_footer=False)
print("\nGenerated encrypted_footer.parquet:")
print("  - Parquet Modular Encryption, encrypted-footer mode (PARE magic)")

# Plaintext-footer mode: the footer is readable (PAR1 magic) and carries
# encryption_algorithm, but the column data is encrypted.
_write_encrypted('core/src/test/resources/encrypted_plaintext_footer.parquet', plaintext_footer=True)
print("\nGenerated encrypted_plaintext_footer.parquet:")
print("  - Parquet Modular Encryption, plaintext-footer mode (PAR1 magic, encrypted data)")

# ============================================================================
# Malformed Metadata Test Files (hardwood-hq/hardwood#604)
# ============================================================================
#
# A structurally valid file whose footer carries a negative data_page_offset.
# The reader must reject it at metadata-parse time with a controlled,
# file-attributed IOException rather than letting the negative value reach a
# buffer slice. Written by rewriting a valid single-column file's footer.

_negative_offset_path = 'core/src/test/resources/negative_data_page_offset.parquet'
pq.write_table(
    pa.table({'id': [1, 2, 3]}, schema=pa.schema([('id', pa.int64(), False)])),
    _negative_offset_path,
    use_dictionary=False,
    compression=None,
)
corrupt_data_page_offset_negative(_negative_offset_path)
print("\nGenerated negative_data_page_offset.parquet:")
print("  - valid footer with data_page_offset = -1 (controlled-rejection fixture)")
