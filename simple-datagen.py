#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, time, timezone
from decimal import Decimal
import uuid

from parquet_bson_annotation import annotate_column_as_bson
from parquet_variant_annotation import annotate_group_as_variant

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
print("  - Data: 5 rows with varying map sizes including empty and null")

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
_T_I32, _T_I64, _T_BIN, _T_LIST, _T_STRUCT = 0x05, 0x06, 0x08, 0x09, 0x0C

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
    def s(self, v):    b = v.encode(); self.buf += _write_varint(len(b)) + b; return self
    def lst(self, t, n):
        self.buf += bytes([(n << 4) | t]) if n < 15 else (bytes([0xF0 | t]) + _write_varint(n))
        return self
    def raw(self, d):  self.buf += d; return self
    def end(self):     self.buf += _STOP; return self
    def out(self):     return bytes(self.buf)

def _kv_struct(k, v):
    w = _ThriftWriter(); w.f(1,_T_BIN).s(k).f(2,_T_BIN).s(v).end(); return w.out()

# SchemaElement fields: 1=type, 3=repetition_type, 4=name, 5=num_children, 6=converted_type
def _schema_elem(name, type_val=None, rep=None, num_children=None, ct=None):
    w = _ThriftWriter()
    if type_val is not None: w.f(1, _T_I32).i32(type_val)
    if rep is not None:      w.f(3, _T_I32).i32(rep)
    w.f(4, _T_BIN).s(name)
    if num_children is not None: w.f(5, _T_I32).i32(num_children)
    if ct is not None:       w.f(6, _T_I32).i32(ct)
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
