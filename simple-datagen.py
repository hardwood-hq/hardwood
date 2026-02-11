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
    'core/src/test/resources/logical_types_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated logical_types_test.parquet:")
print("  - Encoding: PLAIN (use_dictionary=False)")
print("  - Compression: UNCOMPRESSED (compression=None)")
print("  - Data: 3 rows with various logical types (DATE, TIMESTAMP, TIME, DECIMAL, INT_8/16/32/64, UINT_8/16/32/64, UUID)")

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
