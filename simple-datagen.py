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
    'src/test/resources/nested_struct_test.parquet',
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
    'src/test/resources/list_basic_test.parquet',
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
    'src/test/resources/list_struct_test.parquet',
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
    'src/test/resources/nested_list_struct_test.parquet',
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
    'src/test/resources/deep_nested_struct_test.parquet',
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
    'src/test/resources/nested_list_test.parquet',
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
    'src/test/resources/address_book_test.parquet',
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
    'src/test/resources/triple_nested_list_test.parquet',
    use_dictionary=False,
    compression=None,
    data_page_version='1.0'
)

print("\nGenerated triple_nested_list_test.parquet:")
print("  - Schema: id, cube: list<list<list<int32>>>")
print("  - Data: 5 rows with 3-level nested lists")
