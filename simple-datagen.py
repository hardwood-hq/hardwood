import pyarrow as pa
import pyarrow.parquet as pq

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
