#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Derives the footer-rewritten variants of the predicate audit fixtures, and writes the PyArrow file.

Run after `PredicateAudit fixtures <dir>`, with the fixture directory as the only argument:
- legacy_*: flat_* with every modern logicalType removed where a converted_type is set
- dropped_*: flat_* with annotations the physical type cannot carry
- ts12_*: the FIXED_LEN_BYTE_ARRAY(12) columns annotated TIMESTAMP, with bounds in the order of the values
- zerobloom_bloom: flat_bloom with every Bloom filter bitset zeroed
- shapes: the v and sv groups annotated as VARIANT
- pyarrow_nan: NaN outside the float bounds, signed zeros and TIMESTAMP(NANOS), as PyArrow writes them
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from thriftpy2.protocol.compact import TCompactProtocolFactory  # noqa: E402
from thriftpy2.transport import TMemoryBuffer  # noqa: E402

import parquet_annotators as annotators  # noqa: E402

P = annotators._parquet
LAYOUTS = ['single', 'multi', 'dict', 'bloom']


def legacy(directory):
    for layout in LAYOUTS:
        before, footer = annotators._read_parquet_footer(str(directory / f'flat_{layout}.parquet'))
        for element in footer.schema[1:]:
            if element.converted_type is not None:
                element.logicalType = None
        annotators._write_parquet_footer(str(directory / f'legacy_{layout}.parquet'), before, footer)


def dropped(directory):
    millis = P.TimeUnit(MILLIS=P.MilliSeconds())
    micros = P.TimeUnit(MICROS=P.MicroSeconds())
    for layout in LAYOUTS:
        before, footer = annotators._read_parquet_footer(str(directory / f'flat_{layout}.parquet'))
        for element in footer.schema[1:]:
            if element.name == 'i64':
                element.logicalType = P.LogicalType(TIME=P.TimeType(isAdjustedToUTC=True, unit=millis))
            elif element.name == 'i32':
                element.logicalType = P.LogicalType(DECIMAL=P.DecimalType(scale=2, precision=12))
                element.converted_type = P.ConvertedType.DECIMAL
                element.scale = 2
                element.precision = 12
            elif element.name == 'flba4':
                element.logicalType = P.LogicalType(FLOAT16=P.Float16Type())
            elif element.name == 'uuid':
                element.logicalType = P.LogicalType(TIMESTAMP=P.TimestampType(isAdjustedToUTC=True, unit=micros))
                element.converted_type = None
            elif element.name == 'str':
                element.logicalType = P.LogicalType(UUID=P.UUIDType())
                element.converted_type = None
        annotators._write_parquet_footer(str(directory / f'dropped_{layout}.parquet'), before, footer)


TS12_COLUMNS = {
    'ts12_ns': ('NANOS', True),
    'ts12_us_local': ('MICROS', False),
    'ts12_ms': ('MILLIS', True),
}


def ts12(directory):
    """Annotates the ts12_* columns as TIMESTAMP and gives each row group bounds in the order of the values.

    No parquet-java release up to 1.18.1 writes the annotation, so the pinned one writes the columns as plain
    FIXED_LEN_BYTE_ARRAY(12), whose bounds it records in unsigned byte order. The chunk bounds are replaced by
    ones read off the stored values, signed little-endian counts, and the page index, recorded in the same
    unsigned order, is unlinked.
    Statistics stay enabled in the writer: disabled, parquet-java before 1.18.0 writes num_nulls = -1 into v2
    page headers (apache/parquet-java#3585, hardwood-hq/hardwood#1225).
    """
    for layout in LAYOUTS:
        path = str(directory / f'ts12_{layout}.parquet')
        stored = pq.ParquetFile(path)
        bounds = []
        for row_group in range(stored.num_row_groups):
            table = stored.read_row_group(row_group, columns=list(TS12_COLUMNS))
            bounds.append({name: [int.from_bytes(v, 'little', signed=True)
                                  for v in table.column(name).to_pylist() if v is not None]
                           for name in TS12_COLUMNS})
        for name, (unit, utc) in TS12_COLUMNS.items():
            annotators.annotate_element_at_path_as_timestamp(path, [name], unit=unit, is_adjusted_to_utc=utc)
        before, footer = annotators._read_parquet_footer(path)
        for row_group, values in zip(footer.row_groups, bounds):
            for chunk in row_group.columns:
                name = chunk.meta_data.path_in_schema[0]
                if name not in values:
                    continue
                counts = values[name]
                statistics = chunk.meta_data.statistics
                statistics.min = statistics.max = None
                statistics.min_value = min(counts).to_bytes(12, 'little', signed=True)
                statistics.max_value = max(counts).to_bytes(12, 'little', signed=True)
                chunk.column_index_offset = chunk.column_index_length = None
        annotators._write_parquet_footer(path, before, footer)


def zeroed_bloom(directory):
    source = directory / 'flat_bloom.parquet'
    data = bytearray(source.read_bytes())
    _, footer = annotators._read_parquet_footer(str(source))
    for row_group in footer.row_groups:
        for chunk in row_group.columns:
            offset = chunk.meta_data.bloom_filter_offset
            if offset is None:
                continue
            header = P.BloomFilterHeader()
            header.read(TCompactProtocolFactory().get_protocol(TMemoryBuffer(bytes(data[offset:offset + 256]))))
            encoded = TMemoryBuffer()
            header.write(TCompactProtocolFactory().get_protocol(encoded))
            start = offset + len(encoded.getvalue())
            data[start:start + header.numBytes] = bytes(header.numBytes)
    (directory / 'zerobloom_bloom.parquet').write_bytes(bytes(data))


def shapes(directory):
    path = str(directory / 'shapes.parquet')
    annotators.annotate_group_at_path_as_variant(path, ['v'])
    annotators.annotate_group_at_path_as_variant(path, ['sv'])


def pyarrow_nan(directory):
    rows = list(range(8))
    values = [1.0, 2.0, float('nan'), 3.0, 10.0, 11.0, 12.0, 13.0]
    zeros = [0.0, 1.0, 2.0, 3.0, -0.0, -1.0, -2.0, -3.0]
    table = pa.table({
        '__row__': pa.array(rows, pa.int64()),
        'd': pa.array(values, pa.float64()),
        'z': pa.array(zeros, pa.float64()),
        'tsn': pa.array([1_700_000_000_000_000_000 + r for r in rows], pa.timestamp('ns', tz='UTC')),
    })
    pq.write_table(table, str(directory / 'pyarrow_nan.parquet'), row_group_size=4, write_page_index=True,
                   use_dictionary=False)


if __name__ == '__main__':
    target = Path(sys.argv[1])
    legacy(target)
    dropped(target)
    ts12(target)
    zeroed_bloom(target)
    shapes(target)
    pyarrow_nan(target)
    print(f'derived fixtures in {target}')
