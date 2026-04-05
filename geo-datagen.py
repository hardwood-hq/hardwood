#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import struct
import pyarrow as pa
import pyarrow.parquet as pq

def wkb_point(x, y):
    # WKB Point: byte order (1=little endian) + type (1=Point) + x + y
    return struct.pack('<BIdd', 1, 1, x, y)

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

def _geometry_logical_type():
    # LogicalType union field 17 = GeometryType (empty struct with optional crs)
    w = _ThriftWriter()
    w.f(17, _T_STRUCT).end()  # GeometryType with no crs (defaults to OGC:CRS84)
    w.end()
    return w.out()

enc_map = {'PLAIN': 0, 'RLE': 3, 'RLE_DICTIONARY': 8}

# test data for geospatial metadata reading
# define the 3 row groups data
europe_names = ['London', 'Paris', 'Berlin']
europe_points = [wkb_point(-0.12, 51.50), wkb_point(2.35, 48.85), wkb_point(13.40, 52.52)]
asia_names = ['Tokyo', 'Beijing', 'Mumbai']
asia_points = [wkb_point(139.69, 35.68), wkb_point(116.39, 39.91), wkb_point(72.87, 19.07)]
america_names = ['New York', 'Chicago', 'LA']
america_points = [wkb_point(-74.00, 40.71), wkb_point(-87.62, 41.87), wkb_point(-118.24, 34.05)]

# write base file with 3 row groups:
geo_schema = pa.schema([pa.field('city_name', pa.string(), False), pa.field('location', pa.binary(), False)])
geo_base_path = '/tmp/_geo_e2e_base.parquet'
writer = pq.ParquetWriter(geo_base_path, geo_schema, use_dictionary=False, compression=None, data_page_version='1.0')
writer.write_table(pa.table({'city_name': europe_names, 'location': europe_points}, schema=geo_schema))
writer.write_table(pa.table({'city_name': asia_names, 'location': asia_points}, schema=geo_schema))
writer.write_table(pa.table({'city_name': america_names, 'location': america_points}, schema=geo_schema))
writer.close()

# capture pre_footer and all 3 row groups
base_pf = pq.ParquetFile(geo_base_path)
base_rg0 = base_pf.metadata.row_group(0)  # Europe
base_rg1 = base_pf.metadata.row_group(1)  # Asia
base_rg2 = base_pf.metadata.row_group(2)  # Americas

# capture pre_footer from geo_base_path:
with open(geo_base_path, 'rb') as f:
    base_data = f.read()
base_footer_len = struct.unpack('<I', base_data[-8:-4])[0]
pre_footer = base_data[:len(base_data) - 8 - base_footer_len]

# Build FileMetaData with column-level kv metadata
fm = _ThriftWriter()
fm.f(1, _T_I32).i32(2)
fm.f(2, _T_LIST).lst(_T_STRUCT, 3)
fm.raw(_schema_elem("schema", num_children=2))
fm.raw(_schema_elem("city_name", type_val=6, rep=0))
fm.raw(_schema_elem("location", type_val=6, rep=0, logical_type=_geometry_logical_type()))
fm.f(3, _T_I64).i64(9)
fm.f(4, _T_LIST).lst(_T_STRUCT, 3)

continent_bbox = {0: [-0.12, 13.40, 48.85, 52.52], 1: [72.87, 139.69, 19.07, 39.91], 2: [-118.24, -74.00, 34.05, 41.87]}
for rgi in range(3):
    rw = _ThriftWriter()
    rw.f(1, _T_LIST).lst(_T_STRUCT, 2)
    base_rg = base_pf.metadata.row_group(rgi)
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
            bbox.f(1, _T_DOUBLE).double(continent_bbox[rgi][0])  # xmin
            bbox.f(2, _T_DOUBLE).double(continent_bbox[rgi][1])   # xmax
            bbox.f(3, _T_DOUBLE).double(continent_bbox[rgi][2])  # ymin
            bbox.f(4, _T_DOUBLE).double(continent_bbox[rgi][3])  # ymax
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
with open('core/src/test/resources/geospatial_e2e_test.parquet', 'wb') as f:
    f.write(output)

print("\nGenerated _geo_e2e_base.parquet:")
print("  - city_geom column has GeospatialStatistics at field 17")
