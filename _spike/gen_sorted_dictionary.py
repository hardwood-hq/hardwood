import sys, numpy as np, pyarrow as pa, pyarrow.parquet as pq
rows, distinct, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
rng = np.random.default_rng(42)
# ~40-byte URLs whose sorted order differs from the numeric suffix order
keys = rng.permutation(distinct)
dictionary = sorted(f"https://example.com/{k * 2654435761 % 10**9:09d}/item-{k:07d}" for k in keys)
dict_arr = pa.array(dictionary, pa.string())
rg = 1 << 20
with pq.ParquetWriter(out, pa.schema([("id", pa.int64()), ("url", pa.dictionary(pa.int32(), pa.string()))]),
                      dictionary_pagesize_limit=256 << 20, compression="snappy") as w:
    for start in range(0, rows, rg):
        n = min(rg, rows - start)
        idx = pa.array(rng.integers(0, distinct, n, dtype=np.int32))
        url = pa.DictionaryArray.from_arrays(idx, dict_arr)
        w.write_table(pa.table({"id": pa.array(np.arange(start, start + n, dtype=np.int64)), "url": url}), row_group_size=rg)
