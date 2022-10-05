import ndjson
import zstandard

path = 'lean/lean_files_000000000000.jsonl.zst'
with open(path, 'rb') as f: 
    json_bytes = f.read()
    dctx = zstandard.ZstdDecompressor()
    json_str = dctx.decompress(json_bytes)

    data = ndjson.loads(json_str)


print(data[0])
print(data[0].keys())
