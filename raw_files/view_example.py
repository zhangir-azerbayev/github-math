import gzip 
import ndjson

path = 'lean/lean_files_000000000000.jsonl.gz'
with gzip.open(path) as f: 
    data = ndjson.load(f)

print(data[0])
print(data[0].keys())
