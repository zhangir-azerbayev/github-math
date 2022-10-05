import ndjson
import zstandard

path = 'sage/sage_files_000000000000.jsonl.zst'
with open(path, 'rb') as f: 
    json_bytes = f.read()
    dctx = zstandard.ZstdDecompressor()
    json_str = dctx.decompress(json_bytes)

    data = ndjson.loads(json_str)

so_far = set()
for x in data: 
    repo_name = x["meta"]["repo_name"]
    if repo_name not in so_far: 
        print(repo_name)
        so_far.add(repo_name)
    print("#"*80)
    print(x["text"])
