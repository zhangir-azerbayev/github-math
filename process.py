import os
import sys
import requests 
import json
import ndjson 
import gzip
import zstandard
from tqdm import tqdm

from pathlib import Path

RAW_DIR = "raw_files"
PROCESSED_DIR = "processed"
MIN_STARS = 5

def process_files(archives_dir, 
                  dest_dir, 
                  filter_fn,
                  creds):
    """
    `filter_fn` is String -> Bool intended to take a source file as input
    and output whether or not it should be included in the processed 
    dataset. 
    """
    Path(dest_dir).mkdir(parents=True, exist_ok=True)

    new_data = []
    stars_of_repo = dict()
    not_founds = set()

    count_raw = 0
    count_proc = 0

    for archive in os.listdir(archives_dir):
        with gzip.open(os.path.join(archives_dir, archive)) as f: 
            data = ndjson.load(f)
        
        # DONT FORGET YOU PUT THIS HERE FOR TESTING PY
        for line in tqdm(data[:800]): 
            repo_name = line["repo_name"] 

            if repo_name in stars_of_repo: 
                stars = stars_of_repo[repo_name]
            elif repo_name not in not_founds: 
                resp = requests.get("https://api.github.com/repos/" + repo_name, 
                        auth=creds)
                if resp.status_code == 404: 
                    not_founds.add(repo_name)
                    continue
                elif resp.status_code == 451: 
                    # Unavailable for legal reasons
                    not_founds.add(repo_name)
                    continue
                elif resp.status_code != 200: 
                    print(resp.status_code)
                    print(repo_name)
                    raise AssertionError("http request failed")

                metadata = json.loads(resp.content.decode("utf-8"))
                stars = metadata["stargazers_count"]
                stars_of_repo[repo_name] = stars

            text = line["content"]

            if stars >= MIN_STARS and filter_fn(text): 
                meta = {key: line[key] for key in line if key!="content"}
                meta["stars"] = stars
                new_line = {"text": text, "meta": meta}
                new_data.append(new_line)

        save_path = os.path.join(dest_dir, archive.replace(".gz", ".zst"))
        json_str = ndjson.dumps(new_data)
        json_bytes = json_str.encode('utf-8')
        with open(save_path, "wb") as f:
            cctx = zstandard.ZstdCompressor(level=10)
            with cctx.stream_writer(f, size=len(json_bytes)) as compressor: 
                compressor.write(json_bytes)

        count_raw += len(data)
        count_proc += len(new_data)

        # DONT FORGET YOU ADDED THIS FOR PYTHON TESTING
        break
    
    print(f"FOLLOWING REPOS 404'ED: {not_founds}")
    print(f"RAW FILES ANALYZED: {count_raw}")
    print(f"PROCESSED FILES PRODUCED: {count_proc}")
    
    return

def process_lean(creds): 
    archives_dir = os.path.join(RAW_DIR, "lean")
    dest_dir = os.path.join(PROCESSED_DIR, "lean")
    lean_filter_fn = lambda x: True 
    
    print("PROCESSING LEAN SUBSET")
    process_files(archives_dir, dest_dir, lean_filter_fn, creds)

def process_thy(creds): 
    """
    None of the isabelle stuff apart from AFP looks useful, leaving it out
    """
    archives_dir = os.path.join(RAW_DIR, "thy")
    dest_dir = os.path.join(PROCESSED_DIR, "thy")
    thy_filter_fn = lambda x: True 

    print("PROCESSING THY SUBSET")
    process_files(archives_dir, dest_dir, thy_filter_fn, creds)

def process_py(creds): 
    archives_dir = os.path.join(RAW_DIR, "py")
    dest_dir = os.path.join(PROCESSED_DIR, "py")
    
    substrings = ("import numpy", "from numpy", "import scipy", 
            "from scipy", "import sympy", "from sympy")

    py_filter_fn = lambda text: any(x in text for x in substrings)
    
    print("PROCESSING PY SUBSET")
    process_files(archives_dir, dest_dir, py_filter_fn, creds)

def main(): 
    creds = ("zhangir-azerbayev", os.environ["GITHUB_TOKEN"])
    #process_lean(creds)
    process_py(creds)

if __name__=="__main__": 
    main()
