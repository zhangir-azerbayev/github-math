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
                  substrings,
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

    for archive in tqdm(os.listdir(archives_dir)):
        with gzip.open(os.path.join(archives_dir, archive)) as f: 
            data = ndjson.load(f)

        if substrings:
            data = list(filter(lambda x: any(y in x["content"] for y in substrings), data))
 
        for line in tqdm(data): 
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
                    print("WARNING: HTTP REQUEST FAILED")
                    print(f"RESP STATUS CODE{resp.status_code}")
                    print(repo_name)
                    not_founds.add(repo_name)
                    continue 

                metadata = json.loads(resp.content.decode("utf-8"))
                stars = metadata["stargazers_count"]
                stars_of_repo[repo_name] = stars

            text = line["content"]
            
            if stars >= MIN_STARS: 
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

    
    print(f"FOLLOWING REPOS 404'ED: {not_founds}")
    print(f"RAW FILES ANALYZED: {count_raw}")
    print(f"PROCESSED FILES PRODUCED: {count_proc}")
    
    return

def process_lean(creds): 
    archives_dir = os.path.join(RAW_DIR, "lean")
    dest_dir = os.path.join(PROCESSED_DIR, "lean")
    lean_filter_fn = lambda x: True 
    
    print("PROCESSING LEAN SUBSET")
    process_files(archives_dir, dest_dir, None, creds)

def process_sage(creds): 
    archives_dir = os.path.join(RAW_DIR, "sage")
    dest_dir = os.path.join(PROCESSED_DIR, "sage")
    
    print("PROCESSING SAGE SUBSET") 
    process_files(archives_dir, dest_dir, None, creds)

def process_py(creds): 
    archives_dir = os.path.join(RAW_DIR, "py")
    dest_dir = os.path.join(PROCESSED_DIR, "py")
    
    substrings = ("import numpy", "from numpy", "import scipy", 
            "from scipy", "import sympy", "from sympy")

    
    print("PROCESSING PY SUBSET")
    process_files(archives_dir, dest_dir, substrings, creds)

""" 
FILTER FN is a bad way of doing things
Just have if statements in the code for the different
languages
"""
def main(): 
    creds = ("zhangir-azerbayev", os.environ["GITHUB_TOKEN"])
    #process_lean(creds)
    process_py(creds)
    #process_sage(creds)

if __name__=="__main__": 
    main()
