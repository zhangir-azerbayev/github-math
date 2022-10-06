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

def create_stars_of_repo(tuples, creds): 
    """
    `tuples` is a list of (owner, repo_name)

    the key for a repo is "repo" + {hash of owner/repo_name}
    """

    subqueries = [
            f"""\
    repo{i}: repository(owner: "{x[0]}", name: "{x[1]}") {{
        ...repoProperties
    }}""" for i, x in enumerate(tuples)
            ]

    key_of_repo = {x[0] + "/" + x[1]: "repo" + str(i) for i, x in enumerate(tuples)}

    query = """\
fragment repoProperties on Repository {
    stargazers {
            totalCount
        }
    }

query {
""" + "\n".join(subqueries) + "\n}"

    resp = requests.post(
            url = "https://api.github.com/graphql", 
            json = {"query": query}, 
            auth=creds
            )

    print(resp)

    resp_json = json.loads(resp.content.decode('utf-8'))["data"]
    
    print(resp_json)

    stars_of_key = {x: resp_json[x]['stargazers']['totalCount'] for x in resp_json if resp_json[x]}

    stars_of_repo = {repo: stars_of_key[key_of_repo[repo]] for repo in key_of_repo if key_of_repo[repo] in stars_of_key}
    
    print("NO METADATA FOR FOLLOWING REPOS FOUND")
    no_metadata = [repo for repo in key_of_repo if key_of_repo[repo] not in stars_of_key]
    print(no_metadata)

    return stars_of_repo

        
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

    count_raw = 0
    count_proc = 0
    new_count_proc = 0 

    for archive in tqdm(os.listdir(archives_dir)):
        with gzip.open(os.path.join(archives_dir, archive)) as f: 
            data = ndjson.load(f)
    
        # builds hashmap from repo_name to number of stars
        tuples = [y.split("/") for y in list(set([x["repo_name"] for x in data]))]
        stars_of_repo = create_stars_of_repo(tuples, creds)

        # filters out source files that don't contain keywords
        if substrings:
            data = list(filter(lambda x: any(y in x["content"] for y in substrings), data))
 
        for line in tqdm(data): 
            repo_name = line["repo_name"] 

            if repo_name in stars_of_repo: 
                stars = stars_of_repo[repo_name]
            else: 
                continue 

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
