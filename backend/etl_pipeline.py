import pandas as pd
import requests
import yaml
import os
import logging
import json
import time
import re
from io import StringIO
from bs4 import BeautifulSoup

def setup_logging():
    logging.basicConfig(filename="etl.log", level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s")

def extract_structured_blocks(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    results = []

    # ---- 1. JSON object or list ----
    json_blocks = re.findall(r'\{[\s\S]+?\}', text)
    for block in json_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, dict):
                results.append(data)
            elif isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        results.append(entry)
        except Exception:
            continue

    list_blocks = re.findall(r'\[[\s\S]+?\]', text)
    for block in list_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        results.append(entry)
        except Exception:
            continue

    # ---- 2. CSV-like tables ----
    csv_blocks = re.findall(r'((?:[\w" ]+,)+[\w" ]+\n(?:[^\n]*\n?)+)', text)
    for block in csv_blocks:
        try:
            df_csv = pd.read_csv(StringIO(block))
            results.extend(df_csv.to_dict(orient="records"))
        except Exception:
            continue

    # ---- 3. YAML-like blocks ----
    yaml_blocks = re.findall(r'(?:[a-zA-Z0-9_]+:\s[^\n]+\n(?:\s+- .+\n)*)+', text)
    for block in yaml_blocks:
        try:
            yaml_data = yaml.safe_load(block)
            if isinstance(yaml_data, dict):
                results.append(yaml_data)
        except Exception:
            continue

    # ---- 4. HTML: all tags with visible text ----
    soup = BeautifulSoup(text, 'html.parser')
    for tag in soup.find_all(True):
        tag_text = tag.get_text(strip=True)
        if tag_text and len(tag_text) > 3:
            row = {"_html_tag": tag.name, "_html_text": tag_text}
            for attr, val in tag.attrs.items():
                row[f"_html_attr_{attr}"] = str(val)
            results.append(row)

    # ---- 5. Code/Log blocks as raw ----
    raw_blocks = re.findall(r'(def .+?:\n(?:\s+.+\n)*|print\(.+\))', text)
    for code in raw_blocks:
        results.append({'_code_block': code.replace('\n', ' ')})
    log_blocks = re.findall(r'\[\d{4}-\d{2}-\d{2} .+?\] .+', text)
    for log in log_blocks:
        results.append({'_log_entry': log})

    # ---- 6. Everything else as raw text rows ----
    leftover = [
        blk for blk in text.split('\n\n') 
        if blk.strip() and 
        not any(blk in j for j in json_blocks + csv_blocks + raw_blocks + log_blocks + yaml_blocks)
    ]
    for rem in leftover:
        if len(rem.strip()) > 6 and not rem.strip().startswith('<'):
            results.append({'_raw_text': rem.strip()})

    if not results:
        raise ValueError("No extractable block found!")

    df = pd.json_normalize(results)
    print(f"[ETL DEBUG] Extracted {len(df)} merged records from text (all types)")
    return df

def extract(cfg):
    typ = cfg['type']
    src = cfg['source']
    retries = cfg.get('retry_count', 3)
    retry_delay = cfg.get('retry_delay', 1)
    attempt = 0

    if src.endswith('.txt') or src.endswith('.html') or src.endswith('.htm'):
        try:
            df = extract_structured_blocks(src)
            if len(df) == 0:
                raise ValueError("No valid data extracted from file")
            return df
        except Exception as e:
            print(f"[ETL DEBUG] TXT/HTML extract error: {e}")
            raise RuntimeError("No structured/tabular data found in file.")

    while attempt < retries:
        try:
            if typ == "csv":
                df = pd.read_csv(src, low_memory=False)
            elif typ == "json":
                with open(src, 'r', encoding='utf-8') as f:
                    obj = json.load(f)
                    if isinstance(obj, list):
                        df = pd.DataFrame(obj)
                    elif isinstance(obj, dict):
                        main_key = None
                        for k, v in obj.items():
                            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                                main_key = k
                                break
                        if main_key:
                            df = pd.json_normalize(obj[main_key])
                        else:
                            df = pd.json_normalize(obj)
                    else:
                        raise ValueError("Unsupported JSON structure")
            elif typ == "api":
                res = requests.get(src)
                res.raise_for_status()
                df = pd.json_normalize(res.json())
            else:
                raise ValueError(f"Unknown extract type: {typ}")
            print(f"[ETL DEBUG] Extracted shape: {df.shape}")
            return df
        except Exception as e:
            print(f"[ETL DEBUG] Extract error on attempt {attempt+1}: {e}")
            attempt += 1
            if attempt < retries:
                time.sleep(retry_delay)
    raise RuntimeError(f"Extraction failed after {retries} attempts.")

def flatten_value(val, parent_key="", sep="_"):
    items = []
    if isinstance(val, dict):
        for k, v in val.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_value(v, new_key, sep=sep).items())
    elif isinstance(val, list):
        if all(isinstance(i, dict) for i in val):
            for idx, e in enumerate(val):
                new_key = f"{parent_key}{sep}{idx}" if parent_key else f"{idx}"
                items.extend(flatten_value(e, new_key, sep=sep).items())
        else:
            aggregated = ",".join(str(i) for i in val)
            items.append((parent_key, aggregated))
    else:
        items.append((parent_key, val))
    return dict(items)

def flatten_dataframe(df):
    records = df.to_dict(orient="records")
    flattened_records = []
    for rec in records:
        flat = flatten_value(rec)
        if len(flat) == 0:
            flat = {"warning": "empty_record"}
        flattened_records.append(flat)
    return pd.DataFrame.from_records(flattened_records)

def is_hashable(val):
    try:
        hash(val)
        return True
    except Exception:
        return False

def transform(df, cfg):
    df = flatten_dataframe(df)
    hashable_cols = [c for c in df.columns if df[c].map(is_hashable).all()]
    if cfg.get('drop_duplicates', False) and hashable_cols:
        df = df.drop_duplicates(subset=hashable_cols)
    if cfg.get('dropna', False):
        df = df.dropna(how="all")
    try:
        df['num_columns'] = df.count(axis=1)
        df['num_nulls'] = df.isnull().sum(axis=1)
    except Exception:
        pass
    return df

def load(df, cfg):
    dest = cfg['destination']
    print(f"[ETL DEBUG] Saving output to: {dest}")
    out_dir = os.path.dirname(dest)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(dest, index=False)
    print(f"[ETL DEBUG] Saved {len(df)} records to {dest}")

def run_etl_pipeline():
    setup_logging()
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    df = extract(cfg['extract'])
    df = transform(df, cfg.get('transform', {}))
    load(df, cfg['load'])
    print("ETL complete. Output saved to:", cfg['load']['destination'])

if __name__ == "__main__":
    run_etl_pipeline()
