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
from dateutil.parser import parse as dateparse
from deepdiff import DeepDiff

def setup_logging():
    logging.basicConfig(filename="etl.log", level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

def primitive_only(val):
    # Converts pandas/NumPy types and non-serializable objects to Python types/strings
    import numpy as np
    if isinstance(val, (str, int, float, bool)) or val is None:
        return val
    elif isinstance(val, (np.generic,)):
        return val.item()
    try:
        return json.loads(json.dumps(val, default=str))
    except Exception:
        return str(val)

def flatten_value(val, parent_key="", sep="_"):
    items = []
    if isinstance(val, dict):
        for k, v in val.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_value(v, new_key, sep=sep).items())
    elif isinstance(val, list):
        items.append((parent_key, ",".join(map(str, [primitive_only(i) for i in val]))))
    else:
        items.append((parent_key, primitive_only(val)))
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

def extract_structured_blocks(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    results = []
    json_blocks = re.findall(r'\{[\s\S]+?\}', text)
    for block in json_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, dict):
                data['_source_type'] = 'json'
                results.append(data)
            elif isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        entry['_source_type'] = 'json'
                        results.append(entry)
        except:
            pass
    list_blocks = re.findall(r'\[[\s\S]+?\]', text)
    for block in list_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        entry['_source_type'] = 'json'
                        results.append(entry)
        except:
            pass
    csv_blocks = re.findall(r'((?:[\w" ]+,)+[\w" ]+\n(?:[^\n]*\n?)+)', text)
    for block in csv_blocks:
        try:
            df_csv = pd.read_csv(StringIO(block))
            for d in df_csv.to_dict(orient='records'):
                d['_source_type'] = 'csv'
                results.append(d)
        except:
            pass
    yaml_blocks = re.findall(r'(?:[a-zA-Z0-9_]+:\s[^\n]+\n(?:\s+- .+\n)*)+', text)
    for block in yaml_blocks:
        try:
            yaml_data = yaml.safe_load(block)
            if isinstance(yaml_data, dict):
                yaml_data['_source_type'] = 'yaml'
                results.append(yaml_data)
        except:
            pass
    soup = BeautifulSoup(text, 'html.parser')
    for tag in soup.find_all(True):
        tag_text = tag.get_text(strip=True)
        if tag_text and len(tag_text) > 3:
            row = {"_html_tag": tag.name, "_html_text": tag_text, "_source_type": "html"}
            for attr, val in tag.attrs.items():
                row[f"_html_attr_{attr}"] = str(val)
            results.append(row)
    raw_blocks = re.findall(r'(def .+?:\n(?:\s+.+\n)*|print\(.+\))', text)
    for code in raw_blocks:
        results.append({'_code_block': code.replace('\n', ' '), '_source_type': 'code'})
    log_blocks = re.findall(r'\[\d{4}-\d{2}-\d{2} .+?\] .+', text)
    for log in log_blocks:
        results.append({'_log_entry': log, '_source_type': 'log'})
    collected = json_blocks + csv_blocks + raw_blocks + log_blocks + yaml_blocks
    for rem in text.split('\n\n'):
        if rem.strip() and not any(rem in c for c in collected):
            results.append({'_raw_text': rem.strip(), '_source_type': 'raw'})
    # --- Robust fallback so output is never empty ---
    if not results:
        print("[ETL DEBUG] No extractable blocks found, returning error row")
        results.append({'_error': 'No extractable block found', '_source_type': 'error'})
    print(f"[ETL DEBUG] Total extracted blocks: {len(results)} | Breakdown:",
           dict(pd.Series([r.get('_source_type') for r in results]).value_counts()))
    df = pd.json_normalize(results)
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
                raise ValueError('No valid data extracted from file')
            print(f"[ETL DEBUG] Shape after extract: {df.shape}")
            return df
        except Exception as e:
            print(f"[ETL DEBUG] TXT/HTML extract error: {e}")
            return pd.DataFrame([{'_error': str(e), '_source_type': 'error'}])
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

def normalize_value(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).replace(',', '').replace('$', '').strip()
    if s.lower() in ('na', 'n/a', 'null', 'none', ''):
        return None
    try:
        return int(s)
    except:
        try:
            return float(s)
        except:
            try:
                dt = dateparse(s)
                return dt.isoformat()
            except:
                return val

def normalize_data(df):
    for col in df.columns:
        try:
            sample = df[col].dropna().astype(str).values[0]
            if len(sample) > 8 and re.match(r'\d{4}-\d{2}-\d{2}', sample):
                df[col] = df[col].map(normalize_value)
            elif df[col].dropna().map(lambda x: bool(re.match(r'^\$?\d+\.?\d*$', str(x).replace(",", "")) or pd.api.types.is_numeric_dtype(x))).all():
                df[col] = df[col].map(normalize_value)
            else:
                df[col] = df[col].fillna(None)
        except:
            continue
    print(f"[ETL DEBUG] Shape after normalize: {df.shape}")
    return df

def infer_type(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "null"
    s = str(val).strip()
    if s == "":
        return "null"
    if re.match(r"^\d+$", s):
        return "integer"
    if re.match(r"^\d*\.\d+$", s):
        return "float"
    if s.lower() in ("true", "false"):
        return "boolean"
    try:
        _ = dateparse(s)
        return "date"
    except:
        return "string"

def generate_schema(df):
    schema = {
        "schema_id": f"v{int(time.time())}",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "fields": [],
        "primary_key_candidates": [],
        "compatible_dbs": ["postgresql", "mongodb"]
    }
    for col in df.columns:
        col_data = df[col]
        types = col_data.map(infer_type).value_counts().to_dict()
        types.pop("null", None)
        t = max(types, key=types.get) if types else "string"
        nullable = bool(col_data.isnull().any())
        examples = [primitive_only(x) for x in list(col_data.dropna().unique()[:3])]
        confidence = float(types[t] / len(col_data)) if types else 1.0
        schema["fields"].append(
            {"name": col, "path": f"$.{col}", "type": t, "nullable": nullable, "examples": examples, "confidence": confidence}
        )
    schema["primary_key_candidates"] = [
        col for col in df.columns
        if bool(getattr(df[col], "is_unique", False)) and not df[col].isnull().any()
    ]
    return schema

def load_schema(source_id):
    path = f"schemas/{source_id}_schema.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as err:
            print("[ETL DEBUG] Could not load previous schema, skipping:", err)
            return None

def save_schema(source_id, schema):
    os.makedirs("schemas", exist_ok=True)
    path = f"schemas/{source_id}_schema.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    logging.info(f"Saved schema {schema['schema_id']} for source {source_id}")

def diff_schemas(old_schema, new_schema):
    return DeepDiff(old_schema, new_schema, ignore_order=True).to_dict()

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
    source_id = cfg.get("source_id", "default_source")
    df = extract(cfg["extract"])
    print(f"[ETL DEBUG] After extract: {df.shape}")
    df = normalize_data(df)
    df = flatten_dataframe(df)
    print(f"[ETL DEBUG] After flatten: {df.shape}")
    new_schema = generate_schema(df)
    old_schema = load_schema(source_id)
    schema_diff = None
    if old_schema:
        schema_diff = diff_schemas(old_schema, new_schema)
        logging.info(f"Schema diff for {source_id}: {schema_diff}")
    save_schema(source_id, new_schema)
    load(df, cfg["load"])  # data output
    print(f"ETL complete. Schema version: {new_schema['schema_id']}")
    if schema_diff:
        print(f"Schema changes: {schema_diff}")

if __name__ == "__main__":
    run_etl_pipeline()
