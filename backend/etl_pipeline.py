import os
import pandas as pd
import yaml
import json
import time
import re
import logging
from io import StringIO
from bs4 import BeautifulSoup
from dateutil.parser import parse as dateparse
from deepdiff import DeepDiff
import numpy as np

logging.basicConfig(level=logging.DEBUG)

def read_file_content(file_path):
    print("[DEBUG]: Provided path:", file_path)
    print("[DEBUG]: Current working directory:", os.getcwd())
    print("[DEBUG]: File exists at path?", os.path.exists(file_path))
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.txt', '.md'):
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
        print("[DEBUG]: Extracted text from .txt/.md preview:")
        print(repr(text[:300]))
        return text
    elif ext == '.pdf':
        text = ""
        try:
            import pdfplumber
            with pdfplumber.open(file_path) as pdf:
                text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
                print("[DEBUG]: pdfplumber extracted PDF preview:")
                print(repr(text[:300]))
        except Exception as e:
            print("[DEBUG]: pdfplumber extraction failed, error:", e)
        if not text.strip():
            try:
                from PyPDF2 import PdfReader
                reader = PdfReader(file_path)
                text = '\n'.join(page.extract_text() or '' for page in reader.pages)
                print("[DEBUG]: PyPDF2 fallback extracted PDF preview:")
                print(repr(text[:300]))
            except Exception as e2:
                print("[DEBUG]: PyPDF2 extraction failed, error:", e2)
        if not text.strip():
            raise Exception("No extractable text found in PDF. Is this a scanned/image PDF or empty? Extraction failed.")
        return text
    else:
        raise Exception(f"Unsupported file type: {ext}")

def primitive_only(val):
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
    text = read_file_content(file_path)
    results = []
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            results.append(obj)
        elif isinstance(obj, list):
            for rec in obj:
                if isinstance(rec, dict):
                    results.append(rec)
        print('[ETL DEBUG] Loaded as strict JSON')
    except Exception:
        pass
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
    if not results:
        results.append({'_error': 'No extractable block found', '_source_type': 'error'})
    print("[ETL DEBUG] Total extracted blocks:", len(results), "Type breakdown:", dict(pd.Series([r.get('_source_type') for r in results]).value_counts()))
    df = pd.json_normalize(results)
    return df

def extract(cfg):
    src = cfg['source']
    df = extract_structured_blocks(src)
    print(f"[ETL DEBUG] Shape after extract: {df.shape}")
    return df

def normalize_value(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    s_lower = s.lower()
    
    # Null or empty values
    if s_lower in ('na', 'n/a', 'null', 'none', '', 'nan'):
        return None
    
    # Boolean values
    if s_lower in ('true', 'yes', '1'):
        return True
    if s_lower in ('false', 'no', '0'):
        return False
    
    # Percentages - convert to float 0-1
    percent_match = re.match(r'^(\d+(\.\d+)?)%$', s)
    if percent_match:
        try:
            return float(percent_match.group(1)) / 100.0
        except Exception as e:
            logging.debug(f"normalize_value: Could not convert percentage '{s}': {e}")
            return s
    
    # Email normalization - lowercase and strip spaces
    if re.match(r'^[\w\.\+-]+@[\w\.-]+\.[a-zA-Z]{2,}$', s):
        return s_lower
    
    # Phone numbers - simple normalization by removing non-digit chars
    if re.match(r'^\+?[\d\s\-\(\)]+$', s):
        digits = re.sub(r'[^\d]', '', s)
        return digits
    
    # Currency removal and number parsing
    s_no_currency = s.replace(',', '').replace('$', '').replace('₹','').strip()
    try:
        return int(s_no_currency)
    except:
        try:
            return float(s_no_currency)
        except:
            try:
                dt = dateparse(s)
                return dt.isoformat()
            except Exception as e:
                return s_lower

def normalize_data(df):
    for col in df.columns:
        try:
            sample_values = df[col].dropna().astype(str).values
            if len(sample_values) == 0:
                continue
            sample = sample_values[0]
            # Guess if date col by regex
            if len(sample) > 8 and re.match(r'\d{4}-\d{2}-\d{2}', sample):
                df[col] = df[col].map(normalize_value)
            elif df[col].dropna().map(lambda x: bool(re.match(r'^(\$|₹)?\d+(\.\d+)?%?$', str(x).replace(",", "")) or pd.api.types.is_numeric_dtype(x))).all():
                df[col] = df[col].map(normalize_value)
            else:
                # Trim and lowercase strings
                df[col] = df[col].fillna('').map(lambda x: str(x).strip().lower() if pd.notna(x) else None)
        except Exception as e:
            logging.debug(f"normalize_data: Skipping column '{col}' due to error: {e}")
            continue
    logging.debug(f"[ETL DEBUG] Shape after normalize: {df.shape}")
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
        schema["fields"].append({
            "name": col,
            "path": f"$.{col}",
            "type": t,
            "nullable": nullable,
            "examples": examples,
            "confidence": confidence
        })
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

def run_etl_pipeline():
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
        schema_diff = DeepDiff(old_schema, new_schema, ignore_order=True).to_dict()
        print(f"[ETL DEBUG] Schema diff for {source_id}: {schema_diff}")
    save_schema(source_id, new_schema)
    out_path = cfg['load']['destination']
    out_dir = os.path.dirname(out_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(out_path, index=False)
    print(f"[ETL DEBUG] Saved {len(df)} records to {out_path}")
    print(f"ETL complete. Schema version: {new_schema['schema_id']}")
    if schema_diff:
        print(f"Schema changes: {schema_diff}")

if __name__ == "__main__":
    run_etl_pipeline()
