"""
reader.py — Reads CSV, JSON, XML, YAML, or database tables into a pandas DataFrame.
"""
import os
import json
import pandas as pd


def read_file(path: str) -> pd.DataFrame:
    """Read a file and return a DataFrame. Raises ValueError for unsupported formats."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return _read_csv(path)
    elif ext == ".json":
        return _read_json(path)
    elif ext == ".xml":
        return _read_xml(path)
    elif ext in (".yaml", ".yml"):
        return _read_yaml(path)
    else:
        raise ValueError(f"Unsupported file format: '{ext}'. Supported: csv, json, xml, yaml/yml")


def read_db(connection_string: str, query: str) -> pd.DataFrame:
    """Read from a database using SQLAlchemy. Requires sqlalchemy installed."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        raise ImportError("sqlalchemy is required for database reading. Run: pip install sqlalchemy")

    engine = create_engine(connection_string)
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# --- private helpers ---

def _read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    if df.empty:
        raise ValueError(f"CSV file is empty: {path}")
    return df


def _read_json(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # Try records orientation or wrap single object
        if all(isinstance(v, list) for v in data.values()):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
    else:
        raise ValueError("JSON must be an object or array of objects.")

    if df.empty:
        raise ValueError(f"JSON file produced an empty table: {path}")
    return df.astype(str)


def _read_xml(path: str) -> pd.DataFrame:
    try:
        df = pd.read_xml(path)
    except Exception as e:
        raise ValueError(f"Failed to parse XML: {e}")
    if df.empty:
        raise ValueError(f"XML file is empty: {path}")
    return df.astype(str)


def _read_yaml(path: str) -> pd.DataFrame:
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml is required for YAML reading. Run: pip install pyyaml")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        raise ValueError("YAML must be a mapping or list of mappings.")

    if df.empty:
        raise ValueError(f"YAML file is empty: {path}")
    return df.astype(str)
