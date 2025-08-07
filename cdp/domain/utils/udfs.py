import pandas as pd
import pydash
import ast
import re
from typing import Dict

def extract_json(data, path, index=0):
    if isinstance(data, pd._libs.missing.NAType):
        return None
    if not data:
        return None
    if isinstance(data, dict):
        return pydash.get(data, path)
    if isinstance(data, str):
        dict_data = ast.literal_eval(data)
        return pydash.get(dict_data, path)
    if isinstance(data, list):
        d_data = pydash.get(data, index)
        return pydash.get(d_data, path)
    return pydash.get(data, path)

def convert_to_dict(x):
    try:
        return eval(str(x))
    except:
        return None

def lark_transform_mapping(df, mapping_dict, has_record_id = True, write_to_mariadb=True, sourced_from_lark=True):
    if sourced_from_lark:
        df["fields"] = df["fields"].map(lambda x: convert_to_dict(x))

    new_data = pd.DataFrame()

    for new_col, values in mapping_dict.items():
        path = values.get("path")
        if path is None:
            new_data[new_col] = None
        else:
            if sourced_from_lark:
                new_data[new_col] = df["fields"].apply(lambda row: extract_json(row, path))
            else:
                new_data[new_col] = df[path]

    if has_record_id and "record_id" in df.columns:
        new_data["record_id"] = df["record_id"].values

    for new_col, values in mapping_dict.items():
        type = values.get("type")
        if type == "int" or type == "float" or type == "double":
            new_data[new_col] = pd.to_numeric(new_data[new_col], errors="coerce")
        elif type == "ms_timestamp":
            new_data[new_col] = pd.to_datetime(new_data[new_col], utc=True, unit="ms").dt.tz_localize(None)
            if write_to_mariadb:
                new_data[new_col] = new_data[new_col].clip(upper=pd.Timestamp("2038-01-01 00:00:00"))
        elif type == "datetime":
            new_data[new_col] = pd.to_datetime(new_data[new_col], utc=True).dt.tz_localize(None)
        elif type == "date":
            format = values.get("format")
            new_data[new_col] = pd.to_datetime(new_data[new_col], utc=True, format=format).dt.tz_localize(None).dt.date
        elif type == "lark_date":
            new_data[new_col] = pd.to_numeric(new_data[new_col], errors='coerce')
            new_data[new_col] = pd.Timestamp('1900-01-01') + pd.to_timedelta(new_data[new_col] - 2, unit='D')
            new_data[new_col] = new_data[new_col].dt.tz_localize(None)
        elif type == 'user_email':
            new_data[new_col] = new_data[new_col].apply(
                lambda x: x[0].get('email') if x and isinstance(x, list) and isinstance(x[0], dict) else None
            )
        elif type == 'user_id':
            new_data[new_col] = new_data[new_col].apply(
                lambda x: x[0].get('id') if x and isinstance(x, list) and isinstance(x[0], dict) else None
            )
        elif type == 'lark_formula':
            new_data[new_col] = new_data[new_col].apply(
                lambda x: x[0].get('text') if x and isinstance(x, list) and isinstance(x[0], dict) else None
            )
        elif type == 'lark_user':
            new_data[new_col] = new_data[new_col].map(
                lambda x: [{'id': x}]
            )
        elif type == 'bool_int':
            new_data[new_col] = new_data[new_col].apply(lambda x: int(x) if pd.notnull(x) else None)
        elif type != "":
            new_data[new_col] = new_data[new_col].astype(str)

    return new_data

def gaql_query_handler(query: str):
    match = re.search(r"SELECT\s+(.+?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Invalid query! Could not parse 'SELECT ... FROM'. Query: {query}")

    raw_fields = match.group(1)
    field_mappings = {}
    cleaned_fields = []
    full_field_list = []

    for field in raw_fields.split(","):
        field = field.strip()
        parts = re.split(r"\s+AS\s+", field, flags=re.IGNORECASE)

        original_field = parts[0].strip()
        alias = parts[1].strip() if len(parts) > 1 else original_field.replace(".", "_")

        field_mappings[original_field] = alias
        cleaned_fields.append(original_field)
        full_field_list.append(original_field)

    new_query = f"SELECT {', '.join(cleaned_fields)} FROM" + query[match.end():]
    return new_query, field_mappings, full_field_list

