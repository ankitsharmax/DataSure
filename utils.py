import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import *

WORKFLOW_DIR = "workflows"

def save_workflow(name, columns):
    os.makedirs(WORKFLOW_DIR, exist_ok=True)
    with open(os.path.join(WORKFLOW_DIR, f"{name}.json"), "w") as f:
        json.dump(columns, f)

def load_workflows():
    workflows = {}
    for file in os.listdir(WORKFLOW_DIR):
        if file.endswith(".json"):
            with open(os.path.join(WORKFLOW_DIR, file), "r") as f:
                workflows[file.replace(".json", "")] = json.load(f)
    return workflows

def apply_workflow(df, schema_def):
    spark = SparkSession.builder.master("local").appName("DataSure").getOrCreate()
    required_columns = [col_def["name"] for col_def in schema_def]
    df = df[required_columns]
    sdf = spark.createDataFrame(df.astype(str))

    valid_rows = []
    invalid_rows = []

    for row in sdf.collect():
        valid = True
        errors = {}
        new_row = row.asDict()
        for col_def in schema_def:
            col_name = col_def["name"]
            dtype = col_def["type"]
            format = col_def.get("format", "")
            required = col_def.get("required", True)

            value = new_row.get(col_name, None)

            # Handle required fields
            if required:
                if value is None or value == "" or value.lower() == "nan":
                    valid = False
                    errors[col_name] = "Required value missing"
                    continue

            # If optional and empty, skip further validation
            if not required and (value is None or value == "" or value.lower() == "nan"):
                continue

            try:
                if dtype == "Integer":
                    int(value)
                elif dtype == "Double":
                    float(value)
                elif dtype == "Date":
                    pd.to_datetime(value, format=format)
                # No special validation needed for String type here
            except Exception as e:
                valid = False
                errors[col_name] = f"Invalid {dtype} or format"

        if valid:
            valid_rows.append(new_row)
        else:
            new_row["__errors__"] = json.dumps(errors)
            invalid_rows.append(new_row)

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    # Rename columns from 'name' to 'db_name'
    rename_map = {col_def["name"]: col_def["db_name"] for col_def in schema_def}
    valid_df = valid_df.rename(columns=rename_map)
    invalid_df = invalid_df.rename(columns=rename_map)

    return valid_df, invalid_df

def transform_column(value, dtype, fmt_in="", fmt_out="%Y-%m-%d"):
    try:
        if dtype == "Integer":
            return int(value)
        elif dtype == "Double":
            return float(value)
        elif dtype == "Date":
            dt = pd.to_datetime(value, format=fmt_in, errors="coerce")
            if pd.isna(dt):
                return pd.NA
            return dt.strftime(fmt_out)  # Always output in hardcoded format
        else:
            return str(value)
    except Exception:
        return pd.NA
