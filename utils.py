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
    spark = SparkSession.builder.master("local").appName("Validator").getOrCreate()
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

            try:
                value = new_row[col_name]
                if dtype == "Integer":
                    int(value)
                elif dtype == "Double":
                    float(value)
                elif dtype == "Date":
                    pd.to_datetime(value, format=format)
            except:
                valid = False
                errors[col_name] = f"Invalid {dtype} or format"

        if valid:
            valid_rows.append(new_row)
        else:
            new_row["__errors__"] = str(errors)
            invalid_rows.append(new_row)

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    return valid_df, invalid_df
