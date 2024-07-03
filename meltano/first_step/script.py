from datetime import datetime
import pandas as pd
import os
import yaml

def script(tables, date):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    meltano_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
    meltano_file = os.path.join(meltano_dir, "first_step/meltano.yml")
    os.chdir(script_dir)

    with open(meltano_file, "r") as file:
        config = yaml.safe_load(file)

    # PostgreSQL to JSONL
    for table in tables:
        print(f"Running ETL for table: {table}")

        config["plugins"]["extractors"][0]["select"] = [f"public-{table}.*"]

        with open(meltano_file, "w") as file:
            yaml.safe_dump(config, file)

        destination_path = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/postgres/{table}/")
        os.system(f"meltano config target-jsonl set destination_path {destination_path}")
        os.system(f"meltano config target-jsonl set custom_name {date}")
        os.system("meltano run tap-postgres target-jsonl")

    # CSV to JSONL
    destination_path = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), "data/csv/order_details/")
    os.system(f"meltano config target-jsonl set destination_path {destination_path}")
    os.system(f"meltano config target-jsonl set custom_name {date}")
    os.system("meltano run tap-csv target-jsonl")

def convert_jsonl_to_csv(tables, date):
    for table in tables:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        meltano_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
        jsonl_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/postgres/{table}/{date}.jsonl")
        csv_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/postgres/{table}/{date}.csv")

        if os.path.exists(jsonl_file):
            df = pd.read_json(jsonl_file, lines=True)
            df.to_csv(csv_file, index=False)
            os.remove(jsonl_file)

    jsonl_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/csv/order_details/{date}.jsonl")
    csv_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/csv/order_details/{date}.csv")

    if os.path.exists(jsonl_file):
        df = pd.read_json(jsonl_file, lines=True)
        df.to_csv(csv_file, index=False)
        os.remove(jsonl_file)

current_date = datetime.now().strftime('%Y-%m-%d')
tables = ["categories", "customer_customer_demo", "customer_demographics", "customers", "employee_territories", "employees", "orders", "products", "region", "shippers", "suppliers", "territories", "us_states"]

script(tables, current_date)
convert_jsonl_to_csv(tables, current_date)