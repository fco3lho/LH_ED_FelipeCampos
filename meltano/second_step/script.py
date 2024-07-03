from datetime import datetime
import os
import yaml
import csv

current_date = datetime.now().strftime('%Y-%m-%d')
tables = ["categories", "customer_customer_demo", "customer_demographics", "customers", "employee_territories", "employees", "orders", "products", "region", "shippers", "suppliers", "territories", "us_states"]
headers = []

script_dir = os.path.dirname(os.path.abspath(__file__))
meltano_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
meltano_file = os.path.join(meltano_dir, "second_step/meltano.yml")
os.chdir(script_dir)

with open(meltano_file, "r") as file:
    config = yaml.safe_load(file)

# Postgres to Postgres
for table in tables:
  csv_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/postgres/{table}/{current_date}.csv")

  if os.path.exists(csv_file):
    with open(csv_file, mode='r', encoding='utf-8') as file:
      reader = csv.reader(file)
      headers = next(reader)

    config["plugins"]["extractors"][0]["config"]["files"][0]["keys"] = headers
    config["plugins"]["extractors"][0]["config"]["files"][0]["file"] = f"{current_date}.csv"
    config["plugins"]["extractors"][0]["config"]["files"][0]["entity"] = f"{table}"
    config["plugins"]["extractors"][0]["config"]["files"][0]["path"] = f"../../data/postgres/{table}"
    config["plugins"]["extractors"][0]["select"] = [f"{table}.*"]

    with open(meltano_file, "w") as file:
      yaml.safe_dump(config, file)

    os.system("meltano run tap-csv target-postgres")

# CSV to Postgres
csv_file = os.path.join(os.path.abspath(os.path.join(meltano_dir, os.pardir)), f"data/csv/order_details/{current_date}.csv")

if os.path.exists(csv_file):
  with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    headers = next(reader)

  config["plugins"]["extractors"][0]["config"]["files"][0]["keys"] = headers
  config["plugins"]["extractors"][0]["config"]["files"][0]["file"] = f"{current_date}.csv"
  config["plugins"]["extractors"][0]["config"]["files"][0]["entity"] = f"order_details"
  config["plugins"]["extractors"][0]["config"]["files"][0]["path"] = f"../../data/csv/order_details"
  config["plugins"]["extractors"][0]["select"] = ["order_details.*"]

  with open(meltano_file, "w") as file:
    yaml.safe_dump(config, file)

  os.system("meltano run tap-csv target-postgres")