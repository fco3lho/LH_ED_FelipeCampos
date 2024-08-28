from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import json
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
root_dir = os.path.abspath(os.path.join(airflow_dir, os.pardir))

default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, 
    'trigger_rule': 'all_success',
}

def generate_success_json():
   success_info = {
      "status": "Success when running DAG!",
      "date": f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
   }

   messages_dir = os.path.join(root_dir, "messages")

   file_path = os.path.join(messages_dir, f"{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}_success.json")

   out_file = open(file_path, "w") 
   json.dump(success_info, out_file, indent = 4) 
   out_file.close() 

with DAG (
   dag_id='indicium_dag', 
   description='DAG to run Meltano steps sequentially',
   default_args=default_args,
   start_date = datetime(2024, 8, 28), 
   schedule_interval = "@daily", 
   catchup = False
) as dag:

   run_first_step = BashOperator(
      task_id = "run_first_step",
      bash_command = f"{root_dir}/meltano/run_first_step.sh {root_dir}/meltano/first_step"
   )

   run_second_step = BashOperator(
      task_id = "run_second_step",
      bash_command = f"{root_dir}/meltano/run_second_step.sh {root_dir}/meltano/second_step"
   )

   generate_success_file = PythonOperator(
      task_id = "generate_success_json",
      python_callable = generate_success_json
   )


run_first_step >> run_second_step >> generate_success_file