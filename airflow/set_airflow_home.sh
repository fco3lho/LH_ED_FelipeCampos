#!/bin/bash
SCRIPT_DIR=$(dirname "$(realpath "${BASH_SOURCE[0]}")")
export AIRFLOW_HOME=$SCRIPT_DIR
echo "AIRFLOW_HOME is set to $AIRFLOW_HOME"
