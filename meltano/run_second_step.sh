#!/bin/bash
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "$SCRIPT_DIR/venv/bin/activate"
cd "$SCRIPT_DIR/second_step"
python script.py