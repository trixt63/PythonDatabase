# Python scripts for interacting with databases

## How to run
### 1. Create ```data/``` directory
### 2. Create virtual env and install necessary dependencies:
    - Step 1: ```python3 -m venv venv```
    - Step 2: ```source venv/bin/activate
    - Step 3: ```pip install -r requirements.txt```
### 3. Fill .env file according to .env.example

## Jobs
- To log sizes of tables by table names: ```python3 scripts/monitor_table_sizes.py```
- To log sizes of tables by schema names: ```python3 scripts/monitor_schema_table_sizes.py```
