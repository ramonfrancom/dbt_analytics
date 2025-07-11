import requests
from datetime import date
from datetime import timedelta
from datetime import datetime
import json
import time
from pathlib import Path

import os
from dotenv import load_dotenv

load_dotenv()

def generator_dates_in_between(start_date, end_date, step_days=1):
    if step_days == 0:
        raise ValueError("step_days cannot be 0")
    compare = (lambda a,b: a >= b) if step_days < 0 else (lambda a, b: a <= b)

    current = start_date
    while compare(current,end_date):
        yield current
        current += timedelta(days=step_days)

def get_exchange_data():
    api_key = os.getenv("API_KEY")
    http_base = "https://api.exchangerate.host/historical"

    gen_date = generator_dates_in_between(date(2025,5,30),date(2025,5,1),-1)

    for current_date in gen_date:
        http_complete = f"{http_base}?access_key={api_key}&date={current_date}"
        print(http_complete)

        file_path = f"{os.environ['AIRFLOW_HOME']}/data/{current_date}.json"
        path_file = Path(file_path)

        if path_file.exists():
            print(f"Skipping {current_date}, already exists.")
            continue

        response = requests.get(http_complete)
        if response.status_code == 200:
            data = response.json()

            with open(file_path, "w") as file:
                json.dump(data, file)

        time.sleep(10)

if __name__ == "__main__":
    get_exchange_data()