import os
import json


def delete_failed():
    path = f"{os.environ['AIRFLOW_HOME']}/data"

    for file in os.listdir(path):
        # print(file)
        file_path = f"{path}/{file}"
        with open(file_path,'r') as file:
            request_data = json.load(file)
            if request_data["success"] == False:
                os.remove(file_path)

if __name__ == "__main__":
    delete_failed()