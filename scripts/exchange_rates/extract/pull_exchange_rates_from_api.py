import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from lib.snowflake_connector import get_snowflake_connection_with_airflow_conn

def get_requests_from_ct_snowflake(conn_id):
    cursor = get_snowflake_connection_with_airflow_conn(conn_id)
    cursor.execute("""
                        SELECT 
                            ID,
                            ADDITIONAL_INFO 
                        FROM PPCDMDB.ADMIN.CT_REQUEST_INFO 
                        WHERE 
                            PROCESS_NAME = 'Historical Exchange Data Reports for USD' 
                            AND (STATUS = 'Requested' OR STATUS LIKE 'Failed%')
                   """)

    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns,[id, json.loads(dates)])) for id,dates in cursor.fetchall()]

    return data

def filter_dates_requested_with_date_already_processed_ct_snowflake(conn_id, requested_dates):
    cursor = get_snowflake_connection_with_airflow_conn(conn_id)
    print(requested_dates)
    cursor.execute(f"""
                        WITH date_list as (
                            SELECT 
                                TO_DATE(value::String) AS requested_date
                            FROM TABLE(FLATTEN(input => PARSE_JSON('["{'","'.join(requested_dates)}"]')))
                        )
                        SELECT 
                            date_list.requested_date,
                            extracted_dates.STATUS
                        FROM date_list
                        LEFT JOIN PPCDMDB.ADMIN.CT_EXTRACTED_DATES extracted_dates
                            ON date_list.requested_date = extracted_dates.EXTRACT_DATE
                            and extracted_dates.PROCESS_NAME = 'Historical Exchange Data Reports for USD'
                        ;
                   """)
    dates_status = [[date.strftime('%Y-%m-%d'),status] for [date,status] in cursor.fetchall()]
    return dates_status

def get_exchange_data(request,dates_status):
    dict_results= {'Completed': 0, "Failed": 0}
    for date,status in dates_status:
        if status == 'Completed':
            #SKIP
            continue
        elif status == None or status == 'Failed' : # RETRY
            req_status = request_date_to_api_exchange_rates(date)
            update_ct_with_extracted_information(request,date,status,req_status)
            dict_results[req_status] += 1
        else:
            raise ValueError(f"Status of process date: {date}, is not recognized")
    
    update_ct_requests(request,dict_results)

def request_date_to_api_exchange_rates(date):
    api_key = os.getenv("API_KEY")
    http_base = "https://api.exchangerate.host/historical"
    
    http_complete = f"{http_base}?access_key={api_key}&date={date}"
    print(http_complete)

    file_path = f"{os.environ['AIRFLOW_HOME']}/data/{date}.json"

    response = requests.get(http_complete)
    if response.status_code == 200:
        data = response.json()

        if data["success"] == True:
            with open(file_path, "w") as file:
                json.dump(data, file)

    time.sleep(10)

    request_status = 'Completed' if data["success"] == True else 'Failed'
    return request_status


def update_ct_with_extracted_information(request,date,previous_status,new_status):

    if previous_status == None: # Add new record
        cursor.execute(f"""
            INSERT INTO PPCDMDB.ADMIN.CT_EXTRACTED_DATES
            SELECT
                ID AS request_id,
                PROCESS_NAME,
                '{date}' AS EXTRACT_DATE,
                '{new_status}',
                1 AS RECORD_COUNT,
                TS_REQUESTED,
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'
            FROM PPCDMDB.ADMIN.CT_REQUEST_INFO
            WHERE ID = {request['ID']}
            ORDER BY ID DESC, EXTRACT_DATE ASC;
        """)
    else: # Update record
        cursor.execute(f"""
            UPDATE PPCDMDB.ADMIN.CT_EXTRACTED_DATES 
                SET STATUS = '{new_status}'
                {
                    f", TS_COMPLETED ='{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'" if new_status=='Completed' else ''
                }
            WHERE 
                REQUEST_ID={request['ID']} 
                AND EXTRACT_DATE='{date}';
        """)

def update_ct_requests(request,dict_results):

    status = 'Completed' if dict_results['Failed'] == 0 else f'Failed: {dict_results['Failed']}'

    cursor.execute(f"""
            UPDATE PPCDMDB.ADMIN.CT_REQUEST_INFO 
                SET STATUS = '{status}'
                {
                    f", TS_COMPLETED ='{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'" if status=='Completed' else ''
                }
            WHERE 
                ID={request['ID']};
        """)


if __name__ == "__main__":
    load_dotenv()

    snf_user = os.getenv("SNF_USER")
    snf_password = os.getenv("SNF_PASSWORD")
    snf_account = os.getenv("SNF_ACCOUNT")
    
    cursor = get_snowflake_connection_with_airflow_conn(snf_user,snf_password,snf_account)

    data = get_requests_from_ct_snowflake()

    if len(data) > 0:
        for request in data:
            print(request)
            days_requested = request['ADDITIONAL_INFO']['dates']
            dates_status = filter_dates_requested_with_date_already_processed_ct_snowflake(days_requested)
            # update_ct_with_extracted_information(request,'2019-02-21','Failed','Failed')
            get_exchange_data(request,dates_status)