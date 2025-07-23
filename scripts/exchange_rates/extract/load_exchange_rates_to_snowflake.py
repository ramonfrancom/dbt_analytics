from lib.snowflake_connector import get_snowflake_connection_with_airflow_conn
import os

from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

def data_load(conn_id:str):

    data_path = Path('/root/Development/Airflow/data/')
    if not any(data_path.glob("*.json")):
        print("No files where found in data folder")
        return
    
    cursor = get_snowflake_connection_with_airflow_conn(conn_id)

    # cursor.execute("CREATE TABLE PPNCSRC.ADMIN.test (name VARCHAR(30),edad NUMBER(3,0));")

    cursor.execute(f"PUT 'file:///root/Development/Airflow/data/*.json' @fx_stage AUTO_COMPRESS=FALSE")
    cursor.execute("""
        COPY INTO fx_raw(filename,last_modified,data)
        FROM (
            SELECT 
                METADATA$FILENAME,
                METADATA$FILE_LAST_MODIFIED,
                $1
            FROM @fx_stage 
        )
        FILE_FORMAT=(TYPE='JSON')
    """)


if __name__ == "__main__":
    data_load()