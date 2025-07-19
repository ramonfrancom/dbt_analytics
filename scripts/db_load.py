from lib.snowflake_connector import get_snowflake_connection
import os

from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

def data_load():

    data_path = Path('/root/Development/Airflow/data/')
    if not any(data_path.glob("*.json")):
        print("No files where found in data folder")
        return

    snf_user = os.getenv("SNF_USER")
    snf_password = os.getenv("SNF_PASSWORD")
    snf_account = os.getenv("SNF_ACCOUNT")
    
    cursor = get_snowflake_connection(snf_user,snf_password,snf_account)

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