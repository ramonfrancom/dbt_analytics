import snowflake.connector 
import os

from dotenv import load_dotenv

load_dotenv()

def data_load():
    snf_user = os.getenv("SNF_USER")
    snf_password = os.getenv("SNF_PASSWORD")
    snf_account = os.getenv("SNF_ACCOUNT")

    print(snf_user)
    print(snf_password)
    print(snf_account)
    
    conn = snowflake.connector.connect(
        user=snf_user,
        password=snf_password,
        account=snf_account,
        database="PPNCSRC",
        schema="ADMIN"
    )

    cursor = conn.cursor()

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