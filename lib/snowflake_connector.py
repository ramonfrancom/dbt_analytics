import snowflake.connector 
from dotenv import load_dotenv

def get_snowflake_connection(snf_user,snf_password,snf_account):
    conn = snowflake.connector.connect(
        user=snf_user,
        password=snf_password,
        account=snf_account,
        database="PPNCSRC",
        schema="ADMIN"
    )

    cursor = conn.cursor()

    return cursor