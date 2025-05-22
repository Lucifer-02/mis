import pandas as pd
import oracledb


def get_new_records(conn, table_name: str, record_creation_date: str) -> pd.DataFrame:

    QUERY = f"""SELECT * FROM {table_name} WHERE DATE8 = {record_creation_date}"""
    print(QUERY)
    df = pd.read_sql(QUERY, con=conn)
    return df


DB_USER = "etl"
DB_PASSWORD = "123456"
DB_DSN = "localhost:1521/mydatabase"
TABLE_TO_MONITOR = "etl.KPI_ACCOUNT_MASTER"
record_creation_date = "20250505"

QUERY = f"SELECT * FROM {TABLE_TO_MONITOR} WHERE DATE8 = {record_creation_date}"
print(QUERY)

connection = None
cursor = None

try:
    connection = oracledb.connect(
        user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN, events=True
    )

    print("Successfully connected to Oracle Database")

    # cursor = connection.cursor()
    # cursor.execute(QUERY)
    # rows = cursor.fetchall()
    #
    # print("\nQuery Results:")
    # for row in rows:
    #     print(row)

    print(
        get_new_records(
            conn=connection,
            table_name=TABLE_TO_MONITOR,
            record_creation_date=record_creation_date,
        )
    )

except oracledb.Error as e:
    (error,) = e.args
    print(f"Database error: {error.code} - {error.message}")
except Exception as e:
    print(f"An error occurred: {e}")
