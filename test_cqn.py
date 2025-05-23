import oracledb

oracledb.init_oracle_client()


def callback(message: oracledb.Message):
    print("Notification received:")
    print(message.msgid)


# Connect as REPORT user
connection = oracledb.connect("REPORT/123456@localhost:1521/mydatabase", events=True)

# Register for notifications
sub = connection.subscribe(
    namespace=oracledb.SUBSCR_NAMESPACE_DBCHANGE,
    callback=callback,
    # client_initiated=True,
    port=123456,
    operations=oracledb.OPCODE_ALLOPS,
    qos=oracledb.SUBSCR_QOS_ROWIDS,
)

# Register the query
cursor = connection.cursor()
sub.registerquery("SELECT * FROM ETL.FLAG_TBL")

print("Waiting for notifications...")
import time

while True:
    time.sleep(5)
