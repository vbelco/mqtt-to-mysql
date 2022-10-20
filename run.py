import paho.mqtt.client as mqtt
#import mysql.connector
import mariadb

MQTT_HOST = 'broker.hivemq.com'
MQTT_PORT = 1883
MQTT_CLIENT_ID = 'fcac7248-edbe-4a7a-b4bd-40c3d2bbe4df'
MQTT_USER = 'broker.hivemq.com:1883'
MQTT_PASSWORD = ''
TOPIC = 'tlacitka/#'

# Open database connection
#db_conn = mysql.connector.connect(
#    host="46.229.230.163",
#    user="hz024700",
#    password="dhynydor",
#    database="hz024702db"
#)
db_conn =  mariadb.connect(
    host="46.229.230.163",
    user="hz024700",
    password="dhynydor",
    database="hz024702db"
)

def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC)
    print('Prihlasene ku topicu')


def on_message(mqtt_client, user_data, message):
    payload = message.payload.decode('utf-8')
    print(payload)

    #db_conn = user_data['db_conn']
    sql = 'INSERT INTO tlacitka_data (topic, payload) VALUES (?, ?)'
    cursor = db_conn.cursor()
    cursor.execute(sql, (message.topic, payload))
    print('Successfully Added record to mysql')
    db_conn.commit()
    print(cursor.rowcount, "record inserted.")
    cursor.close()


def main():
    #db_conn = MySQLdb.connect(mysql_server, mysql_username, mysql_passwd, mysql_db)

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.user_data_set({'db_conn': db_conn})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_forever()


main()