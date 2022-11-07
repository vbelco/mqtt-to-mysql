import paho.mqtt.client as mqtt
#import mysql.connector
import mariadb

MQTT_HOST = '37.9.171.206'
MQTT_PORT = 1883
MQTT_CLIENT_ID_TLACITKA = 'mqtt-to-mysql-bridge_tlacitka'
MQTT_USER = 'bis'
MQTT_PASSWORD = 'kfd6kld8ibh'
TOPIC = 'tlacitka/#'
TOPIC_ERROR = 'error/#'

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
    mqtt_client.subscribe(TOPIC_ERROR)
    print('Prihlasene ku topicu tlacitka a error topicu')


def on_message(mqtt_client, user_data, message):
    payload = message.payload.decode('utf-8')
    print(payload)
    topic = message.topic

    if mqtt.topic_matches_sub("error/#", topic):
        print("Error received")
        sql = 'INSERT INTO error (topic, payload) VALUES (?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (topic, payload))
        print('Successfully Added record to mysql')
        db_conn.commit()
        print(cursor.rowcount, "record inserted.")
        cursor.close()
    
    if mqtt.topic_matches_sub("tlacitka/#", topic):
        print("Tlacitka received")
        sql = 'INSERT INTO tlacitka_data (topic, payload) VALUES (?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (topic, payload))
        print('Successfully Added record to mysql')
        db_conn.commit()
        print(cursor.rowcount, "record inserted.")
        cursor.close()

def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID_TLACITKA)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.user_data_set({'db_conn': db_conn})

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    
    mqtt_client.loop_forever()
    #mqtt_client_error.loop_forever()
    #mqtt_client.loop_start()

main()