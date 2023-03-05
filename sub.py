import paho.mqtt.client as mqtt
import json
import mysql.connector
# Define MQTT broker settings
broker_address = "broker.hivemq.com"
broker_port = 1883
topic = "7am/mqtt"

# Define buffer for storing received data
buffer = ""

def insert_to_database(payload):

    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="input_sensor_server"
        )

        with connection.cursor() as cursor:
            sql = "INSERT INTO input_sensor (client,NodeID, Time, Humidity, Temperature, ThemalArray) VALUES (%s,%s, NOW(), %s, %s, %s)"
            values = (
            payload["client_ip"],
            payload["node_id"],
            payload["relative_humidity"],
            payload["temperature"],
            payload["thermal_array"]
)
            cursor.execute(sql, values)
        connection.commit()
        print("Data inserted into MySQL database.")
    except Exception as e:
        print(f"Failed to write to MySQL database: {e}")
    finally:
        connection.close()


# Define on_connect callback function
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code "+str(rc))
    # Subscribe to topic
    client.subscribe(topic)

# Define on_message callback function
def on_message(client, userdata, message):
    global buffer
    # Append received data to buffer
    buffer += message.payload.decode()
    # Check if buffer contains entire message
    if buffer.endswith("}"):
        # Parse JSON message
        payload = json.loads(buffer)
        # Insert payload data into MySQL database
        print(payload)
        insert_to_database(payload)
        # Clear buffer
        buffer = ""

# Create MQTT client and set callbacks
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker and start loop
client.connect(broker_address, broker_port)
client.loop_forever()