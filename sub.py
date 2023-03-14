import paho.mqtt.client as mqtt
import json
import mysql.connector
import socket
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

        cursor = connection.cursor()
        sql = "INSERT INTO input_sensor (client,NodeID, Time, Humidity, Temperature, ThemalArray) VALUES (%s,%s, NOW(), %s, %s, %s)"
        values = (
            payload["client_id"],
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
        cursor.close()
        connection.close()



# Define on_connect callback function
def on_connect(client, userdata, flags, rc):
    # Get the IP address of the remote client
    client_ip = socket.gethostbyname(socket.gethostname())
    print(f"User with IP address {client_ip} connected to broker server")

    print("Connected to MQTT broker with result code " + str(rc))
    # Subscribe to topic
    client.subscribe(topic)

    

# Define on_disconnect callback function
def on_disconnect(client, userdata, rc):
    # Get the IP address of the remote client
    client_ip = socket.gethostbyname(socket.gethostname())
    print(f"User with IP address {client_ip} disconnected from broker server")
    
    
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
        print(f"Received data from client IP:{payload['client_ip']} ({payload['client_id']}){payload}")
       
        insert_to_database(payload)
        # Clear buffer
        buffer = ""



# Create MQTT client and set callbacks
client = mqtt.Client(userdata=[socket.gethostname()])
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Connect to MQTT broker and start loop
client.connect(broker_address, broker_port)
client.loop_forever()