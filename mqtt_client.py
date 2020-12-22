import paho.mqtt.client as mqtt
import json

# This file allows to connect to MQTT broker to receive the RPI data
# and save data in CSV file

MQTT_HOST = ""
MQTT_PORT = 1883
TOPIC = "RPI/temp_sens_0"

def process_data(data):
    json_file = json.loads(data)
    csv_info = "{0},{1},{2},{3}\n".format(json_file['device'], json_file['temperature'], json_file['humidity'], json_file['date'])
    return csv_info

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(TOPIC)
    print("Subscribed to topic {0}".format(TOPIC))

def on_message(client, userdata, msg):
    print("Message Received")
    payload = msg.payload.decode('UTF-8')
    print(msg.topic+" "+payload)
    csv_info = process_data(payload)
    with open("data.csv", "a+") as data_file:
        data_file.write(csv_info)
        data_file.close()


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(host=MQTT_HOST, port=MQTT_PORT)
client.loop_forever()
