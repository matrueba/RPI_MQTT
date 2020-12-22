import paho.mqtt.client as mqtt
import time
from datetime import datetime
import board
import adafruit_dht
import json

# This file allows recieve data from DHT22 sensor thorugh port 18 of RPI 
# and publish the information to MQTT broker 

MQTT_HOST = ""
MQTT_PORT = 1883
TOPIC = "RPI/temp_sens_0"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

dht = adafruit_dht.DHT22(board.D18,  use_pulseio=False)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(host=MQTT_HOST, port=MQTT_PORT)
client.loop_start()

while True:
    try:
        temperature = dht.temperature
        humidity = dht.humidity
        sensor_info = {
            'device': 'RP1_0',
            'temperature': temperature,
            'humidity': humidity,
            'date': str(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
        }
        sensor_mess = json.dumps(sensor_info)
        client.publish(topic=TOPIC, payload=sensor_mess, qos=0)
        print("Message published: {}".format(sensor_mess))
    except RuntimeError as error:
        print(error.args[0])
        time.sleep(3)
        continue
    except Exception as error:
        dht.exit()
        raise error
    time.sleep(5)