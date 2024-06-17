import paho.mqtt.client as mqtt
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# MQTT settings
MQTT_BROKER = "iot.coreflux.cloud"
MQTT_PORT = 1883
BASE_TOPIC = "Company/Divison/Country/Local/AssemblyLines/Machine1"

# Signal variables
di2 = di3 = di4 = di5 = di6 = di7 = False
counterOk = counterNOk = total_counter = 0
stop_condition_sent = start_condition_sent = False
machine_status = "Idle"
last_production_time = time.time()

# Shift management
shift_duration = timedelta(hours=8)
current_shift_start = datetime.now()
shift_data = []

# Callback function on connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe("Advantech/74FE488485D3/data")
    else:
        print("Failed to connect, return code %d\n", rc)

# Callback function on receiving a message
def on_message(client, userdata, message):
    global di2, di3, di4, di5, di6, di7

    payload = message.payload.decode("utf-8")
    data = json.loads(payload)

    di2 = data.get("di2", False)
    di3 = data.get("di3", False)
    di4 = data.get("di4", False)
    di5 = data.get("di5", False)
    di6 = data.get("di6", False)
    di7 = data.get("di7", False)

    process_signals(client)

# Function to process the signals and publish messages
def process_signals(client):
    global di2, di3, di4, di5, di6, di7
    global stop_condition_sent, start_condition_sent
    global counterOk, counterNOk, total_counter
    global machine_status, last_production_time
    global shift_duration, current_shift_start, shift_data

    current_time = time.time()
    cycle_time = current_time - last_production_time

    if di5:
        counterOk += 1
        total_counter += 1
        last_production_time = current_time
        publish_message(client, "Counters/Ok", counterOk)
        publish_message(client, "CycleTime", cycle_time)
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "NewPartOk"})
        if machine_status != "Producing":
            machine_status = "Producing"
            publish_message(client, "Status", machine_status)
            publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToProduction"})
        publish_message(client, "Counters/Total", total_counter)
    elif di6:
        counterNOk += 1
        total_counter += 1
        last_production_time = current_time
        publish_message(client, "Counters/Scrap", counterNOk)
        publish_message(client, "CycleTime", cycle_time)
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "NewPartNotOk"})
        if machine_status != "ProducingWithScrap":
            machine_status = "ProducingWithScrap"
            publish_message(client, "Status", machine_status)
            publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToProduction"})
        publish_message(client, "Counters/Total", total_counter)

    if not di2 and di3 and not stop_condition_sent:
        publish_message(client, "Status", "In Alarm")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        stop_condition_sent = True
        start_condition_sent = False
        machine_status = "In Alarm"
        print("A stop condition has been detected.")
    elif di4 and not start_condition_sent:
        stop_condition_sent = False
        start_condition_sent = True
        publish_message(client, "Status", "Idle")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        machine_status = "Idle"
        print("Machine is running.")

    if machine_status in ["Producing", "ProducingWithScrap"] and (current_time - last_production_time) > 120:
        machine_status = "Idle"
        publish_message(client, "Status", "Idle")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        print("Machine is idle due to inactivity.")

    # Shift management
    if datetime.now() - current_shift_start >= shift_duration:
        end_shift(client)

# Function to handle end of shift
def end_shift(client):
    global shift_data, current_shift_start, counterOk, counterNOk, total_counter
    current_shift_end = datetime.now()

    shift_summary = {
        "start": current_shift_start.strftime("%Y-%m-%d %H:%M:%S"),
        "end": current_shift_end.strftime("%Y-%m-%d %H:%M:%S"),
        "counterOk": counterOk,
        "counterNOk": counterNOk,
        "total_counter": total_counter
    }

    shift_data.append(shift_summary)
    print("Shift ended:", shift_summary)

    # Reset counters for the new shift
    counterOk = counterNOk = total_counter = 0

    # Start a new shift
    current_shift_start = datetime.now()

    # Publish shift summary
    publish_message(client, "Shift/Summary", shift_summary)

# Function to publish messages
def publish_message(client, topic_suffix, message):
    topic = f"{BASE_TOPIC}/{topic_suffix}"
    payload = json.dumps(message) if isinstance(message, dict) else str(message)
    client.publish(topic, payload)
    print(f"Published to {topic}: {payload}")

# Initialize MQTT client
client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect

client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    client.loop_stop()
    client.disconnect()
