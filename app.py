import paho.mqtt.client as mqtt
import json
import time

# MQTT settings
MQTT_BROKER = "iot.coreflux.cloud"
MQTT_PORT = 1883
BASE_TOPIC = "Company/Divison/Country/Local/AssemblyLines/Machine1"

# Signal variables
di2 = False  # part presence
di3 = False  # red light
di4 = False  # green light
di5 = False  # ok counter
di6 = False  # scrap counter
di7 = False  # manual mode

counterOk = 0
counterNOk = 0
total_counter = 0
stop_condition_sent = False  # flag to track if stop condition message has been sent
start_condition_sent = False

machine_status = "Idle"
last_production_time = time.time()

# Callback function on connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to the topic once connected
        client.subscribe("Advantech/74FE488485D3/data")
    else:
        print("Failed to connect, return code %d\n", rc)


# Callback function on receiving a message
def on_message(client, userdata, message):
    global di2, di3, di4, di5, di6, di7

    payload = message.payload.decode("utf-8")
    data = json.loads(payload)

    # Update signal states
    di2 = data.get("di2", False)
    di3 = data.get("di3", False)
    di4 = data.get("di4", False)
    di5 = data.get("di5", False)
    di6 = data.get("di6", False)
    di7 = data.get("di7", False)

    # Process the signals according to your requirements
    process_signals(client)

# Function to process the signals and publish messages
def process_signals(client):
    global di2, di3, di4, di5, di6, di7
    global stop_condition_sent, start_condition_sent
    global counterOk, counterNOk, total_counter
    global machine_status, last_production_time

    current_time = time.time()
    cycle_time = current_time - last_production_time

    if di5:  # OK part produced
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
        publish_message(client,"Counters/Total",total_counter) 
    elif di6:  # NOK part produced
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
        publish_message(client,"Counters/Total", total_counter)       
    # Publish total counter


    if not di2 and di3 and not stop_condition_sent:
        publish_message(client, "Status", "In Alarm")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        stop_condition_sent = True
        start_condition_sent = False
        machine_status = "In Alarm"
        print("A stop condition has been detected.")
    elif di4 and not start_condition_sent:  # Reset stop condition if green light is on
        stop_condition_sent = False
        start_condition_sent = True
        publish_message(client, "Status", "Idle")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        machine_status = "Idle"
        print("Machine is running.")

    # Check for idle condition
    if machine_status in ["Producing", "ProducingWithScrap"] and (current_time - last_production_time) > 120:
        machine_status = "Idle"
        publish_message(client, "Status", "Idle")
        publish_message(client, "Message", {"timestamp": int(current_time), "type": "StatusChangedToIdle"})
        print("Machine is idle due to inactivity.")

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

# Connect to MQTT broker
client.connect(MQTT_BROKER, MQTT_PORT)

# Start the MQTT client
client.loop_start()

# Keep the script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    client.loop_stop()
    client.disconnect()

