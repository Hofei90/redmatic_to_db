import queue
import threading
import paho.mqtt.client as mqtt
from functools import partial
import json
from db_model_postgrest import Datum
from db_model_postgrest import sende_daten
import datetime
import time
import os
import toml


SKRIPTPFAD = os.path.abspath(os.path.dirname(__file__))
CONFIGDATEI = "redmatic_to_sqldb_cfg.toml"


def load_config():
    """Lädt die Konfiguration aus dem smartmeter_cfg.toml File"""
    configfile = os.path.join(SKRIPTPFAD, CONFIGDATEI)
    with open(configfile) as conffile:
        config = toml.loads(conffile.read())
    return config


CONFIG = load_config()
DATAPOINTS = ["STATE", "LOWBAT", "SET_TEMPERATURE", "VALVE_STATE", "ACTUAL_TEMPERATURE", "CONTROL_MODE",
              "FAULT_REPORTING", "BATTERY_STATE"]
queue_ = queue.Queue()


def on_message(client, userdata, msg):
    print(msg.payload.decode("utf-8"))
    queue_.put(msg.payload)


def main_mqtt():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("192.168.178.38", 1883, 60)
    client.subscribe("hm/#")
    client.loop_forever()


def convert_mqtt_daten(data):
    try:
        data_convert = json.loads(data.decode("utf-8"))
    except AttributeError:
        return None
    return data_convert


def convert_wert(val, datapoint_type):
    """ALT
    if datapoint_type == "INTEGER":
        wert = int(val)
    elif datapoint_type == "FLOAT":
        wert = float(val)
    elif datapoint_type == "BOOL":
        wert = bool(val)
    else:
        wert = val
    """
    return float(val)


def reduce_data(data):
    return Datum(
        datetime.datetime.fromtimestamp(data["ts"] / 1000),
        data["hm"]["deviceName"],
        convert_wert(data["val"], data["hm"]["datapointType"]),
        data["hm"]["datapoint"]
    )


def main_daten_verarbeiten():
    headers = {f"Authorization": "{user} {token}".format(user=CONFIG["user"],
                                                         token=CONFIG["token"])}
    while True:
        data = []
        while not queue_.empty():
            datum = convert_mqtt_daten(queue_.get())
            if datum["hm"]["datapoint"] in DATAPOINTS:
                data.append(reduce_data(datum))
        if data:
            print(data)
            sende_daten(CONFIG["url"], CONFIG["table"], headers, data)
        time.sleep(30)


def main():
    threading.Thread(target=main_daten_verarbeiten).start()
    main_mqtt()


if __name__ == "__main__":
    main()
