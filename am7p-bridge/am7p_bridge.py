import json
import time
import socket
import struct
import traceback
from typing import Dict, Tuple, Optional

import paho.mqtt.client as mqtt

OPTIONS_PATH = "/data/options.json"

CONFIG_TOPIC_SENSOR = "homeassistant/sensor/{}/config"
CONFIG_TOPIC_BIN = "homeassistant/binary_sensor/{}/config"

STATE_TOPIC_FORMAT = "homeassistant/sensor/{}/state"          # JSON measurements
META_TOPIC_FORMAT = "homeassistant/sensor/{}/last_packet_s"   # seconds since last packet
ONLINE_TOPIC_FORMAT = "homeassistant/binary_sensor/{}/online" # "ON"/"OFF"

SENSORS = [
    {
        "name": "PM2.5",
        "device_class": "pm25",
        "unit_of_measurement": "µg/m³",
        "value_template": "{{ value_json.pm25 }}",
    },
    {
        "name": "PM10",
        "device_class": "pm10",
        "unit_of_measurement": "µg/m³",
        "value_template": "{{ value_json.pm10 }}",
    },
    {
        "name": "HCHO",
        "unique_id": "hcho",
        "device_class": "volatile_organic_compounds",
        "unit_of_measurement": "mg/m³",
        "value_template": "{{ value_json.hcho }}",
    },
    {
        "name": "TVOC",
        "unique_id": "tvoc",
        "device_class": "volatile_organic_compounds",
        "unit_of_measurement": "mg/m³",
        "value_template": "{{ value_json.tvoc }}",
    },
    {
        "name": "CO2",
        "unique_id": "co2",
        "device_class": "carbon_dioxide",
        "unit_of_measurement": "ppm",
        "value_template": "{{ value_json.co2 }}",
    },
    {
        "name": "Temperature",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
        "value_template": "{{ value_json.temperature }}",
    },
    {
        "name": "Humidity",
        "device_class": "humidity",
        "unit_of_measurement": "%",
        "value_template": "{{ value_json.humidity }}",
    },
]

def load_options() -> dict:
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_name_by_ip(ip: str, addr_to_name: Dict[str, str]) -> str:
    return addr_to_name.get(ip, "am7p")

def mqtt_connect(mqtt_host: str, mqtt_user: str, mqtt_password: str) -> mqtt.Client:
    client = mqtt.Client()
    client.username_pw_set(mqtt_user, mqtt_password)
    client.connect(mqtt_host)
    client.loop_start()
    return client

def publish_discovery(client: mqtt.Client, name: str) -> None:
    device_block = {
        "identifiers": [f"airmaster_{name}"],
        "name": name,
    }

    # Measurement sensors (JSON state)
    state_topic = STATE_TOPIC_FORMAT.format(name)
    for sensor in SENSORS:
        cfg = dict(sensor)
        unique_part = cfg.get("unique_id") or cfg.get("device_class") or cfg["name"].lower().replace(" ", "_")
        uid = f"{name}_{unique_part}"

        cfg.update({
            "state_topic": state_topic,
            "unique_id": uid,
            "device": device_block,
        })

        client.publish(CONFIG_TOPIC_SENSOR.format(uid), json.dumps(cfg), retain=True)

    # Online binary sensor
    online_uid = f"{name}_online"
    online_cfg = {
        "name": "Online",
        "unique_id": online_uid,
        "device_class": "connectivity",
        "state_topic": ONLINE_TOPIC_FORMAT.format(name),
        "payload_on": "ON",
        "payload_off": "OFF",
        "device": device_block,
    }
    client.publish(CONFIG_TOPIC_BIN.format(online_uid), json.dumps(online_cfg), retain=True)

    # Seconds since last packet
    last_uid = f"{name}_last_packet_s"
    last_cfg = {
        "name": "Seconds since last packet",
        "unique_id": last_uid,
        "state_topic": META_TOPIC_FORMAT.format(name),
        "unit_of_measurement": "s",
        "device_class": "duration",
        "device": device_block,
    }
    client.publish(CONFIG_TOPIC_SENSOR.format(last_uid), json.dumps(last_cfg), retain=True)

def publish_state(client: mqtt.Client, name: str, payload: dict) -> None:
    client.publish(STATE_TOPIC_FORMAT.format(name), json.dumps(payload), retain=False)

def publish_meta(client: mqtt.Client, name: str, age_s: int, online: bool) -> None:
    client.publish(META_TOPIC_FORMAT.format(name), str(int(age_s)), retain=True)
    client.publish(ONLINE_TOPIC_FORMAT.format(name), "ON" if online else "OFF", retain=True)

def make_udp_socket(port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("", port))
    s.settimeout(1.0)
    return s

def parse_packet(data: bytes) -> Optional[dict]:
    # Minimum length guard: offset 23 + 7*2 bytes = 14
    if len(data) < 23 + 14:
        return None
    x = struct.unpack_from(">HHHHHHH", data[23:])
    return {
        "pm25": x[0],
        "pm10": x[1],
        "hcho": x[2] / 100,
        "tvoc": x[3] / 100,
        "co2": x[4],
        "temperature": (x[5] - 3500) / 100,
        "humidity": x[6] / 100,
    }

def main() -> None:
    opts = load_options()

    mqtt_host = opts["mqtt_host"]
    mqtt_user = opts["mqtt_user"]
    mqtt_password = opts["mqtt_password"]
    udp_port = int(opts["udp_port"])

    online_timeout_s = int(opts.get("online_timeout_s", 10))
    meta_publish_every_s = int(opts.get("meta_publish_every_s", 2))
    addr_to_name = dict(opts.get("addr_to_name", {}))

    print(f"[am7p] mqtt_host={mqtt_host} udp_port={udp_port} online_timeout_s={online_timeout_s} meta_publish_every_s={meta_publish_every_s}")
    print(f"[am7p] addr_to_name={addr_to_name}")

    client = mqtt_connect(mqtt_host, mqtt_user, mqtt_password)
    sock = make_udp_socket(udp_port)

    known_names = set()
    last_packet_ts: Dict[str, float] = {}

    next_meta_ts = time.time() + meta_publish_every_s

    try:
        while True:
            now = time.time()

            # 1) Receive a packet (or timeout)
            try:
                data, addr = sock.recvfrom(1024)
                ip = addr[0]
                name = get_name_by_ip(ip, addr_to_name)

                payload = parse_packet(data)
                if payload is not None:
                    if name not in known_names:
                        known_names.add(name)
                        publish_discovery(client, name)

                    publish_state(client, name, payload)
                    last_packet_ts[name] = now

            except socket.timeout:
                pass

            # 2) Periodic meta publish (online + age)
            if now >= next_meta_ts:
                next_meta_ts = now + meta_publish_every_s

                for name in list(known_names):
                    ts = last_packet_ts.get(name, 0.0)
                    age = int(now - ts) if ts else 999999
                    online = (ts != 0.0) and (age <= online_timeout_s)
                    publish_meta(client, name, age, online)

    except KeyboardInterrupt:
        print("[am7p] interrupted")
    except Exception:
        print("[am7p] fatal error:\n" + traceback.format_exc())
        raise
    finally:
        # Mark all known devices offline on exit
        try:
            now = time.time()
            for name in list(known_names):
                ts = last_packet_ts.get(name, 0.0)
                age = int(now - ts) if ts else 999999
                publish_meta(client, name, age, False)
        except Exception:
            pass

        try:
            sock.close()
        except Exception:
            pass

        try:
            client.loop_stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
