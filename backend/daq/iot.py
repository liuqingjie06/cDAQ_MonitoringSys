"""
IoT transmission helper.
- type=log : append to backend/data/iot_log.jsonl
- type=mqtt: publish to MQTT broker (AWS IoT compatible TLS if certs provided)
Rewritten to align with backend/license/iottest.py behavior.
"""

import json
import datetime
import ssl
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

_config = {
    "type": "log",
    "host": "127.0.0.1",
    "port": 8883,
    "topic": "cdaq/data",
    "username": "",
    "password": "",
    "client_id": "cdaq-client",
    "ca_cert": "",
    "certfile": "",
    "keyfile": "",
}


def set_config(cfg: dict | None):
    if not cfg:
        return
    _config.update(cfg)


def _resolve(path_str: str):
    if not path_str:
        return None
    p = Path(path_str)
    if not p.is_absolute():
        base = Path(__file__).resolve().parents[2]  # project root
        # avoid duplicated "backend/backend" when config uses "backend/..."
        if path_str.startswith("backend/"):
            p = (base / path_str).resolve()
        else:
            p = (base / "backend" / path_str).resolve()
    return p


def _log_record(data: dict):
    root = Path(__file__).resolve().parent.parent / "data"
    root.mkdir(exist_ok=True)
    log_file = root / "iot_log.jsonl"
    record = {
        "ts": datetime.datetime.now().isoformat(),
        "payload": data,
        "topic": _config.get("topic"),
        "type": _config.get("type"),
        "host": _config.get("host"),
    }
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _publish_log(data: dict):
    _log_record(data)


def _publish_mqtt(data: dict):
    if mqtt is None:
        raise RuntimeError("paho-mqtt not installed")

    client = mqtt.Client(
        client_id=_config.get("client_id") or None,
        protocol=mqtt.MQTTv311
    )

    if _config.get("username"):
        client.username_pw_set(_config.get("username"), _config.get("password") or None)

    ca = _resolve(_config.get("ca_cert") or "")
    cert = _resolve(_config.get("certfile") or "")
    key = _resolve(_config.get("keyfile") or "")
    if ca or cert or key:
        for path_val, label in [(ca, "ca_cert"), (cert, "certfile"), (key, "keyfile")]:
            if path_val and not path_val.exists():
                raise FileNotFoundError(f"{label} not found: {path_val}")
        client.tls_set(
            ca_certs=str(ca) if ca else None,
            certfile=str(cert) if cert else None,
            keyfile=str(key) if key else None,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        client.tls_insecure_set(False)

    host = _config.get("host", "127.0.0.1")
    port = int(_config.get("port", 1883))
    topic = _config.get("topic", "cdaq/data")

    def on_publish(c, u, mid):
        _log_record({"info": "mqtt publish ok", "mid": mid, "topic": topic, "payload": data})

    client.on_publish = on_publish

    client.connect(host, port, keepalive=60)
    client.loop_start()
    payload = json.dumps(data, ensure_ascii=False)
    info = client.publish(topic, payload, qos=1)
    info.wait_for_publish(timeout=5)
    import time
    time.sleep(0.2)
    client.loop_stop()
    client.disconnect()


def publish(data: dict):
    try:
        if _config.get("type") == "mqtt":
            _publish_mqtt(data)
        else:
            _publish_log(data)
    except Exception as e:
        try:
            _log_record({"error": str(e), "payload": data})
        except Exception:
            print("[iot] publish error:", e)
