"""
IoT transmission helper.
- type=log : append to backend/data/iot_log.jsonl
- type=mqtt: publish to MQTT broker (AWS IoT compatible TLS if certs provided)
Rewritten to align with backend/license/iottest.py behavior.
"""

import json
import datetime
import ssl
import threading
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
    "control_topic": "+/control/stream",
    "username": "",
    "password": "",
    "client_id": "cdaq-client",
    "ca_cert": "",
    "certfile": "",
    "keyfile": "",
}
_stream_enabled_local = False
_stream_enabled_remote = False
_sub_client = None
_sub_lock = threading.Lock()
_control_display_names = []


def set_stream_enabled(enabled: bool, source: str = "local") -> None:
    global _stream_enabled_local, _stream_enabled_remote
    flag = bool(enabled)
    if source == "remote":
        _stream_enabled_remote = flag
    else:
        _stream_enabled_local = flag


def _is_stream_enabled() -> bool:
    return _stream_enabled_local or _stream_enabled_remote


def set_control_display_names(names) -> None:
    global _control_display_names
    _control_display_names = [str(n).strip() for n in (names or []) if str(n).strip()]


def set_config(cfg: dict | None):
    if not cfg:
        return
    _config.update(cfg)
    if _config.get("type") == "mqtt":
        _ensure_control_subscriber()
    else:
        _stop_control_subscriber()


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


def _parse_stream_enabled(payload_raw):
    if payload_raw is None:
        return None, None, None
    try:
        text = payload_raw.decode("utf-8") if isinstance(payload_raw, (bytes, bytearray)) else str(payload_raw)
    except Exception:
        return None, None, None
    text = text.strip()
    if not text:
        return None, None, None
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            display_name = data.get("display_name")
            timestamp = data.get("timestamp") or data.get("ts")
            for key in ("enabled", "stream", "stream_enabled"):
                if key in data:
                    val = data.get(key)
                    if isinstance(val, bool):
                        return val, display_name, timestamp
                    if isinstance(val, (int, float)):
                        return bool(val), display_name, timestamp
                    if isinstance(val, str):
                        text = val.strip().lower()
                        break
            else:
                return None, display_name, timestamp
        elif isinstance(data, bool):
            return data, None, None
        elif isinstance(data, (int, float)):
            return bool(data), None, None
        elif isinstance(data, str):
            text = data.strip().lower()
    except Exception:
        text = text.lower()
    if text in ("1", "true", "on", "yes", "enable", "enabled", "start"):
        return True, None, None
    if text in ("0", "false", "off", "no", "disable", "disabled", "stop"):
        return False, None, None
    return None, None, None


def _stop_control_subscriber():
    global _sub_client
    with _sub_lock:
        if not _sub_client:
            return
        try:
            _sub_client.loop_stop()
            _sub_client.disconnect()
        except Exception:
            pass
        _sub_client = None


def _ensure_control_subscriber():
    global _sub_client
    if mqtt is None:
        return
    if _config.get("type") != "mqtt":
        return
    with _sub_lock:
        if _sub_client:
            return

        client_id = _config.get("client_id") or None
        sub_id = f"{client_id}-sub" if client_id else None
        client = mqtt.Client(client_id=sub_id, protocol=mqtt.MQTTv311)

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
        topic = _config.get("control_topic") or "+/control/stream"

        def on_connect(c, u, flags, rc):
            try:
                c.subscribe(topic, qos=1)
            except Exception:
                pass

        def on_message(c, u, msg):
            enabled, display_name, timestamp = _parse_stream_enabled(msg.payload)
            if enabled is None:
                return
            if not display_name or not timestamp:
                return
            name_from_topic = ""
            try:
                name_from_topic = str(msg.topic or "").split("/control/stream")[0].rstrip("/")
            except Exception:
                name_from_topic = ""
            if name_from_topic:
                if _control_display_names and name_from_topic not in _control_display_names:
                    return
                if str(display_name).strip() != name_from_topic:
                    return
            set_stream_enabled(enabled, source="remote")

        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(host, port, keepalive=60)
        client.loop_start()
        _sub_client = client


def _log_record(data: dict, topic: str | None = None):
    root = Path(__file__).resolve().parent.parent / "data"
    root.mkdir(exist_ok=True)
    log_file = root / "iot_log.jsonl"
    record = {
        "ts": datetime.datetime.now().isoformat(),
        "payload": data,
        "topic": topic or _config.get("topic"),
        "type": _config.get("type"),
        "host": _config.get("host"),
    }
    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _publish_log(data: dict, topic: str | None = None):
    _log_record(data, topic=topic)


def _publish_mqtt(data: dict, topic: str | None = None):
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
    topic = topic or _config.get("topic", "cdaq/data")

    def on_publish(c, u, mid):
        _log_record({"info": "mqtt publish ok", "mid": mid, "topic": topic, "payload": data}, topic=topic)

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


def publish(data: dict, topic: str | None = None):
    if topic and "/stream/" in topic and not _is_stream_enabled():
        return
    try:
        if _config.get("type") == "mqtt":
            _publish_mqtt(data, topic=topic)
        else:
            _publish_log(data, topic=topic)
    except Exception as e:
        try:
            _log_record({"error": str(e), "payload": data}, topic=topic)
        except Exception:
            print("[iot] publish error:", e)
