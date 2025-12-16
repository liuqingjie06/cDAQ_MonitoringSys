import ssl, json, time
import paho.mqtt.client as mqtt

host = "a3bu3p0y1xuf8q.ats.iot.cn-north-1.amazonaws.com.cn"
port = 8883
topic = "tower/data"

client_id = "TowerMonitor1"

ca = "AmazonRootCA1.pem"
cert = "f4f0d80e076beaeb6a907702e5e59fa5a6018f0b359d6c09de81ee14f784f52f-certificate.pem.crt"
key = "f4f0d80e076beaeb6a907702e5e59fa5a6018f0b359d6c09de81ee14f784f52f-private.pem.key"

def on_connect(client, userdata, flags, rc):
    print("Connected, rc =", rc)
    if rc == 0:
        client.subscribe("tower/#")
        print("Subscribed to tower/#")
    else:
        print("Connect failed!")

def on_message(client, userdata, msg):
    print("RECV:", msg.topic, msg.payload.decode())

def on_publish(client, userdata, mid):
    print("Publish OK, mid =", mid)

# -------- SUB --------
sub = mqtt.Client(
    client_id=client_id+"-sub",
    protocol=mqtt.MQTTv311
)
sub.tls_set(
    ca_certs=ca,
    certfile=cert,
    keyfile=key,
    tls_version=ssl.PROTOCOL_TLSv1_2
)
sub.on_connect = on_connect
sub.on_message = on_message

sub.connect(host, port)
sub.loop_start()

# -------- PUB --------
pub = mqtt.Client(
    client_id=client_id+"-pub",
    protocol=mqtt.MQTTv311
)
pub.tls_set(
    ca_certs=ca,
    certfile=cert,
    keyfile=key,
    tls_version=ssl.PROTOCOL_TLSv1_2
)
pub.on_publish = on_publish

pub.connect(host, port)
pub.loop_start()

time.sleep(1)

pub.publish(
    topic,
    json.dumps({"test": "hello from python"}),
    qos=1
)

time.sleep(2)

pub.loop_stop()
pub.disconnect()

time.sleep(5)

sub.loop_stop()
sub.disconnect()
