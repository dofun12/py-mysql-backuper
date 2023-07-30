import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage
import config_factory as cf
from main import log


class MqttConnector():
    def __init__(self, config: dict, topic: str, message: str):
        self.config = config
        self.message = message
        self.topic = topic

    def mqtt_onconnect(self, client: mqtt.Client, userdata, flags, rc):
        log.info(f"MQTT Connected!")

    def mqtt_onmessage(self, client: mqtt.Client, userdata, msg: MQTTMessage):
        log.info(f"Received message! {msg.topic}")

    def connect(self):
        user = cf.get_config_value(self.config, cf.CONFIG_MQTT, 'user')
        passwd = cf.get_config_value(self.config, cf.CONFIG_MQTT, 'passwd')
        host = cf.get_config_value(self.config, cf.CONFIG_MQTT, 'host')
        port = cf.get_config_value(self.config, cf.CONFIG_MQTT, 'port')

        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set(user, passwd)
        try:
            mqtt_client.connect(host, port, 60)
        except:
            log.error(f"Error connect on mqtt {host}:{port}")
            return
        mqtt_client.on_connect = self.mqtt_onconnect
        mqtt_client.on_message = self.mqtt_onmessage
        try:
            mqtt_client.publish(self.topic, payload=self.message)
        except:
            log.error(f"Error sending topic {self.topic} on {host}:{port}")
            return
        log.info(f"MQTT sended!")
        mqtt_client.disconnect()


def send_mqtt(config: dict, topic: str, message: str):
    MqttConnector(config, topic, message).connect()
