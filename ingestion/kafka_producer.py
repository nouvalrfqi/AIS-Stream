import json

from kafka.serializer import Serializer


class _KeySerializer(Serializer):
    def serialize(self, topic, headers, data):
        return str(data).encode("utf-8")


class _ValueSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


class AisKafkaProducer:
    def __init__(self, config: dict):
        from kafka import KafkaProducer

        self.producer = KafkaProducer(
            bootstrap_servers=config["kafka_bootstrap_servers"],
            key_serializer=_KeySerializer(),
            value_serializer=_ValueSerializer(),
        )
        self.topic = config["kafka_topic_raw"]

    def send(self, canonical: dict):
        return self.producer.send(
            topic=self.topic,
            key=canonical["mmsi"],
            value=canonical,
        )

    def flush(self):
        self.producer.flush()

    def close(self):
        try:
            self.flush()
        finally:
            self.producer.close()