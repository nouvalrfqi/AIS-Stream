from common.dlq import send_to_dlq


class _FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value=None, **kwargs):
        self.sent.append((topic, value))
        return self

    def flush(self, timeout=None):
        return None


def test_send_to_dlq_publishes_payload():
    producer = _FakeProducer()
    send_to_dlq(producer, "ais.dlq", {"mmsi": "1"}, reason="validation_failed", source="ingestion")
    assert producer.sent
    topic, payload = producer.sent[0]
    assert topic == "ais.dlq"
    assert payload["_dlq_reason"] == "validation_failed"
    assert payload["_dlq_source"] == "ingestion"
    assert payload["event"]["mmsi"] == "1"


def test_send_to_dlq_adds_timestamp():
    producer = _FakeProducer()
    send_to_dlq(producer, "ais.dlq", {"mmsi": "2"}, reason="poison")
    _, payload = producer.sent[0]
    assert isinstance(payload["_dlq_timestamp"], float)
    assert payload["_dlq_timestamp"] > 0