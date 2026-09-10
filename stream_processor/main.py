import json
import logging

from common.dlq import close_dlq, init_dlq
from stream_processor import db
from stream_processor.config import load_config
from stream_processor.consumer import StreamProcessorConsumer

logger = logging.getLogger("stream_processor")


def _log(level: str, msg: str, **fields):
    record = {"event": msg}
    record.update(fields)
    getattr(logger, level)(json.dumps(record))


def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=config["log_level"],
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    safe_config = dict(config)
    safe_config["postgres_url"] = config["postgres_url"].split("@")[-1]
    _log("info", "stream processor starting", **safe_config)

    conn = db.connect(config)
    db.ensure_schema(conn)
    conn.close()
    _log("info", "postgis schema ensured")

    dlq_producer = init_dlq(config["kafka_bootstrap_servers"], config["kafka_topic_dlq"])
    config["dlq_producer"] = dlq_producer

    consumer = StreamProcessorConsumer(config, _log)
    consumer.run()
    close_dlq(dlq_producer)


if __name__ == "__main__":
    main()