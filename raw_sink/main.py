import json
import logging

from common.dlq import close_dlq, init_dlq
from raw_sink.config import load_config
from raw_sink.consumer import RawSinkConsumer
from raw_sink.s3_storage import S3Storage

logger = logging.getLogger("raw_sink")


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
    safe_config = {k: v for k, v in config.items() if "secret" not in k and "access" != k}
    _log("info", "raw sink starting", **safe_config)

    storage = S3Storage(config)
    _log("info", "s3 storage ready", bucket=storage.bucket)

    dlq_producer = init_dlq(config["kafka_bootstrap_servers"], config["kafka_topic_dlq"])
    config["dlq_producer"] = dlq_producer

    consumer = RawSinkConsumer(config, storage, _log)
    consumer.run()
    close_dlq(dlq_producer)
    _log("info", "raw sink stopped")


if __name__ == "__main__":
    main()