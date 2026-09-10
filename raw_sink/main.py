import json
import logging

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

    consumer = RawSinkConsumer(config, storage, _log)
    consumer.run()
    _log("info", "raw sink started", group=config["kafka_consumer_group"])


if __name__ == "__main__":
    main()