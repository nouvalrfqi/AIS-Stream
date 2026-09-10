import hashlib

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class S3Storage:
    def __init__(self, config: dict):
        self.bucket = config["s3_bucket"]
        self.prefix = config["s3_prefix"]
        self.client = boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint"] or None,
            aws_access_key_id=config["s3_access_key"],
            aws_secret_access_key=config["s3_secret_key"],
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                self.client.create_bucket(Bucket=self.bucket)
                print(f"S3: bucket '{self.bucket}' created")
            except ClientError as exc:
                raise RuntimeError(f"failed to create bucket {self.bucket}: {exc}") from exc

    def object_key(self, event_date: str, event_hour: str, filename: str) -> str:
        return f"{self.prefix}/dt={event_date}/HH={event_hour}/{filename}"

    def put_parquet(self, event_date: str, event_hour: str, data: bytes, idempotency_id: str) -> str:
        key = self.object_key(event_date, event_hour, f"{idempotency_id}.parquet")
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return key

    @staticmethod
    def make_idempotency_id(offset_start: int, offset_end: int, group_id: str, content_salt: str) -> str:
        digest = hashlib.sha256(f"{group_id}:{offset_start}:{offset_end}:{content_salt}".encode("utf-8")).hexdigest()[:16]
        return f"{offset_start}-{offset_end}-{digest}"