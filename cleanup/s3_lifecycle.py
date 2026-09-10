import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class S3Lifecycle:
    def __init__(self, config: dict):
        self.config = config
        self.client = boto3.client(
            "s3",
            endpoint_url=config["s3_endpoint"] or None,
            aws_access_key_id=config["s3_access_key"],
            aws_secret_access_key=config["s3_secret_key"],
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )

    def _rule_id(self) -> str:
        return f"expire-{self.config['s3_prefix']}-{self.config['s3_retention_days']}d"

    def get_rule(self) -> dict | None:
        try:
            response = self.client.get_bucket_lifecycle_configuration(
                Bucket=self.config["s3_bucket"]
            )
        except ClientError:
            return None
        return next(
            (r for r in response.get("Rules", []) if r.get("ID") == self._rule_id()),
            None,
        )

    def apply_rule(self) -> bool:
        rule = {
            "ID": self._rule_id(),
            "Status": "Enabled",
            "Filter": {"Prefix": self.config["s3_prefix"]},
            "Expiration": {"Days": self.config["s3_retention_days"]},
        }
        if self.get_rule() == rule:
            return False
        self.client.put_bucket_lifecycle_configuration(
            Bucket=self.config["s3_bucket"],
            LifecycleConfiguration={"Rules": [rule]},
        )
        return True

    def remove_rule(self) -> bool:
        if not self.get_rule():
            return False
        self.client.delete_bucket_lifecycle(Bucket=self.config["s3_bucket"])
        return True