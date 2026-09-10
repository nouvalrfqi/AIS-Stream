import os

import pytest

from dotenv import load_dotenv

load_dotenv()

pytestmark = pytest.mark.integration


def _topic_available():
    try:
        from kafka import KafkaConsumer

        c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), api_version=(2, 5, 0))
        topics = c.topics()
        c.close()
        return "ais.raw" in topics and "ais.dlq" in topics
    except Exception:
        return False


def _postgres_available():
    try:
        import psycopg2

        conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
        conn.close()
        return True
    except Exception:
        return False


def _minio_available():
    try:
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            region_name="auto",
        )
        s3.head_bucket(Bucket=os.getenv("S3_BUCKET", "maritime-raw"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _topic_available(), reason="Kafka infra tidak tersedia")
def test_kafka_topics_and_partitions():
    from kafka import KafkaConsumer

    c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), api_version=(2, 5, 0))
    assert len(c.partitions_for_topic("ais.raw")) == 8
    assert "ais.dlq" in c.topics()
    c.close()


@pytest.mark.skipif(not _postgres_available(), reason="PostGIS infra tidak tersedia")
def test_postgres_schema_tables():
    import psycopg2

    conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('vessel_current_state'), to_regclass('vessel_track')")
    r = cur.fetchone()
    cur.close()
    conn.close()
    assert r == ("vessel_current_state", "vessel_track")


@pytest.mark.skipif(not _minio_available(), reason="MinIO infra tidak tersedia")
def test_minio_raw_prefix_has_parquet():
    import boto3
    import pyarrow.parquet as pq
    from io import BytesIO

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name="auto",
    )
    objs = s3.list_objects_v2(Bucket=os.getenv("S3_BUCKET", "maritime-raw"), Prefix="raw/").get("Contents", [])
    parquet = [o for o in objs if o["Key"].endswith(".parquet")]
    assert parquet, "MinIO raw/ tidak berisi parquet"

    body = s3.get_object(
        Bucket=os.getenv("S3_BUCKET", "maritime-raw"),
        Key=sorted(parquet, key=lambda o: o["Key"])[-1]["Key"],
    )["Body"].read()
    table = pq.read_table(BytesIO(body))
    names = set(table.column_names)
    assert {"event_time", "ingested_at", "latitude", "longitude"}.issubset(names)