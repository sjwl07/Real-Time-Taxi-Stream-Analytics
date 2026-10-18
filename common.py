"""Helpers shared by the three Spark stages (driver side only)."""
import glob
import os
import shutil
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))


def spark_session(app, slots, shuffle_partitions=4):
    jars = sorted(glob.glob(os.path.join(ROOT, "jars", "*.jar")))
    if not jars:
        sys.exit("ERROR: no jars in ./jars (see README, setup step)")
    spark = (SparkSession.builder.appName(app).master(f"local[{slots}]")
             .config("spark.jars", ",".join(jars))
             .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_topic(spark, cfg, topic, max_offsets, schema):
    """Kafka source -> one column per field of `schema` (DDL string)."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", int(max_offsets))
           .option("failOnDataLoss", "false")
           .load())
    return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")


def lag_and_offsets(progress):
    """From Spark's lastProgress: (lag, offsets processed), summed over partitions.
    Spark tracks offsets in its checkpoint (no Kafka consumer group), so lag comes from here."""
    try:
        src = progress["sources"][0]
        done = sum(int(o) for parts in (src.get("endOffset") or {}).values() for o in parts.values())
        top = sum(int(o) for parts in (src.get("latestOffset") or {}).values() for o in parts.values())
        return top - done, done
    except Exception:
        return None, None


def reset_state(*paths):
    """Delete Spark checkpoints / state snapshots for a clean run."""
    for p in paths:
        if os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        elif os.path.exists(p):
            os.remove(p)