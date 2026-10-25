# Expose gold: copy the gold Delta tables into Postgres so BI tools can query them with SQL
import os

from utils.config import GOLD_BASE
from utils.session import get_spark

TABLES = ["daily_revenue", "trip_stats_by_hour", "top_pickup_zones", "payment_type_mix"]

host = os.getenv("WAREHOUSE_HOST", "warehouse")
db = os.getenv("WAREHOUSE_DB", "analytics")
URL = f"jdbc:postgresql://{host}:5432/{db}"
PROPS = {
    "user": os.getenv("WAREHOUSE_USER", "analytics"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "analytics"),
    "driver": "org.postgresql.Driver",
}


def main() -> None:
    spark = get_spark("gold_to_postgres")
    for name in TABLES:
        df = spark.read.format("delta").load(f"{GOLD_BASE}/{name}")
        df.write.mode("overwrite").option("truncate", "true").jdbc(URL, name, properties=PROPS)
        print(f"EXPOSE: {name} -> {df.count()} rows")


if __name__ == "__main__":
    main()
