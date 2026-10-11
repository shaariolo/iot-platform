"""
04_time_travel.py — демонстрация time travel в Delta Lake.

Демонстрирует:
- Чтение конкретной версии Delta-таблицы (VERSION AS OF)
- Append новых данных → создание новой версии
- Сравнение версий 0 и 1
- История таблицы

Показывает, что старые данные физически остаются,
а Delta позволяет читать любую версию.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from delta.tables import DeltaTable

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType,
)


# Путь к Delta-таблице (создана в 03_write_to_delta.py)
DELTA_PATH = "s3a://iot-platform/delta/iot_events/"


def create_spark() -> SparkSession:
    """Создаёт SparkSession с Delta + S3A."""
    return (
        SparkSession.builder
        .appName("04-time-travel")
        .master("spark://spark-master:7077")

        # --- Delta Lake extensions ---
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # --- S3A для MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        .config("spark.sql.shuffle.partitions", "4")

        .getOrCreate()
    )


def generate_new_events(num_events: int = 100) -> list[dict]:
    """Генерирует новые IoT-события для append."""
    events = []
    device_ids = [f"sensor-{i:03d}" for i in range(1, 11)]
    base_time = datetime.now(timezone.utc)

    for i in range(num_events):
        events.append({
            "event_id": str(uuid.uuid4()),
            "device_id": random.choice(device_ids),
            "timestamp": (base_time + timedelta(seconds=i)),
            "temperature": round(random.uniform(15.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "pressure": round(random.uniform(990.0, 1030.0), 2),
            "battery": random.randint(0, 100),
        })
    return events

def get_schema() -> StructType:
    """Явная схема — совпадает с Delta-таблицей."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True), 
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("battery", IntegerType(), True),
    ])


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("TIME TRAVEL ДЕМОНСТРАЦИЯ")
    print("=" * 60)

    # ========================================================
    # ШАГ 1: Читаем текущее состояние (version 0)
    # ========================================================
    print(f"\n--- ШАГ 1: version 0 (до append) ---")

    df_v0 = spark.read.format("delta").load(DELTA_PATH)
    count_v0 = df_v0.count()
    print(f"Строк в version 0: {count_v0}")

    # ========================================================
    # ШАГ 2: Генерируем новые данные
    # ========================================================
    print(f"\n--- ШАГ 2: генерирую 100 новых событий ---")

    new_events = generate_new_events(100)
    new_df = spark.createDataFrame(new_events, schema=get_schema())
    print(f"Сгенерировано: {new_df.count()} событий")

    # ========================================================
    # ШАГ 3: APPEND → создаётся version 1
    # ========================================================
    print(f"\n--- ШАГ 3: append в Delta → version 1 ---")

    (
        new_df.write
        .format("delta")
        .mode("append")              # ← append, не overwrite!
        .save(DELTA_PATH)
    )

    print("✅ Version 1 создана")

    # ========================================================
    # ШАГ 4: Читаем version 0 (должна быть 1000)
    # ========================================================
    print(f"\n--- ШАГ 4: читаю version 0 через time travel ---")

    df_v0_after = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)    # ← time travel!
        .load(DELTA_PATH)
    )
    count_v0_after = df_v0_after.count()
    print(f"Строк в version 0: {count_v0_after}")
    print(f"Ожидалось: {count_v0}")

    if count_v0_after == count_v0:
        print("✅ Version 0 не изменилась — time travel работает!")
    else:
        print(f"⚠️ Расхождение!")

    # ========================================================
    # ШАГ 5: Читаем version 1 (должна быть 1100)
    # ========================================================
    print(f"\n--- ШАГ 5: читаю version 1 ---")

    df_v1 = (
        spark.read
        .format("delta")
        .option("versionAsOf", 1)
        .load(DELTA_PATH)
    )
    count_v1 = df_v1.count()
    print(f"Строк в version 1: {count_v1}")
    print(f"Ожидалось: {count_v0 + 100}")

    if count_v1 == count_v0 + 100:
        print("✅ Version 1 содержит старые + новые данные")
    else:
        print(f"⚠️ Расхождение!")

    # ========================================================
    # ШАГ 6: Читаем текущую версию
    # ========================================================
    print(f"\n--- ШАГ 6: читаю текущую версию (без versionAsOf) ---")

    df_current = spark.read.format("delta").load(DELTA_PATH)
    count_current = df_current.count()
    print(f"Строк в текущей версии: {count_current}")

    if count_current == count_v1:
        print("✅ Текущая версия = version 1")

    # ========================================================
    # ШАГ 7: История таблицы
    # ========================================================
    print(f"\n--- ШАГ 7: история Delta-таблицы ---")

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    (
        delta_table.history()
        .select("version", "timestamp", "operation", "operationParameters")
        .show(truncate=False)
    )

    # ========================================================
    # ШАГ 8: Сравнение — сколько событий от каждого датчика
    #         в version 0 vs version 1
    # ========================================================
    print(f"\n--- ШАГ 8: агрегация по датчикам в version 0 vs version 1 ---")

    print("\nVersion 0:")
    (
        df_v0_after
        .groupBy("device_id")
        .agg(count("*").alias("events_v0"))
        .orderBy("device_id")
        .show(truncate=False)
    )

    print("\nVersion 1:")
    (
        df_v1
        .groupBy("device_id")
        .agg(count("*").alias("events_v1"))
        .orderBy("device_id")
        .show(truncate=False)
    )

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()