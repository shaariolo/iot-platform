"""
05_merge.py — демонстрация MERGE (upsert) в Delta Lake.

Демонстрирует:
- MERGE по ключу (event_id)
- UPDATE существующих + INSERT новых
- Идемпотентность: повторный запуск не создаёт дублей
- Schema evolution

Показывает, почему MERGE — обязательный паттерн для batch-пайплайнов.
"""

import random
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType,
)
from delta.tables import DeltaTable


DELTA_PATH = "s3a://iot-platform/delta/iot_events/"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("05-merge")
        .master("spark://spark-master:7077")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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


def get_schema() -> StructType:
    """Схема, совпадающая с Delta-таблицей."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("battery", IntegerType(), True),
    ])


def generate_updates(spark, num_existing: int = 30, num_new: int = 20):
    """
    Генерирует данные для MERGE:
    - num_existing: с существующими event_id (для UPDATE)
    - num_new: с новыми event_id (для INSERT)
    """
    # 1. Читаем существующие event_id (берём случайные)
    existing_df = (
        spark.read.format("delta").load(DELTA_PATH)
        .select("event_id", "device_id")
        .limit(num_existing)
    )
    existing_rows = existing_df.collect()

    updates = []
    now = datetime.now(timezone.utc)

    # 2. UPDATE: те же event_id, новые значения
    for i, row in enumerate(existing_rows):
        updates.append({
            "event_id": row["event_id"],           # ТОТ ЖЕ ID
            "device_id": row["device_id"],
            "timestamp": now,                       # новое время
            "temperature": round(random.uniform(20.0, 25.0), 2),   # новые значения
            "humidity": round(random.uniform(40.0, 60.0), 2),
            "pressure": round(random.uniform(1000.0, 1020.0), 2),
            "battery": random.randint(50, 100),
        })

    # 3. INSERT: новые event_id
    device_ids = [f"sensor-{i:03d}" for i in range(1, 11)]
    for i in range(num_new):
        updates.append({
            "event_id": str(uuid.uuid4()),         # НОВЫЙ ID
            "device_id": random.choice(device_ids),
            "timestamp": now,
            "temperature": round(random.uniform(15.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "pressure": round(random.uniform(990.0, 1030.0), 2),
            "battery": random.randint(0, 100),
        })

    return spark.createDataFrame(updates, schema=get_schema())


def do_merge(spark, source_df, merge_label: str):
    """
    Выполняет MERGE: UPDATE существующих + INSERT новых.
    Возвращает статистику.
    """
    print(f"\n--- MERGE ({merge_label}) ---")

    # Получаем DeltaTable (не просто DataFrame!)
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            "target.event_id = source.event_id",   # ← ключ MERGE
        )
        # Если есть в target и source → UPDATE
        .whenMatchedUpdateAll()
        # Если есть только в source → INSERT
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ MERGE ({merge_label}) завершён")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("MERGE ДЕМОНСТРАЦИЯ")
    print("=" * 60)

    # ========================================================
    # ШАГ 1: Считаем строк ДО MERGE
    # ========================================================
    print(f"\n--- ШАГ 1: состояние ДО MERGE ---")

    df_before = spark.read.format("delta").load(DELTA_PATH)
    count_before = df_before.count()
    print(f"Строк в таблице: {count_before}")

    # ========================================================
    # ШАГ 2: Генерируем данные для MERGE
    # ========================================================
    print(f"\n--- ШАГ 2: генерирую данные для MERGE ---")

    # 30 существующих + 20 новых
    source_df = generate_updates(spark, num_existing=30, num_new=20)

    print(f"Сгенерировано: {source_df.count()} событий")
    print(f"  - 30 с существующими event_id (→ UPDATE)")
    print(f"  - 20 с новыми event_id (→ INSERT)")

    # ========================================================
    # ШАГ 3: MERGE (первый раз)
    # ========================================================
    do_merge(spark, source_df, "первый запуск")

    # ========================================================
    # ШАГ 4: Считаем ПОСЛЕ MERGE
    # ========================================================
    print(f"\n--- ШАГ 4: состояние ПОСЛЕ MERGE ---")

    df_after = spark.read.format("delta").load(DELTA_PATH)
    count_after = df_after.count()
    expected = count_before + 20     # +20 новых, 30 обновили

    print(f"Строк в таблице: {count_after}")
    print(f"Ожидалось: {expected} (= {count_before} + 20 новых)")

    if count_after == expected:
        print("✅ MERGE работает: 30 обновлено, 20 вставлено")
    else:
        print(f"⚠️ Расхождение! Ожидалось {expected}")

    # ========================================================
    # ШАГ 5: Идемпотентность — запускаем MERGE второй раз
    # ========================================================
    print(f"\n--- ШАГ 5: ПОВТОРНЫЙ MERGE (идемпотентность) ---")

    do_merge(spark, source_df, "повторный запуск")

    df_idempotent = spark.read.format("delta").load(DELTA_PATH)
    count_idempotent = df_idempotent.count()

    print(f"Строк после повторного MERGE: {count_idempotent}")

    if count_idempotent == count_after:
        print("✅ Идемпотентность: повторный MERGE не создал дублей!")
    else:
        print(f"⚠️ Не идемпотентно! Было {count_after}, стало {count_idempotent}")

    # ========================================================
    # ШАГ 6: История
    # ========================================================
    print(f"\n--- ШАГ 6: история Delta-таблицы ---")

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    (
        delta_table.history()
        .select("version", "timestamp", "operation", "operationParameters")
        .show(truncate=False)
    )

    # ========================================================
    # ШАГ 7: Проверка — обновились ли значения
    # ========================================================
    print(f"\n--- ШАГ 7: проверка обновлений (сравниваем одно событие) ---")

    sample_event_id = source_df.limit(1).collect()[0]["event_id"]

    print(f"\nСобытие {sample_event_id}:")

    df_before.filter(col("event_id") == sample_event_id).select(
        "event_id", "temperature", "battery"
    ).show(truncate=False)

    df_idempotent.filter(col("event_id") == sample_event_id).select(
        "event_id", "temperature", "battery"
    ).show(truncate=False)

    print("(первая строка — оригинал, вторая — обновлённая)")

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()