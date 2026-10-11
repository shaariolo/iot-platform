"""
03_write_to_delta.py — читает CSV из MinIO, пишет Delta-таблицу.

Демонстрирует:
- Подключение Spark к MinIO через S3A
- Активация Delta Lake (extensions + catalog)
- Чтение CSV
- Запись в Delta (Parquet + _delta_log)
- Чтение Delta обратно
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count


# Пути в MinIO
SOURCE_CSV = "s3a://iot-platform/iot_events.csv"
DELTA_PATH = "s3a://iot-platform/delta/iot_events/"


def create_spark() -> SparkSession:
    """Создаёт SparkSession с поддержкой Delta Lake и S3A."""
    return (
        SparkSession.builder
        .appName("03-write-to-delta")
        .master("spark://spark-master:7077")

        # --- Delta Lake extensions ---
        # Без этих двух конфигов Delta не активируется
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

        # --- Для нашего размера данных ---
        .config("spark.sql.shuffle.partitions", "4")

        .getOrCreate()
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark версия: {spark.version}")
    print(f"Delta версия: смотри --packages в команде запуска")
    print("=" * 60)

    # ========================================================
    # 1. Читаем CSV из MinIO
    # ========================================================
    print(f"\n--- Читаю CSV: {SOURCE_CSV} ---")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(SOURCE_CSV)
    )

    total = df.count()
    print(f"Прочитано строк: {total}")

    # ========================================================
    # 2. Пишем в Delta (overwrite — для простоты)
    # ========================================================
    print(f"\n--- Пишу Delta: {DELTA_PATH} ---")

    (
        df.write
        .format("delta")
        .mode("overwrite")             # для первого раза — overwrite
        .save(DELTA_PATH)
    )

    print("✅ Delta-таблица создана")

    # ========================================================
    # 3. Читаем Delta обратно
    # ========================================================
    print(f"\n--- Читаю Delta обратно ---")

    delta_df = spark.read.format("delta").load(DELTA_PATH)

    delta_count = delta_df.count()
    print(f"Строк в Delta: {delta_count}")

    if delta_count == total:
        print("✅ Количество строк совпадает")
    else:
        print(f"⚠️ Расхождение: CSV={total}, Delta={delta_count}")

    # ========================================================
    # 4. Смотрим схему и первые строки
    # ========================================================
    print("\n--- Схема Delta ---")
    delta_df.printSchema()

    print("\n--- Первые 5 строк ---")
    delta_df.show(5, truncate=False)

    # ========================================================
    # 5. Агрегация (проверяем, что таблица работает как обычно)
    # ========================================================
    agg_df = (
        delta_df.groupBy("device_id")
        .agg(
            count("*").alias("events_count"),
            avg("temperature").alias("avg_temperature"),
        )
        .orderBy("device_id")
    )

    print("\n--- Агрегация по датчикам ---")
    agg_df.show(truncate=False)

    # ========================================================
    # 6. Информация о Delta: версия и история
    # ========================================================
    print("\n--- История Delta-таблицы ---")

    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_table.history().show(truncate=False)

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()