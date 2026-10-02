"""
02_read_from_minio.py — читает CSV из MinIO через S3A.

- Подключение Spark к MinIO через S3A
- Автоматическую загрузку JAR'ов через spark.jars.packages
- Чтение CSV с inferSchema
- Простые агрегации
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col


def main():
    # 1. SparkSession с настройками для S3A
    spark = (
        SparkSession.builder
        .appName("02-read-from-minio")
        .master("spark://spark-master:7077")

        # Настройки S3A для MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        # Уменьшаем количество партиций для shuffle
        .config("spark.sql.shuffle.partitions", "4")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark версия: {spark.version}")
    print("=" * 60)

    # 2. Путь к CSV в MinIO
    # Формат: s3a://<bucket>/<path>
    csv_path = "s3a://iot-platform/iot_events.csv"
    print(f"\nЧитаю CSV: {csv_path}\n")

    # 3. Читаем CSV
    # header=True      — первая строка = имена колонок
    # inferSchema=True — Spark сам определит типы
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    # 4. Схема DataFrame — что Spark «увидел» в CSV
    print("--- Схема данных ---")
    df.printSchema()

    # 5. Первые 5 строк
    print("\n--- Первые 5 строк ---")
    df.show(5, truncate=False)

    # 6. Сколько всего строк
    total = df.count()
    print(f"\nВсего строк: {total}")

    # 7. Агрегация по датчикам
    agg_df = (
        df.groupBy("device_id")
        .agg(
            count("*").alias("events_count"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
        )
        .orderBy("device_id")
    )

    print("\n--- Агрегация по датчикам ---")
    agg_df.show(truncate=False)

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()