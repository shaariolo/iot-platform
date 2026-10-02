"""
01_hello_spark.py

- Создание SparkSession
- DataFrame из Python-списка
- Transformations (groupBy, agg)
- Actions (show, count)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

def main ():
    # 1. Создаём SparkSession
    spark = (
        SparkSession.builder
        .appName("01-hello-spark")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )    

    # 2. Убираем логи Spark
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark версия: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")
    print("=" * 60)

    # 3. Создаем DataFrame из Python-списка
    data = [
        ("sensor-001", 22.5, 45.0),
        ("sensor-001", 23.1, 44.0),
        ("sensor-002", 19.8, 50.0),
        ("sensor-002", 20.5, 48.0),
        ("sensor-003", 25.0, 40.0),       
    ]
    columns = ["device_id", "temperature", "humidity"]

    df = spark.createDataFrame(data, schema=columns)

    print("\n--- Исходный DataFrame ---")
    df.show()

    # 4. Трансформация (lazy)
    agg_df = (
        df.groupBy("device_id")
        .agg(
            count("*").alias("events_count"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
        )
        .orderBy("device_id")
    )

    # 5. Action — Spark ВЫПОЛНЯЕТ job
    print("\n--- Агрегация по датчикам ---")
    agg_df.show()

    print(f"\nВсего строк: {df.count()}")

    spark.stop()
    print("\nSparkSession закрыта.")


if __name__ == "__main__":
    main()