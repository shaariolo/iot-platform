"""
06_optimize_vacuum.py — демонстрация OPTIMIZE и VACUUM.

Демонстрирует:
- Проверку количества файлов в Delta
- OPTIMIZE — компакция мелких файлов
- VACUUM — физическое удаление старых файлов
- Влияние на time travel

Показывает обязательные операции гигиены Data Lake.
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable


DELTA_PATH = "s3a://iot-platform/delta/iot_events/"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("06-optimize-vacuum")
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

        # ВАЖНО: отключаем проверку retention для демо
        # В проде НИКОГДА так не делай — это опасно
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        .getOrCreate()
    )


def get_stats(spark, label: str):
    """Возвращает статистику о таблице."""
    print(f"\n--- {label} ---")

    df = spark.read.format("delta").load(DELTA_PATH)
    count = df.count()
    print(f"Строк в таблице: {count}")

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    # Количество файлов через detail()
    detail = delta_table.detail().collect()[0]
    num_files = detail["numFiles"]
    size_bytes = detail["sizeInBytes"]

    print(f"Parquet-файлов: {num_files}")
    print(f"Размер: {size_bytes} байт ({size_bytes/1024:.1f} KB)")

    return count, num_files, size_bytes


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("OPTIMIZE И VACUUM ДЕМОНСТРАЦИЯ")
    print("=" * 60)

    # ========================================================
    # ШАГ 1: Статистика ДО OPTIMIZE
    # ========================================================
    count_before, files_before, size_before = get_stats(spark, "ДО OPTIMIZE")

    # ========================================================
    # ШАГ 2: Показываем историю
    # ========================================================
    print(f"\n--- История таблицы ДО OPTIMIZE ---")

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    history_before = delta_table.history().count()
    print(f"Всего версий в истории: {history_before}")

    # ========================================================
    # ШАГ 3: OPTIMIZE (компакция)
    # ========================================================
    print(f"\n--- OPTIMIZE (компакция мелких файлов) ---")
    print("Читаю все Parquet-файлы, записываю один большой...")

    (
        delta_table.optimize()
        .executeCompaction()
    )

    print("✅ OPTIMIZE завершён")

    # ========================================================
    # ШАГ 4: Статистика ПОСЛЕ OPTIMIZE
    # ========================================================
    count_after, files_after, size_after = get_stats(spark, "ПОСЛЕ OPTIMIZE")

    print(f"\n📊 Сравнение:")
    print(f"  Файлов: {files_before} → {files_after}")
    print(f"  Строк:  {count_before} → {count_after}")

    if count_before == count_after:
        print("✅ Количество строк НЕ изменилось (OPTIMIZE — не трансформация)")
    else:
        print(f"⚠️ Строки изменились!")

    if files_after < files_before:
        print(f"✅ Файлов стало меньше: {files_before} → {files_after}")
    else:
        print(f"⚠️ Файлов не уменьшилось")

    # ========================================================
    # ШАГ 5: VACUUM с retention 0 (для демо!)
    # ========================================================
    print(f"\n--- VACUUM (retention 0 часов — ТОЛЬКО ДЛЯ ДЕМО!) ---")
    print("В проде retention должен быть 168 часов (7 дней)!")

    (
        delta_table.vacuum(retentionHours=0)
    )

    print("✅ VACUUM завершён")

    # ========================================================
    # ШАГ 6: Проверка — time travel старше не работает
    # ========================================================
    print(f"\n--- Проверка time travel после VACUUM ---")

    try:
        df_v0 = (
            spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(DELTA_PATH)
        )
        count_v0 = df_v0.count()
        print(f"Version 0: {count_v0} строк — доступна")
    except Exception as e:
        print(f"❌ Version 0 недоступна: {e}")

    # ========================================================
    # ШАГ 7: Финальная статистика
    # ========================================================
    get_stats(spark, "ПОСЛЕ VACUUM")

    # ========================================================
    # ШАГ 8: История
    # ========================================================
    print(f"\n--- История таблицы ПОСЛЕ OPTIMIZE + VACUUM ---")

    (
        delta_table.history()
        .select("version", "timestamp", "operation", "operationParameters")
        .show(truncate=False)
    )

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()