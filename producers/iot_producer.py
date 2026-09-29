"""
IoT Telemetry Producer.

Генерирует синтетические события от IoT-датчиков и отправляет их в Kafka.

Топик: iot.telemetry.raw
Ключ: device_id (для партиционирования по устройствам)
Формат значения: JSON (UTF-8)
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

# 1. НАСТРОЙКА ЛОГИРОВАНИЯ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TOPIC = "iot.telemetry.raw"
DEVICES_COUNT = 10          # сколько разных датчиков "существует"
EVENTS_PER_DEVICE = 5       # сколько событий от каждого датчика отправим
SEND_INTERVAL_SEC = 0.1     # пауза между отправками (100 мс)


# 2. ЗАГРУЗКА КОНФИГА
def load_config() -> str:
    """
    Загружает .env из корня проекта и возвращает bootstrap servers.
    Скрипт лежит в producers/, .env — в корне проекта (на уровень выше).
    """
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL")
    if not bootstrap_servers:
        logger.error("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL не найдена в .env")
        sys.exit(1)

    return bootstrap_servers

# 3. ГЕНЕРАЦИЯ СОБЫТИЯ
def generate_event(device_id: str) -> dict:
    """
    Создаёт одно синтетическое IoT-событие.
    
    Возвращает словарь, который потом превратится в JSON.
    """
    return {
        "event_id": str(uuid.uuid4()),                      # уникальный ID события
        "device_id": device_id,                             # ID датчика (он же будет ключом)
        "timestamp": datetime.now(timezone.utc).isoformat(),# ISO 8601 UTC
        "temperature": round(random.uniform(15.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "pressure": round(random.uniform(990.0, 1030.0), 2),
        "battery": random.randint(0, 100),
        "firmware_version": "1.2.3",
    }

# 4. CALLBACK ДЛЯ DELIVERY REPORT
def delivery_report(err, msg):
    """
    Вызывается Kafka, когда сообщение доставлено или упало.
    
    err — ошибка (или None, если успех)
    msg — объект сообщения с метаданными (topic, partition, offset)
    """
    if err is not None:
        logger.error(f"Ошибка доставки: {err}")
    else:
        logger.info(
            f"✓ device={msg.key().decode('utf-8')} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

# 5. СОЗДАНИЕ PRODUCER
def create_producer(bootstrap_servers: str) -> Producer:
    """
    Создаёт Kafka Producer с нужными настройками.
    """
    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "iot-telemetry-producer",

        # Гарантия доставки: leader получил сообщение (для RF=1 это ок)
        "acks": "all",

        # Сколько повторять при ошибке
        "retries": 5,
        "retry.backoff.ms": 500,

        # Общий таймаут на одно сообщение (включая retries)
        "delivery.timeout.ms": 30000,  # 30 сек

        # Уменьшаем буферизацию, чтобы сообщения уходили быстрее
        "linger.ms": 10,
    }
    return Producer(config)

# 6. ОТПРАВКА СОБЫТИЯ
def send_event(producer: Producer, event: dict) -> None:
    """
    Отправляет одно событие в Kafka.
    
    Ключ = device_id (для партиционирования).
    Значение = JSON-строка в UTF-8.
    """
    key_bytes = event["device_id"].encode("utf-8")
    value_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")

    try:
        producer.produce(
            topic=TOPIC,
            key=key_bytes,
            value=value_bytes,
            callback=delivery_report,
        )
        # poll(0) — не блокируя, обрабатываем готовые callback'и
        # Без него delivery_report будет вызван только при flush()
        producer.poll(0)
    except BufferError:
        # Буфер producer'а переполнен — ждём, потом retry
        logger.warning("Буфер переполнен, ждём...")
        producer.poll(1)  # ждём до 1 сек
        # Повторная попытка после того, как буфер освободится
        producer.produce(
            topic=TOPIC,
            key=key_bytes,
            value=value_bytes,
            callback=delivery_report,
        )
    except KafkaException as e:
        logger.error(f"Ошибка Kafka: {e}")

# 7. MAIN
def main():
    logger.info("=== IoT Telemetry Producer ===")

    bootstrap_servers = load_config()
    logger.info(f"Подключаюсь к Kafka: {bootstrap_servers}")
    logger.info(f"Топик: {TOPIC}")
    logger.info(f"Датчиков: {DEVICES_COUNT}, событий от каждого: {EVENTS_PER_DEVICE}")
    logger.info("")

    producer = create_producer(bootstrap_servers)

    # Список device_id
    device_ids = [f"sensor-{i:03d}" for i in range(1, DEVICES_COUNT + 1)]

    total_sent = 0
    try:
        for device_id in device_ids:
            for _ in range(EVENTS_PER_DEVICE):
                event = generate_event(device_id)
                send_event(producer, event)
                total_sent += 1
                time.sleep(SEND_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("\nПрервано пользователем (Ctrl+C)")

    finally:
        # flush() — дождаться отправки всех сообщений из буфера.
        # БЕЗ него часть сообщений потеряется!
        logger.info("")
        logger.info("Flushing... (дожидаюсь отправки всех сообщений)")
        remaining = producer.flush(timeout=30)

        if remaining > 0:
            logger.warning(f"⚠ Не удалось отправить {remaining} сообщений")
        else:
            logger.info(f"✅ Отправлено событий: {total_sent}")


if __name__ == "__main__":
    main()