# 1. Импорты
# - os, pathlib для работы с путями
# - dotenv для загрузки .env
# - confluent_kafka.admin.AdminClient
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

# 2. Загрузка .env
# - определить путь к корню проекта (на уровень выше producers/)
project_root = Path(__file__).resolve().parent.parent
env_path = project_root / ".env"

# - load_dotenv(путь)
load_dotenv(dotenv_path=env_path)

# 3. Получить bootstrap servers
# - os.getenv("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL")
# - если None — вывести ошибку и выйти
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS_EXTERNAL")
if not bootstrap_servers:
    print("ERROR: KAFKA_BOOTSTRAP_SERVERS_EXTERNAL not found in .env")
    sys.exit(1)    

# 4. Создать AdminClient
# - config = {"bootstrap.servers": bootstrap}
try:
    admin_client = AdminClient({
        "bootstrap.servers": bootstrap_servers
    })
# 5. Запросить метаданные
# - metadata = admin.list_topics(timeout=10)
    metadata = admin_client.list_topics(timeout=10)
# 6. Вывести:
# - cluster_id
# - количество брокеров
# - список топиков
    cluster_id = metadata.cluster_id
    brokers_count = len(metadata.brokers)
    topic_names = list(metadata.topics.keys())

    print(f"Cluster ID: {cluster_id}")
    print(f"Brokers: {brokers_count}")
    print("Topics:")
    for topic in topic_names:
        print(f"- {topic}")

# 7. Обработать исключения (try/except)
except KafkaException as e:
    print(f"ERROR: Failed to connect to Kafka: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Unexpected error: {e}")
    sys.exit(1)  
