"""
Генерирует CSV с синтетическими IoT-событиями для загрузки в MinIO.
Файл: data/iot_events.csv
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_events(num_events: int = 1000) -> list[dict]:
    """Генерирует список синтетических IoT-событий."""
    events = []
    device_ids = [f"sensor-{i:03d}" for i in range(1, 11)]  # 10 датчиков
    base_time = datetime.now(timezone.utc) - timedelta(hours=1)

    for i in range(num_events):
        device_id = random.choice(device_ids)
        events.append({
            "event_id": str(uuid.uuid4()),
            "device_id": device_id,
            "timestamp": (base_time + timedelta(seconds=i * 3)).isoformat(),
            "temperature": round(random.uniform(15.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "pressure": round(random.uniform(990.0, 1030.0), 2),
            "battery": random.randint(0, 100),
        })
    return events


def main():
    project_root = Path(__file__).resolve().parent.parent
    output_dir = project_root / "data"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "iot_events.csv"

    events = generate_events(num_events=1000)

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=events[0].keys())
        writer.writeheader()
        writer.writerows(events)

    print(f"✅ Сгенерировано {len(events)} событий")
    print(f"📁 Файл: {output_file}")


if __name__ == "__main__":
    main()