import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")

try:
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
        print(
            f'{event["timestamp"]} | {event["sensor_id"]} | '
            f'T={temperature}°C | RH={event["humidity_pct"]}%'
        )
        if temperature >= 35:
            print("ALERT: High temperature detected!")
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    consumer.close()
    total = 0
alerts = 0

print(f"Listening to topic: {TOPIC}")

try:
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
        total += 1
        print(
            f'{event["timestamp"]} | {event["sensor_id"]} | '
            f'T={temperature}°C | RH={event["humidity_pct"]}%'
        )
        if temperature >= 35:
            alerts += 1
            print("ALERT: High temperature detected!")
except KeyboardInterrupt:
    print(f"Consumer stopped by user. Received: {total} | Alerts: {alerts}")
finally:
    consumer.close()