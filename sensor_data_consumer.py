from confluent_kafka import Consumer, Producer
from kafka_config import kafka_config
import json
import time
from colorama import Fore, Style, init

# Ініціалізація colorama для роботи з кольорами
init(autoreset=True)

# Створення Kafka Consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "group.id": "sensor_group",
        "auto.offset.reset": "earliest",
    }
)

# Створення Kafka Producer
producer = Producer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Підписка на топік building_sensors
consumer.subscribe(["building_sensors"])


# Обробка отриманих повідомлень
def process_message(message):
    # Перетворення повідомлення в Python об'єкт
    data = json.loads(message.value().decode("utf-8"))
    sensor_id = data["sensor_id"]
    temperature = data["temperature"]
    humidity = data["humidity"]

    # Виведення отриманих даних (лімітована кількість інформації)
    print(Fore.YELLOW + f"Sensor: {sensor_id}")
    print(Fore.GREEN + f"Temp: {temperature}°C | Humidity: {humidity}%")
    
    # Перевірка порогових значень для температури
    if temperature > 40:
        alert_message = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": data["timestamp"],
            "message": "High temperature alert!",
        }
        print(Fore.RED + "ALERT: High Temperature!")
        producer.produce("temperature_alerts", key=sensor_id, value=json.dumps(alert_message))

    # Перевірка порогових значень для вологості
    if humidity > 80 or humidity < 20:
        alert_message = {
            "sensor_id": sensor_id,
            "humidity": humidity,
            "timestamp": data["timestamp"],
            "message": "Humidity alert!",
        }
        print(Fore.CYAN + "ALERT: Humidity out of range!")
        producer.produce("humidity_alerts", key=sensor_id, value=json.dumps(alert_message))

    # Важливо! Для гарантії відправки повідомлень у Kafka
    producer.flush()

    print("-" * 30)  # Розділяє повідомлення для кожного сенсора

# Обробка повідомлень з топіку
try:
    timeout = 10  # Таймаут в секундах
    start_time = time.time()  # Час початку виконання

    while True:
        # Читання повідомлення з топіку з таймаутом
        msg = consumer.poll(timeout=1.0)  # Читання з топіку
        if msg is None:
            continue
        elif msg.error():
            print(Fore.RED + f"Error: {msg.error()}")
        else:
            process_message(msg)

        # Перевірка часу роботи скрипту
        if time.time() - start_time > timeout:
            print(Fore.MAGENTA + "Timeout reached, stopping consumer.")
            break

except KeyboardInterrupt:
    print(Fore.MAGENTA + " Consumer interrupted.")
finally:
    consumer.close()
    print(Fore.GREEN + "Consumer closed.")
