from confluent_kafka import Producer
from kafka_config import kafka_config
import json
import uuid
import time
import random
from colorama import Fore, Style

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

# Кількість повідомлень, які потрібно відправити
num_messages = 10

# Обробка повідомлень для кожного сенсора
for i in range(num_messages):
    # Генерація даних для одного датчика
    sensor_id = str(uuid.uuid4())
    temperature = random.randint(25, 45)
    humidity = random.randint(15, 85)
    timestamp = time.time()

    data = {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
    }

    # Виведення з кольоровим форматуванням
    print(
        f"{Fore.YELLOW}Generating data for sensor {Fore.CYAN}{sensor_id}{Style.RESET_ALL}"
    )
    print(f"{Fore.GREEN}Temperature: {temperature}°C{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Humidity: {humidity}%{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Timestamp: {timestamp}{Style.RESET_ALL}")
    print("-" * 50)  # Розділяє дані кожного сенсора

    # Відправка даних до топіку building_sensors
    topic_name = "building_sensors"
    producer.produce(
        topic_name,
        key=sensor_id,
        value=json.dumps(data),
        callback=lambda err, msg: print(
            f"{Fore.GREEN}Message sent: {msg.value} to {msg.topic}{Style.RESET_ALL}"
            if err is None
            else f"{Fore.RED}Error: {err}{Style.RESET_ALL}"
        ),
    )
    producer.flush()

    # Підсумок для кожного повідомлення
    print(f"{Fore.BLUE}Data sent to topic {topic_name}{Style.RESET_ALL}\n")

print(f"{Fore.CYAN}Finished sending {num_messages} messages.{Style.RESET_ALL}")
