from kafka import KafkaConsumer
import json
import time
from loguru import logger

class KafkaMessageConsumer:
    def __init__(self, topic: str, bootstrap_servers: list, group_id: str | None = None):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.log = logger

        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=60000,
            group_id=f"{group_id}",   
            auto_offset_reset='latest',
            # enable_auto_commit=False,
            # value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        self.log.info(f"Connecting to Kafka: {bootstrap_servers}")
        self.log.info(f"Consumer started on topic: {topic}")

    def listen(self, callback: None):
        self.log.info("Listening for messages...")
        try:
            for msg in self.consumer:
                data = json.loads(msg.value.decode('utf-8'))

                if callback:
                    result = callback(data)
                    yield result
                else:
                    yield data
        except json.JSONDecodeError as e:
            print(f"Message is not valid JSON: {e}")
    
    def close(self):
        self.consumer.close()
        print("Kafka consumer closed")