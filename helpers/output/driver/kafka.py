import atexit
from kafka import KafkaProducer


from helpers.output.driver import OutputDriver

class KafkaOutputDriver(OutputDriver):
    name = "kafka"
    def __init__(self, topic: str, bootstrap_servers: list, *args, **kwargs):
        super(KafkaOutputDriver, self).__init__(*args, **kwargs)
        self.topic = topic
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            max_request_size=10485760,
            request_timeout_ms=60000,
        )
        atexit.register(self.close)

    def put(self, output: str, **kwargs):
        self.kafka_producer.send(self.topic, output)
        self.kafka_producer.flush()

    def close(self):
        self.kafka_producer.close()