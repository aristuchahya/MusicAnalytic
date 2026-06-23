from helpers.output.driver.kafka import KafkaOutputDriver
from helpers.output.driver.beanstalk import BeanstalkOutputDriver

class OutputDriverFactory:

    @staticmethod
    def create_output_driver(*args, **kwargs):
        destination = kwargs.get("destination")
        assert destination, "Destination is required"

        if destination == "kafka":
            return OutputDriverFactory.create_kafka_output_driver(*args, **kwargs)
        elif destination == "beanstalk":
            return OutputDriverFactory.create_beanstalk_output_driver(*args, **kwargs)


    @staticmethod
    def create_kafka_output_driver(*args, **kwargs):
        return KafkaOutputDriver(
            topic=kwargs.pop("output"),
            bootstrap_servers=kwargs.pop("bootstrap_servers").split(","),
            *args,
            **kwargs
        )
    
    @staticmethod
    def create_beanstalk_output_driver(*args, **kwargs):
        return BeanstalkOutputDriver(
            tube=kwargs.pop("output"),
            host=kwargs.pop("beanstalk_host"),
            port=kwargs.pop("beanstalk_port"),
            *args,
            **kwargs
        )