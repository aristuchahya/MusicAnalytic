
from helpers.input.driver.beanstalk import BeanstalkInputDriver



class InputDriverFactory:
    @staticmethod
    def create_input_driver(*args, **kwargs):
        if kwargs.get("beanstalk_host") and kwargs.get("beanstalk_port"):
            return InputDriverFactory.create_beanstalk_input_driver(*args, **kwargs)
        



    @staticmethod
    def create_beanstalk_input_driver(*args, **kwargs):
        return BeanstalkInputDriver(
            tube=kwargs.pop("input"),
            host=kwargs.pop("beanstalk_host"),
            port=kwargs.pop("beanstalk_port"),
            *args,
            **kwargs
        )


