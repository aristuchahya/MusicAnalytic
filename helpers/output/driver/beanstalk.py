import atexit

from helpers.input.driver.beanstalk import BeanstalkInputDriver
from helpers.output.driver import OutputDriver


class BeanstalkOutputDriver(OutputDriver):
    name = "beanstalk"

    def __init__(
        self, tube: str, host: str = "localhost", port: int = 11300, *args, **kwargs
    ):
        super(BeanstalkOutputDriver, self).__init__(*args, **kwargs)
        self.beanstalk = BeanstalkInputDriver(
            tube=tube, host=host, port=port, *args, **kwargs
        )
        atexit.register(self.close)

    def put(self, output: str, **kwargs):
        self.beanstalk.put(body=output, **kwargs)

    def close(self):
        self.beanstalk.close()
