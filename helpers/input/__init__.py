import logging

from helpers.input.driver.factory import InputDriverFactory

logger = logging.getLogger(__name__)


class Input:
    def __init__(self, *args, **kwargs):
        self.driver = InputDriverFactory.create_input_driver(*args, **kwargs)
        logger.debug(f"using {self.driver.name} input driver")

    def exception_handler(self, e, **kwargs):
        self.driver.exception_handler(e, **kwargs)

    def put_message(self, **kwargs):
        self.driver.put_message(**kwargs)

    def __iter__(self):
        yield from self.driver
