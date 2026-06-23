import logging

from helpers.output.driver.factory import OutputDriverFactory

logger = logging.getLogger(__name__)

class Output:
    def __init__(self, *args, **kwargs):
        self.driver = OutputDriverFactory.create_output_driver(*args, **kwargs)
        logger.debug(f"using output driver: {self.driver.name}")
    
    def put(self, output: str, *args, **kwargs):
        self.driver.put(output, *args, **kwargs)