from abc import ABC
import hashlib
from helpers.output import Output
from helpers.input import Input
from loguru import logger
from configparser import ConfigParser
from helpers.database import DatabaseDriver

class Controllers(ABC):

    job : dict = {}
    def __init__(self, *args, **kwargs):
        self.conn = DatabaseDriver()
        self.log = logger
        self.config = ConfigParser()
        self.config.read(kwargs.get("config"))

        config_val = kwargs.get("config")
        if isinstance(config_val, str):
            self.config.read(config_val)      # kalau masih path, baca filenya
        elif isinstance(config_val, ConfigParser):
            self.config = config_val          # kalau sudah object, langsung pakai
        else:
            raise ValueError("Parameter 'config' harus berupa path string atau ConfigParser object")

        kwargs["config"] = self.config

        if kwargs.get("input"):
            self.input_name = kwargs.get("input")
            self.source_name = kwargs.get("source")
            self.input = Input(*args, **kwargs)
        else:
            self.input = None

        if kwargs.get("destination"):
            self.output_name = kwargs.get("output")
            self.destination_name = kwargs.get("destination")
            self.output = Output(*args, **kwargs)

    
    def generate_id(self, value):
        hash_object = hashlib.md5(value.encode())
        hex_dig = hash_object.hexdigest()
        return hex_dig

    async def main(self):

        jobs = self.input or [{}]

        for job in jobs:
            if not job:
                self.log.info(f"No jobs available")
                continue
            self.job = job
            try:
                await self.handler(job)
            except Exception as e:
                self.log.error(e)

    
    async def handler(self, job: dict):
        pass


