from abc import ABC, abstractmethod

class OutputDriver(ABC):
    name = None

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def put(self, output: str):
        pass

    @abstractmethod
    def close(self):
        pass