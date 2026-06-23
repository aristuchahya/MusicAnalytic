from abc import ABC, abstractmethod


class InputDriver(ABC):
    name = None

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get(self):
        pass

    def close(self):
        pass

    def exception_handler(self, e):
        pass

    def __iter__(self):
        yield from self.get()
