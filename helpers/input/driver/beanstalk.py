import atexit
import json

from greenstalk import DEFAULT_TUBE, Client, TimedOutError, NotFoundError

from helpers.input.driver import InputDriver


class BeanstalkInputDriver(InputDriver):
    name = "beanstalk"

    def __init__(
        self,
        tube: str = DEFAULT_TUBE,
        host: str = "localhost",
        port: int = 11300,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.beanstalk = Client((host, port), use=tube, watch=tube)
        self.job = None
        self.body = None
        atexit.register(self.close)

    def __delete(self):
        if self.job:
            self.beanstalk.delete(self.job)
            self.job = None

    def __bury(self, **kwargs):
        if self.job:
            self.beanstalk.bury(self.job, **kwargs)
            self.job = None

    def __release(self, **kwargs):
        if self.job:
            self.beanstalk.release(self.job, **kwargs)
            self.job = None

    def __put(self, **kwargs):
        if kwargs.get("body"):
            self.beanstalk.put(kwargs.pop("body"), **kwargs)

    def get(self):
        while True:
            try:
                self.job = self.beanstalk.reserve(timeout=10)
                yield json.loads(self.job.body)
                self.__delete()
            except TimedOutError:
                yield None
            except BrokenPipeError:
                raise
            except Exception:
                raise

    def put(self, **kwargs):
        if kwargs.get("body"):
            self.__put(**kwargs)

    def close(self):
        self.beanstalk.close()

    def exception_handler(self, e, **kwargs):
        action = kwargs.get("action")
        if action == "release":
            self.__release()
        elif action == "delete":
            self.__delete()
        else:
            self.__bury()
