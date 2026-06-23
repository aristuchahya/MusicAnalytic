from controllers import Controllers


class BaseCrawl(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hello = "Hello World"

    async def handler(self, job: dict):
        print(job)
        