class Worker:
    async def run(self):
        raise NotImplementedError()

    async def init(self):
        pass
