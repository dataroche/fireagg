HEALTH_COUNTER_MAX = 3


class Worker:
    def __init__(self):
        self.is_live = False
        self.running = False
        self.health_counter = HEALTH_COUNTER_MAX

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def mark_alive(self):
        if not self.is_live:
            self.is_live_callback()
            self.is_live = True

        self.health_counter = HEALTH_COUNTER_MAX

    def is_live_callback(self):
        pass

    async def run(self):
        raise NotImplementedError()

    async def init(self):
        pass
