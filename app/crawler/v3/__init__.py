import asyncio

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

DAYS_TO_PARSE = 3


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks), return_exceptions=True)


class ServerRefusedError(Exception):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return None

    def data(self):
        return self.data
