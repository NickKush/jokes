import asyncio
from dataclasses import dataclass
import logging

from bs4 import BeautifulSoup
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] %(name)s: %(message)s")

log = logging.getLogger("jokes")

JOKES_URL = 'https://anek.ws/anekdot.php?a={}'

@dataclass
class Payload:
    url: str
    index: int


async def task_parser(jobs: asyncio.Queue, out: asyncio.Queue) -> None:
    while True:
        payload: Payload = await jobs.get()

        async with httpx.AsyncClient() as client:
            response = await client.get(payload.url)

            if response.status_code != 200:
                log.info(f"Joke `{payload.index}` returned status - { response.status_code }")
                jobs.task_done()
                continue

            data = response.text

        soup = BeautifulSoup(data, features='html.parser')
        found = soup.find('div', { 'id': 'anek' })

        if found is None:
            log.info(f"Joke `{payload.index}` does not exists")
            jobs.task_done()
            continue

        result = found.get_text()

        if not result:
            log.info(f"Joke `{payload.index}` empty")
            jobs.task_done()
            continue

        await out.put(result)
        jobs.task_done()


async def run(start_index: int, end_index: int, task_count: int) -> None:
    # Generate all urls for memes
    url_pool = asyncio.Queue()
    for index in range(start_index - 1, end_index + 1):
        url_pool.put_nowait(
            Payload(
                url=JOKES_URL.format(index),
                index=index
            )
        )

    orig_size = url_pool.qsize()

    log.info(f"Jokes to load: {url_pool.qsize()}")

    result_pool = asyncio.Queue()
    tasks = [
        asyncio.create_task(task_parser(url_pool, result_pool)) for _ in range(task_count)
    ]
    
    # Lazy loop to check percentage..
    while not url_pool.empty():
        log.info(f"Working... [{orig_size - url_pool.qsize()}/{orig_size}]")
        await asyncio.sleep(1)

    # Destroy tasks. We don't need them anymore.
    for task in tasks:
        task.cancel()

    # Save data into file
    log.info("Saving to file...")
    with open('jokes.txt', 'w') as file:
        file.write("Welcome to bad jokes world!\nEnjoy :)\n")

        while not result_pool.empty():
            data = result_pool.get_nowait()

            file.write('\n\n--------------\n\n')
            file.writelines(data)


def main() -> int:
    # TODO: args
    start_index = 100
    end_index = 1000

    worker_count = 20

    asyncio.run(run(start_index, end_index, worker_count))

    return 0
    

if __name__ == '__main__':
    raise SystemExit(main())
