import asyncio
import logging  # noqa

from furniture_mover.config import Config
from furniture_mover.furniture_mover import FurnitureMover

# logging.basicConfig(level=logging.DEBUG)


async def main():
    config = Config(url="http://localhost:5984", user="admin", password="adminadmin",)

    fm = FurnitureMover(config)
    try:
        await fm.save_all_docs("tmp/test.txt", "test")
    finally:
        await fm.close()


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
