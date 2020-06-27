import json
from pathlib import Path
from typing import Union

import aiofiles

from furniture_mover.couch import CouchDb


class FurnitureMover:
    def __init__(self, config):
        self._couch = CouchDb(config)

    async def close(self) -> None:
        await self._couch.close()

    async def save_all_docs(self, filepath: Union[str, Path], db) -> None:
        async with aiofiles.open(filepath, mode="w", encoding="utf-8") as outf:
            async for doc in self._couch.get_all_docs(db):
                await outf.write(json.dumps(doc) + "\n")
