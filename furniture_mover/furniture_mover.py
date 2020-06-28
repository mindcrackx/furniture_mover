import asyncio
import json
import sys
from pathlib import Path
from typing import Union

import aiofiles

from furniture_mover.couch import CouchDb


class FurnitureMover:
    def __init__(self, config):
        self._couch: CouchDb = CouchDb(config)

    async def close(self) -> None:
        await self._couch.close()

    async def save_all_docs(self, filepath: Union[str, Path], db: str) -> None:
        try:
            async with aiofiles.open(filepath, mode="w", encoding="utf-8") as outf:
                async for doc in self._couch.get_all_docs(db):
                    await outf.write(json.dumps(doc, ensure_ascii=False) + "\n")
        except Exception as e:
            sys.exit(f"Exception opening or writing file: {str(e)}")

    async def insert_all_docs(
        self,
        filepath: Union[str, Path],
        db: str,
        same_revision: bool = True,
        db_exists_ok_if_empty: bool = True,
    ) -> None:
        await self._couch.create_db(db, db_exists_ok_if_empty)
        try:
            async with aiofiles.open(filepath, mode="r", encoding="utf-8") as inf:
                await asyncio.gather(
                    *[
                        self._couch.insert_doc(
                            db, json.loads(line), same_revision=same_revision
                        )
                        async for line in inf
                    ]
                )
        except Exception as e:
            sys.exit(f"Exception opening or writing file: {str(e)}")
