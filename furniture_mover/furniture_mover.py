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

    @staticmethod
    async def from_all_docs_file(infile: Path, outfile: Path) -> None:
        data = None
        try:
            async with aiofiles.open(infile, mode="r", encoding="utf-8") as inf:
                data = json.loads(await inf.read())
                print(data)
        except Exception as e:
            sys.exit(f"Exception opening or reading file {infile}: {str(e)}")

        if data is not None and "rows" in data:
            async with aiofiles.open(outfile, mode="w", encoding="utf-8") as outf:
                for row in data["rows"]:
                    await outf.write(json.dumps(row["doc"], ensure_ascii=False) + "\n")
