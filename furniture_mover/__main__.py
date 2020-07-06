import asyncio
import os
from pathlib import Path
from typing import Optional

import typer

from furniture_mover.config import Config
from furniture_mover.furniture_mover import FurnitureMover

if os.name == "nt":
    # https://stackoverflow.com/questions/62412754/python-asyncio-errors-oserror-winerror-6-the-handle-is-invalid-and-runtim
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


app = typer.Typer()


@app.command("import")
def import_data(
    filepath: Path,
    db: str,
    url: str = typer.Option("http://localhost:5984"),
    user: Optional[str] = typer.Option(None),
    password: Optional[str] = typer.Option(None),
    proxy: Optional[str] = typer.Option(None),
    timeout: float = typer.Option(3),
    db_exists_ok_if_empty: bool = typer.Option(True),
    same_revision: bool = typer.Option(True),
) -> None:
    config = Config(
        url=url, user=user, password=password, proxy=proxy, timeout=timeout,
    )

    async def _run(config, filepath, db, same_revision, db_exists_ok_if_empty):
        fm = FurnitureMover(config)
        try:
            await fm.insert_all_docs(filepath, db, same_revision, db_exists_ok_if_empty)
        finally:
            await fm.close()

    asyncio.run(_run(config, filepath, db, same_revision, db_exists_ok_if_empty))


@app.command("export")
def export_data(
    filepath: Path,
    db: str,
    url: str = typer.Option("http://localhost:5984"),
    user: Optional[str] = typer.Option(None),
    password: Optional[str] = typer.Option(None),
    proxy: Optional[str] = typer.Option(None),
    timeout: float = typer.Option(3),
) -> None:
    config = Config(
        url=url, user=user, password=password, proxy=proxy, timeout=timeout,
    )

    async def _run(config, filepath, db):
        fm = FurnitureMover(config)
        try:
            await fm.save_all_docs(filepath, db)
        finally:
            await fm.close()

    asyncio.run(_run(config, filepath, db))


@app.command("export_from_all_docs_file")
def export_data_from_all_docs_file(all_docs_filepath: Path, filepath: Path) -> None:
    async def _run(infile, outfile):
        await FurnitureMover.from_all_docs_file(all_docs_filepath, filepath)

    asyncio.run(_run(all_docs_filepath, filepath))


@app.command("filter")
def filter(filter_file: Path, infile: Path) -> None:
    async def _run(filter_file, infile) -> None:
        await FurnitureMover.filter_infile(filter_file, infile)

    asyncio.run(_run(filter_file, infile))


if __name__ == "__main__":
    app()
