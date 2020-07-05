import asyncio
import os
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
    # config: Config, db, db_exists_ok_if_empty: bool, same_revision: bool, filepath: str
    filepath: str,
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
    # config: Config, filepath: str, db: str
    filepath: str,
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


if __name__ == "__main__":
    app()
