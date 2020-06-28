import asyncio
import os
from typing import Optional

import typer

from furniture_mover.config import Config
from furniture_mover.furniture_mover import FurnitureMover

if os.name == "nt":
    # https://stackoverflow.com/questions/62412754/python-asyncio-errors-oserror-winerror-6-the-handle-is-invalid-and-runtim
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def import_data(
    config: Config, db, db_exists_ok_if_empty: bool, same_revision: bool, filepath: str
) -> None:
    fm = FurnitureMover(config)
    try:
        await fm.insert_all_docs(filepath, db, same_revision, db_exists_ok_if_empty)
    finally:
        await fm.close()


async def export_data(config: Config, filepath: str, db: str) -> None:
    fm = FurnitureMover(config)
    try:
        await fm.save_all_docs(filepath, db)
    finally:
        await fm.close()


def main(
    filepath: str,
    db: str,
    export: bool = typer.Option(False),
    Import: bool = typer.Option(False),
    url: str = typer.Option("http://localhost:5984"),
    user: Optional[str] = typer.Option(None),
    password: Optional[str] = typer.Option(None),
    proxy: Optional[str] = typer.Option(None),
    timeout: float = typer.Option(3),
    db_exists_ok_if_empty: bool = typer.Option(True),
    same_revision: bool = typer.Option(True),
):
    config = Config(
        url=url, user=user, password=password, proxy=proxy, timeout=timeout,
    )
    if export and Import:
        typer.echo("Only use export or import, not both.")
        typer.Exit()

    if not export and not Import:
        typer.echo("Use export or import.")
        typer.Exit()

    if export:
        asyncio.run(export_data(config, filepath, db))
    if Import:
        asyncio.run(
            import_data(config, db, db_exists_ok_if_empty, same_revision, filepath)
        )


if __name__ == "__main__":
    typer.run(main)
