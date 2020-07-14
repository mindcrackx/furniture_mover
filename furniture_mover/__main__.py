import logging
import logging.config
from pathlib import Path
from typing import Optional

import typer

from furniture_mover.config import Config
from furniture_mover.furniture_mover import FurnitureMover

app = typer.Typer()

logging.basicConfig(level=60)  # use 60 so nothing gets logged by default
logger = logging.getLogger()

if Path("furniture_mover.ini").exists():
    logging.config.fileConfig("furniture_mover.ini")


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
    cert_verify: bool = typer.Option(True),
) -> None:
    logger.info("import got called")
    config = Config(
        url=url,
        user=user,
        password=password,
        proxy=proxy,
        timeout=timeout,
        cert_verify=cert_verify,
    )

    fm = FurnitureMover(config)
    try:
        fm.insert_all_docs(filepath, db, same_revision, db_exists_ok_if_empty)
    finally:
        fm.close()


@app.command("export")
def export_data(
    filepath: Path,
    db: str,
    url: str = typer.Option("http://localhost:5984"),
    user: Optional[str] = typer.Option(None),
    password: Optional[str] = typer.Option(None),
    proxy: Optional[str] = typer.Option(None),
    timeout: float = typer.Option(3),
    cert_verify: bool = typer.Option(True),
) -> None:
    logger.info("export got called")
    config = Config(
        url=url,
        user=user,
        password=password,
        proxy=proxy,
        timeout=timeout,
        cert_verify=cert_verify,
    )

    fm = FurnitureMover(config)
    try:
        fm.save_all_docs(filepath, db)
    finally:
        fm.close()


@app.command("export_from_all_docs_file")
def export_data_from_all_docs_file(all_docs_filepath: Path, filepath: Path) -> None:
    logger.info("export_from_all_docs_file got called")
    FurnitureMover.from_all_docs_file(all_docs_filepath, filepath)


@app.command("filter")
def filter(filter_file: Path, infile: Path) -> None:
    logger.info("filter got called")
    FurnitureMover.filter_infile(filter_file, infile)


if __name__ == "__main__":
    app()
