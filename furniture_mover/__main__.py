from pathlib import Path
from typing import Optional

import typer

from furniture_mover.config import Config
from furniture_mover.furniture_mover import FurnitureMover

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
) -> None:
    config = Config(
        url=url, user=user, password=password, proxy=proxy, timeout=timeout,
    )

    fm = FurnitureMover(config)
    try:
        fm.save_all_docs(filepath, db)
    finally:
        fm.close()


@app.command("export_from_all_docs_file")
def export_data_from_all_docs_file(all_docs_filepath: Path, filepath: Path) -> None:
    FurnitureMover.from_all_docs_file(all_docs_filepath, filepath)


@app.command("filter")
def filter(filter_file: Path, infile: Path) -> None:
    FurnitureMover.filter_infile(filter_file, infile)


if __name__ == "__main__":
    app()
