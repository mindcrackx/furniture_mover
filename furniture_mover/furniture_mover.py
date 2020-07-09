import json
import re
import sys
from pathlib import Path
from typing import Union

from furniture_mover.couch import CouchDb


class FurnitureMover:
    def __init__(self, config):
        self._couch: CouchDb = CouchDb(config)

    def close(self) -> None:
        self._couch.close()

    def save_all_docs(self, filepath: Union[str, Path], db: str) -> None:
        try:
            with open(filepath, mode="w", encoding="utf-8") as outf:
                for doc in self._couch.get_all_docs(db):
                    outf.write(json.dumps(doc, ensure_ascii=False) + "\n")
        except Exception as e:
            sys.exit(f"Exception opening or writing file: {str(e)}")

    def insert_all_docs(
        self,
        filepath: Union[str, Path],
        db: str,
        same_revision: bool = True,
        db_exists_ok_if_empty: bool = True,
    ) -> None:
        self._couch.create_db(db, db_exists_ok_if_empty)
        try:
            with open(filepath, mode="r", encoding="utf-8") as inf:
                for line in inf:
                    self._couch.insert_doc(
                        db, json.loads(line), same_revision=same_revision
                    )
        except Exception as e:
            sys.exit(f"Exception opening or writing file: {str(e)}")

    @staticmethod
    def from_all_docs_file(infile: Path, outfile: Path) -> None:
        data = None
        try:
            with open(infile, mode="r", encoding="utf-8") as inf:
                data = json.loads(inf.read())
        except Exception as e:
            sys.exit(f"Exception opening or reading file {infile}: {str(e)}")

        if data is not None and "rows" in data:
            with open(outfile, mode="w", encoding="utf-8") as outf:
                for row in data["rows"]:
                    outf.write(json.dumps(row["doc"], ensure_ascii=False) + "\n")

    @staticmethod
    def filter_infile(filter_file: Path, infile: Path) -> None:
        filters = None
        try:
            with open(filter_file, "r", encoding="utf-8") as inf:
                filters = json.loads(inf.read())
        except Exception as e:
            sys.exit(f"Exception opening or reading file {filter_file}: {str(e)}")

        for filter_ in filters:
            with open(filter_["filepath"], "w", encoding="utf-8") as outf:
                with open(infile, "r", encoding="utf-8") as inf:
                    for line in inf:
                        data = json.loads(line)
                        for regex in filter_["regex_filters"]:
                            if re.match(regex, data["_id"]):
                                outf.write(json.dumps(data) + "\n")
                                break
