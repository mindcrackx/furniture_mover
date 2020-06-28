import sys
from contextlib import contextmanager
from typing import AsyncIterator, List

import httpx
from httpx._exceptions import ConnectError, ConnectTimeout, HTTPError, InvalidURL

from furniture_mover.config import Config


class CouchDb:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._authentication = None

        if self._config.user and self._config.password:
            self._authentication = (self._config.user, self._config.password)

        try:
            if self._config.user and self._config.password:
                self._client = httpx.AsyncClient(
                    proxies=self._config.proxy,
                    timeout=self._config.timeout,
                    base_url=self._config.url,
                    headers={
                        "Content-Type": "application/json",
                        "Accept-Charset": "utf-8",
                        "Cache-Control": "no-cache",
                    },
                    auth=(self._config.user, self._config.password),
                )

            else:
                self._client = httpx.AsyncClient(
                    proxies=self._config.proxy,
                    timeout=self._config.timeout,
                    base_url=self._config.url,
                    headers={
                        "Content-Type": "application/json",
                        "Accept-Charset": "utf-8",
                        "Cache-Control": "no-cache",
                    },
                )
        except InvalidURL as e:
            sys.exit(f"Got an invalid URL {self._config.url}. {str(e)}")

    async def close(self) -> None:
        await self._client.aclose()

    @contextmanager
    def handle_web(self, raise_status: List[int] = []):
        try:
            yield
        except HTTPError as e:
            if e.response is not None:
                if e.response.status_code in raise_status:
                    raise e
                if e.response.status_code == 401:
                    sys.exit("Unauthorized: User or Password is wrong or missing.")
                else:
                    sys.exit(
                        f"Got unexpected status code {e.response.status_code} with message {e.response.text}"  # noqa
                    )
        except ConnectError as e:
            sys.exit(f"Error connecting with base_url {self._client.base_url} {str(e)}")
        except ConnectTimeout as e:
            sys.exit(
                f"Timeout Error connecting with base_url {self._client.base_url} {str(e)}"
            )
        except Exception as e:
            sys.exit(f"Got unexpected exception: {str(e)}")

    async def create_db(self, db: str, exists_ok_if_empty: bool = True) -> None:
        try:
            with self.handle_web(raise_status=[412]):
                response = await self._client.put(f"{db}")
                response.raise_for_status()
        except HTTPError:
            if exists_ok_if_empty:
                db_info = await self._client.get(f"{db}")
                db_info.raise_for_status()
                if db_info.json()["doc_count"] == 0:
                    return
                else:
                    sys.exit(f"Database {db} exists but is not empty. Aborting.")

    async def get_all_docs(self, db: str) -> AsyncIterator[dict]:
        with self.handle_web():
            response = await self._client.get(f"{db}/_all_docs?include_docs=true")
            response.raise_for_status()

        data = response.json()
        if "rows" not in data:
            sys.exit(
                f"got unexpected response, 'rows' missing in json. Response was {data}"
            )

        for row in data["rows"]:
            yield row["doc"]

    async def insert_doc(self, db: str, doc: dict, same_revision: bool = True) -> None:
        def _get_rev_num_from_doc(doc: dict) -> int:
            return int(doc["_rev"].split("-")[0])

        def _get_rev_num_from_rev(rev: str) -> int:
            return int(rev.split("-")[0])

        doc_id = doc["_id"]
        doc_rev_num = _get_rev_num_from_doc(doc)
        del doc["_rev"]

        with self.handle_web():
            response = await self._client.put(f"{db}/{doc_id}", json=doc)
            response.raise_for_status()

        if not same_revision or doc_rev_num == "1":
            return

        with self.handle_web():
            response = await self._client.get(f"{db}/{doc_id}")
            response.raise_for_status()

        current_doc_rev = response.json()["_rev"]
        current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)

        while current_doc_rev_num < doc_rev_num:
            with self.handle_web():
                updatresponse = await self._client.put(
                    f"{db}/{doc_id}?rev={current_doc_rev}", json=doc
                )
                updatresponse.raise_for_status()

            with self.handle_web():
                response = await self._client.get(f"{db}/{doc_id}")
                response.raise_for_status()

            current_doc_rev = response.json()["_rev"]
            current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)
