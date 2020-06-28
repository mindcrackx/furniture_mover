import sys
from typing import AsyncIterator

import httpx
from httpx._exceptions import HTTPError

from furniture_mover.config import Config


class CouchDb:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._authentication = None

        if self._config.user and self._config.password:
            self._authentication = (self._config.user, self._config.password)

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

    async def close(self) -> None:
        await self._client.aclose()

    async def create_db(self, db: str, exists_ok_if_empty: bool = True) -> None:
        try:
            response = await self._client.put(f"/{db}")
            response.raise_for_status()
        except HTTPError:
            if response.status_code == 401:
                sys.exit("Unauthorized: User or Password is wrong or missing.")
            if exists_ok_if_empty:
                if response.status_code == 412:
                    db_info = await self._client.get(f"/{db}")
                    db_info.raise_for_status()
                    if db_info.json()["doc_count"] == 0:
                        return
                    else:
                        sys.exit(f"Database {db} exists but is not empty. Aborting.")
            sys.exit(
                f"Got unexpected status code {response.status_code} with message {response.text}"
            )

    async def get_all_docs(self, db: str) -> AsyncIterator[dict]:
        try:
            response = await self._client.get(f"/{db}/_all_docs?include_docs=true")
            response.raise_for_status()
        except HTTPError:
            if response.status_code == 401:
                sys.exit("Unauthorized: User or Password is wrong or missing.")
            sys.exit(
                f"Got unexpected status code {response.status_code} with message {response.text}"
            )

        data = response.json()
        if "rows" not in data:
            sys.exit(
                f"got unexpected response, 'rows' missing in json. Response was {data}"
            )

        for row in data["rows"]:
            yield row["doc"]

    async def insert_doc(self, db: str, doc: dict, same_revision: bool = True) -> None:
        def _get_rev_num_from_doc(doc: dict) -> str:
            return doc["_rev"].split("-")[0]

        def _get_rev_num_from_rev(rev: str) -> str:
            return rev.split("-")[0]

        doc_id = doc["_id"]
        doc_rev_num = _get_rev_num_from_doc(doc)
        del doc["_rev"]

        try:
            response = await self._client.put(f"/{db}/{doc_id}", json=doc)
            response.raise_for_status()
        except HTTPError:
            if response.status_code == 401:
                sys.exit("Unauthorized: User or Password is wrong or missing.")
            sys.exit(
                f"Got unexpected status code {response.status_code} with message {response.text}"
            )

        if not same_revision or doc_rev_num == "1":
            return

        try:
            response = await self._client.get(f"/{db}/{doc_id}")
            response.raise_for_status()
        except HTTPError:
            if response.status_code == 401:
                sys.exit("Unauthorized: User or Password is wrong or missing.")
            sys.exit(
                f"Got unexpected status code {response.status_code} with message {response.text}"
            )

        current_doc_rev = response.json()["_rev"]
        current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)

        while current_doc_rev_num < doc_rev_num:
            try:
                updatresponse = await self._client.put(
                    f"/{db}/{doc_id}?rev={current_doc_rev}", json=doc
                )
                updatresponse.raise_for_status()
            except HTTPError:
                if response.status_code == 401:
                    sys.exit("Unauthorized: User or Password is wrong or missing.")
                sys.exit(
                    f"Got unexpected status code {response.status_code} with message {response.text}"
                )

            try:
                response = await self._client.get(f"/{db}/{doc_id}")
                response.raise_for_status()
            except HTTPError:
                if response.status_code == 401:
                    sys.exit("Unauthorized: User or Password is wrong or missing.")
                sys.exit(
                    f"Got unexpected status code {response.status_code} with message {response.text}"
                )

            current_doc_rev = response.json()["_rev"]
            current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)
