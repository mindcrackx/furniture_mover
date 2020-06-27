from typing import AsyncIterator

import httpx

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
        except Exception as e:
            if exists_ok_if_empty:
                if response.status_code == 412:
                    db_info = await self._client.get(f"/{db}")
                    db_info.raise_for_status()
                    if db_info.json()["doc_count"] == 0:
                        return
            raise e

    async def get_all_docs(self, db: str) -> AsyncIterator[dict]:
        response = await self._client.get(f"/{db}/_all_docs?include_docs=true")
        data = response.json()
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

        response = await self._client.put(f"/{db}/{doc_id}", json=doc)
        response.raise_for_status()

        if not same_revision or doc_rev_num == "1":
            return

        response = await self._client.get(f"/{db}/{doc_id}")
        response.raise_for_status()
        current_doc_rev = response.json()["_rev"]
        current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)

        while current_doc_rev_num < doc_rev_num:
            update_response = await self._client.put(
                f"/{db}/{doc_id}?rev={current_doc_rev}", json=doc
            )
            update_response.raise_for_status()

            response = await self._client.get(f"/{db}/{doc_id}")
            response.raise_for_status()
            current_doc_rev = response.json()["_rev"]
            current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)
