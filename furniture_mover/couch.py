from typing import AsyncIterable, AsyncIterator

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

    async def get_all_docs(self, db: str) -> AsyncIterator[dict]:
        response = await self._client.get(f"/{db}/_all_docs?include_docs=true")
        data = response.json()
        for row in data["rows"]:
            yield row["doc"]

    async def insert_all_docs(
        self, db: str, docs: AsyncIterable[dict], same_revision: bool = False
    ) -> None:
        pass
