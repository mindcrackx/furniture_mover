import sys
from contextlib import contextmanager
from typing import Iterator, List

from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectionError,
    ConnectTimeout,
    HTTPError,
    InvalidURL,
    MissingSchema,
)
from requests.packages.urllib3.util.retry import Retry
from requests_toolbelt import sessions

from furniture_mover.config import Config


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self._timeout = 3
        if "timeout" in kwargs:
            self._timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self._timeout
        return super().send(request, **kwargs)


class CouchDb:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._client = sessions.BaseUrlSession(base_url=self._config.url)

        # always call raise_for_status()
        assert_status_hook = (
            lambda response, *args, **kwargs: response.raise_for_status()
        )
        self._client.hooks["response"] = [assert_status_hook]

        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=[
                "HEAD",
                "GET",
                "PUT",
                "POST",
                "DELETE",
                "OPTIONS",
                "TRACE",
            ],
            backoff_factor=1,
        )

        # retry and timeout strategy
        timeout_adapter = TimeoutHTTPAdapter(
            timeout=self._config.timeout, max_retries=retry_strategy
        )
        self._client.mount("http://", timeout_adapter)
        self._client.mount("https://", timeout_adapter)

        # authentication
        if self._config.user and self._config.password:
            self._client.auth = (self._config.user, self._config.password)

        # always use headers on each request
        self._client.headers.update(
            {
                "Content-Type": "application/json",
                "Accept-Charset": "utf-8",
                "Cache-Control": "no-cache",
            }
        )

    def close(self) -> None:
        self._client.close()

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
            else:
                sys.exit(
                    f"HTTPError connection with base_url {self._client.base_url} {str(e)}"
                )

        except ConnectionError as e:
            sys.exit(f"Error connecting with base_url {self._client.base_url} {str(e)}")

        except ConnectTimeout as e:
            sys.exit(
                f"Timeout Error connecting with base_url {self._client.base_url} {str(e)}"
            )

        except InvalidURL as e:
            sys.exit(f"Got an invalid url {str(e)}")

        except MissingSchema as e:
            sys.exit(f"Got an invalid url with missing schema {str(e)}")

        except Exception as e:
            print(type(e))
            print(e)
            sys.exit(f"Got unexpected exception: {str(e)}")

    def create_db(self, db: str, exists_ok_if_empty: bool = True) -> None:
        try:
            with self.handle_web(raise_status=[412]):
                response = self._client.put(f"{db}")
                response.raise_for_status()
        except HTTPError:
            if exists_ok_if_empty:
                db_info = self._client.get(f"{db}")
                db_info.raise_for_status()
                if db_info.json()["doc_count"] == 0:
                    return
                else:
                    sys.exit(f"Database {db} exists but is not empty. Aborting.")
            else:
                sys.exit(f"Database {db} already exists. Aborting.")

    def get_all_docs(self, db: str) -> Iterator[dict]:
        with self.handle_web():
            response = self._client.get(f"{db}/_all_docs?include_docs=true")
            response.raise_for_status()

        data = response.json()
        if "rows" not in data:
            sys.exit(
                f"got unexpected response, 'rows' missing in json. Response was {data}"
            )

        for row in data["rows"]:
            yield row["doc"]

    def insert_doc(self, db: str, doc: dict, same_revision: bool = True) -> None:
        def _get_rev_num_from_doc(doc: dict) -> int:
            return int(doc["_rev"].split("-")[0])

        def _get_rev_num_from_rev(rev: str) -> int:
            return int(rev.split("-")[0])

        doc_id = doc["_id"]
        doc_rev_num = _get_rev_num_from_doc(doc)
        del doc["_rev"]

        with self.handle_web():
            response = self._client.put(f"{db}/{doc_id}", json=doc)
            response.raise_for_status()

        if not same_revision or doc_rev_num == "1":
            return

        with self.handle_web():
            response = self._client.get(f"{db}/{doc_id}")
            response.raise_for_status()

        current_doc_rev = response.json()["_rev"]
        current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)

        while current_doc_rev_num < doc_rev_num:
            with self.handle_web():
                updatresponse = self._client.put(
                    f"{db}/{doc_id}?rev={current_doc_rev}", json=doc
                )
                updatresponse.raise_for_status()

            with self.handle_web():
                response = self._client.get(f"{db}/{doc_id}")
                response.raise_for_status()

            current_doc_rev = response.json()["_rev"]
            current_doc_rev_num = _get_rev_num_from_rev(current_doc_rev)
