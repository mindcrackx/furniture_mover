import sys
from contextlib import contextmanager
from copy import deepcopy
from typing import Dict, Iterator, List

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

TargetRevNum = int
DocId = str


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

    def insert_bulk_docs(
        self, db: str, docs: List[dict], same_revision: bool = True
    ) -> None:
        def _get_rev_num(rev) -> TargetRevNum:
            return TargetRevNum(rev.split("-")[0])

        initial_insert: List[dict] = deepcopy(docs)
        for doc in initial_insert:
            del doc["_rev"]

        mapping_target_revnum: Dict[DocId, TargetRevNum] = {}
        mapping_docid_to_doc: Dict[DocId, dict] = {}
        if same_revision:
            for doc in docs:
                # ignore revision 1 (only docs with rev 2 and up have to be updated again)
                doc_revnum = _get_rev_num(doc["_rev"])
                if doc_revnum > 1:
                    mapping_docid_to_doc[doc["_id"]] = doc
                    mapping_target_revnum[doc["_id"]] = doc_revnum

        # initial insert
        with self.handle_web():
            response = self._client.post(
                f"{db}/_bulk_docs", json={"docs": initial_insert}
            )
            del initial_insert

            for doc_info in response.json():
                if "error" in doc_info:
                    sys.exit(f"Error inserting docs: {doc_info}")
                elif same_revision:
                    if mapping_target_revnum.get(doc_info["id"], None):
                        mapping_docid_to_doc[doc_info["id"]]["_rev"] = doc_info["rev"]

        # if only revision 1 is needed, then we are finished here.
        if not same_revision:
            return

        # 1 bulk update for each revision
        while len(mapping_target_revnum) > 0:
            with self.handle_web():
                response = self._client.post(
                    f"{db}/_bulk_docs",
                    json={"docs": list(mapping_docid_to_doc.values())},
                )
                for doc_info in response.json():
                    if "error" in doc_info:
                        sys.exit(f"Error updating doc: {doc_info}")
                    if (
                        _get_rev_num(doc_info["rev"])
                        == mapping_target_revnum[doc_info["id"]]
                    ):
                        del mapping_target_revnum[doc_info["id"]]
                        del mapping_docid_to_doc[doc_info["id"]]
                    else:
                        mapping_docid_to_doc[doc_info["id"]]["_rev"] = doc_info["rev"]
