import httpx
import pytest

MASTER_DB = "master_testdb"

DOCS = [
    (3, {"_id": "testdoc_1"}),
    (1, {"_id": "testdoc_2", "test": "test"}),
    (15, {"_id": "testdoc_3", "test": "test", "another": "test"}),
    (1, {"_id": "testdoc_4"}),
]


def get_rev_num_from_doc(doc: dict) -> int:
    return int(doc["_rev"].split("-")[0])


@pytest.fixture(scope="module")
def setup_masterdb() -> None:
    with httpx.Client(
        base_url="http://localhost:5984/", auth=("admin", "adminadmin"), timeout=3
    ) as client:
        response = client.get(f"{MASTER_DB}")
        if response.status_code == 200:
            client.delete(f"{MASTER_DB}")

        response = client.put(f"{MASTER_DB}")
        assert response.status_code == 201

        for rev, doc in DOCS:
            client.put(f"{MASTER_DB}/{doc['_id']}", json=doc)
            current = client.get(f"{MASTER_DB}/{doc['_id']}")

            while rev > get_rev_num_from_doc(current.json()):
                client.put(
                    f"{MASTER_DB}/{doc['_id']}?rev={current.json()['_rev']}", json=doc
                )
                current = client.get(f"{MASTER_DB}/{doc['_id']}")


@pytest.fixture(scope="function")
def drop_dbs() -> None:
    with httpx.Client(
        base_url="http://localhost:5984/", auth=("admin", "adminadmin"), timeout=3
    ) as client:

        all_dbs = client.get("_all_dbs").json()
        for db in all_dbs:
            if db == MASTER_DB:
                continue
            response = client.delete(f"{db}")
            assert response.status_code == 200
