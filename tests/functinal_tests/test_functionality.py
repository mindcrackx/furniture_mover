import json
from tempfile import NamedTemporaryFile

import httpx
import typer
from typer.testing import CliRunner

from furniture_mover.__main__ import main
from tests.functinal_tests.conftest import DOCS, MASTER_DB, get_rev_num_from_doc

app = typer.Typer()
app.command()(main)

runner = CliRunner()


def test_invalid_no_credentials(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(app, ["--export", filename, MASTER_DB])
    assert result.exit_code == 1
    assert "Unauthorized: User or Password is wrong or missing." in result.stdout

    result = runner.invoke(app, ["--import", filename, MASTER_DB])
    assert result.exit_code == 1
    assert "Unauthorized: User or Password is wrong or missing." in result.stdout


def test_export(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        [
            "--export",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            filename,
            MASTER_DB,
        ],
    )
    data = []
    with open(filename, "r", encoding="utf8") as inf:
        for line in inf:
            doc = json.loads(line)
            rev_num = get_rev_num_from_doc(doc)
            del doc["_rev"]
            data.append((rev_num, doc))

    assert result.exit_code == 0

    assert len(data) == len(DOCS)
    assert data == DOCS


def test_import(setup_masterdb, drop_dbs):
    data = [
        {"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"},
        {
            "_id": "testdoc_2",
            "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00",
            "test": "test",
        },
        {
            "_id": "testdoc_3",
            "_rev": "15-305afde91ffef71edcff06458e17c186",
            "test": "test",
            "another": "test",
        },
        {"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"},
    ]
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    with open(filename, "w", encoding="utf8") as inf:
        inf.write("\n".join(json.dumps(x) for x in data))

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            filename,
            "import_testdb",
        ],
    )

    print(result.stdout)
    assert result.exit_code == 0

    with httpx.Client(
        base_url="http://localhost:5984/import_testdb/",
        auth=("admin", "adminadmin"),
        timeout=3,
    ) as client:
        assert len(data) == client.get("").json()["doc_count"]
        for doc in data:
            response = client.get(doc["_id"])
            assert response.status_code == 200
            assert doc == response.json()


def test_import_file_does_not_exist(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    # tmpfile does not exist here / is deleted
    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            filename,
            "import_testdb",
        ],
    )
    assert result.exit_code == 1
    assert (
        "Exception opening or writing file: [Errno 2] No such file or directory: "
        in result.stdout
    )


def test_url_not_fount(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        [
            "--export",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--url",
            "http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/",
            filename,
            "import_testdb",
        ],
    )
    print(result.stdout)
    assert result.exit_code == 1
    assert (
        "Error connecting with base_url http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/ [Errno 11001] getaddrinfo failed"  # noqa
        in result.stdout
    )

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--url",
            "http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/",
            filename,
            "import_testdb",
        ],
    )
    print(result.stdout)
    assert result.exit_code == 1
    assert (
        "Error connecting with base_url http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/ [Errno 11001] getaddrinfo failed"  # noqa
        in result.stdout
    )


def test_invalid_url(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        [
            "--export",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--url",
            "lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/",
            filename,
            "import_testdb",
        ],
    )
    print(result.stdout)
    assert result.exit_code == 1
    assert (
        "Got an invalid URL lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/. No scheme included in URL."  # noqa
        in result.stdout
    )


def test_import_with_no_same_revision(setup_masterdb, drop_dbs):
    data = [
        {"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"},
        {
            "_id": "testdoc_2",
            "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00",
            "test": "test",
        },
        {
            "_id": "testdoc_3",
            "_rev": "15-305afde91ffef71edcff06458e17c186",
            "test": "test",
            "another": "test",
        },
        {"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"},
    ]
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    with open(filename, "w", encoding="utf8") as inf:
        inf.write("\n".join(json.dumps(x) for x in data))

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--no-same-revision",
            filename,
            "import_testdb",
        ],
    )

    print(result.stdout)
    assert result.exit_code == 0

    with httpx.Client(
        base_url="http://localhost:5984/import_testdb/",
        auth=("admin", "adminadmin"),
        timeout=3,
    ) as client:
        assert len(data) == client.get("").json()["doc_count"]
        for doc in data:
            response = client.get(doc["_id"])
            response_json = response.json()
            assert response.status_code == 200
            assert response_json["_rev"].startswith("1-")

            del doc["_rev"]
            del response_json["_rev"]
            assert doc == response_json


def test_import_with_no_db_exists_ok_if_empty__empty_db_exists(
    setup_masterdb, drop_dbs
):
    data = [
        {"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"},
        {
            "_id": "testdoc_2",
            "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00",
            "test": "test",
        },
        {
            "_id": "testdoc_3",
            "_rev": "15-305afde91ffef71edcff06458e17c186",
            "test": "test",
            "another": "test",
        },
        {"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"},
    ]
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    with open(filename, "w", encoding="utf8") as inf:
        inf.write("\n".join(json.dumps(x) for x in data))

    with httpx.Client(
        base_url="http://localhost:5984/", auth=("admin", "adminadmin"), timeout=3,
    ) as client:
        response = client.put("import_testdb_exists")
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--no-db-exists-ok-if-empty",
            filename,
            "import_testdb_exists",
        ],
    )

    print(result.stdout)
    assert "Database import_testdb_exists already exists. Aborting." in result.stdout
    assert result.exit_code == 1


def test_import_with_db_exists_ok_if_empty__empty_db_exists(setup_masterdb, drop_dbs):
    data = [
        {"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"},
        {
            "_id": "testdoc_2",
            "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00",
            "test": "test",
        },
        {
            "_id": "testdoc_3",
            "_rev": "15-305afde91ffef71edcff06458e17c186",
            "test": "test",
            "another": "test",
        },
        {"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"},
    ]
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    with open(filename, "w", encoding="utf8") as inf:
        inf.write("\n".join(json.dumps(x) for x in data))

    with httpx.Client(
        base_url="http://localhost:5984/", auth=("admin", "adminadmin"), timeout=3,
    ) as client:
        response = client.put("import_testdb_exists")
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            filename,
            "import_testdb_exists",
        ],
    )

    print(result.stdout)
    assert result.exit_code == 0

    with httpx.Client(
        base_url="http://localhost:5984/import_testdb_exists/",
        auth=("admin", "adminadmin"),
        timeout=3,
    ) as client:
        assert len(data) == client.get("").json()["doc_count"]
        for doc in data:
            response = client.get(doc["_id"])
            assert response.status_code == 200
            assert doc == response.json()


def test_import_with_db_exists_ok_if_empty__NOT_empty_db_exists(
    setup_masterdb, drop_dbs
):
    data = [
        {"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"},
        {
            "_id": "testdoc_2",
            "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00",
            "test": "test",
        },
        {
            "_id": "testdoc_3",
            "_rev": "15-305afde91ffef71edcff06458e17c186",
            "test": "test",
            "another": "test",
        },
        {"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"},
    ]
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    with open(filename, "w", encoding="utf8") as inf:
        inf.write("\n".join(json.dumps(x) for x in data))

    with httpx.Client(
        base_url="http://localhost:5984/", auth=("admin", "adminadmin"), timeout=3,
    ) as client:
        response = client.put("import_testdb_exists")
        assert response.status_code == 201
        # insert 1 doc
        response = client.put("import_testdb_exists/testdoc", json={"test": "test"})
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "--import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            filename,
            "import_testdb_exists",
        ],
    )

    print(result.stdout)
    assert (
        "Database import_testdb_exists exists but is not empty. Aborting."
        in result.stdout
    )
    assert result.exit_code == 1
