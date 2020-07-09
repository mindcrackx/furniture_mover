import json
from tempfile import NamedTemporaryFile

from requests_toolbelt import sessions
from typer.testing import CliRunner

from furniture_mover.__main__ import app
from tests.functional_tests.conftest import DOCS, MASTER_DB, get_rev_num_from_doc

runner = CliRunner()


def test_invalid_no_credentials(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(app, ["export", filename, MASTER_DB])
    assert result.exit_code == 1
    assert "Unauthorized: User or Password is wrong or missing." in result.stdout

    result = runner.invoke(app, ["import", filename, MASTER_DB])
    assert result.exit_code == 1
    assert "Unauthorized: User or Password is wrong or missing." in result.stdout


def test_export(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        ["export", "--user", "admin", "--password", "adminadmin", filename, MASTER_DB],
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
            "import",
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

    with sessions.BaseUrlSession(
        base_url="http://localhost:5984/import_testdb/",
    ) as client:
        client.auth = ("admin", "adminadmin")
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
            "import",
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


def test_url_not_found(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        [
            "export",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--url",
            "http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/",
            "--timeout",
            "0.2",
            filename,
            "import_testdb",
        ],
    )
    print(result.stdout)
    assert result.exit_code == 1
    assert (
        "Error connecting with base_url http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/"
        in result.stdout
    )

    result = runner.invoke(
        app,
        [
            "import",
            "--user",
            "admin",
            "--password",
            "adminadmin",
            "--url",
            "http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/",
            "--timeout",
            "0.2",
            filename,
            "import_testdb",
        ],
    )
    print(result.stdout)
    assert result.exit_code == 1
    assert (
        "Error connecting with base_url http://lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge/"
        in result.stdout
    )


def test_missing_schema(setup_masterdb, drop_dbs):
    with NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name
        print(filename)

    result = runner.invoke(
        app,
        [
            "export",
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
        "Got an invalid url with missing schema Invalid URL 'lhkjhasgdfbkajvchlaskjehrnvasgfhsalkdjfhabsfkhge"  # noqa
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
            "import",
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

    with sessions.BaseUrlSession(
        base_url="http://localhost:5984/import_testdb/",
    ) as client:
        client.auth = ("admin", "adminadmin")
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

    with sessions.BaseUrlSession(base_url="http://localhost:5984/") as client:
        client.auth = ("admin", "adminadmin")
        response = client.put("import_testdb_exists")
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "import",
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

    with sessions.BaseUrlSession(base_url="http://localhost:5984/") as client:
        client.auth = ("admin", "adminadmin")
        response = client.put("import_testdb_exists")
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "import",
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

    with sessions.BaseUrlSession(
        base_url="http://localhost:5984/import_testdb_exists/",
    ) as client:
        client.auth = ("admin", "adminadmin")
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

    with sessions.BaseUrlSession(base_url="http://localhost:5984/") as client:
        client.auth = ("admin", "adminadmin")
        response = client.put("import_testdb_exists")
        assert response.status_code == 201
        # insert 1 doc
        response = client.put("import_testdb_exists/testdoc", json={"test": "test"})
        assert response.status_code == 201

    result = runner.invoke(
        app,
        [
            "import",
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


def test_export_from_all_docs_file():
    data = """
{"total_rows":4,"offset":0,"rows":[
{"id":"testdoc_1","key":"testdoc_1","value":{"rev":"3-825cb35de44c433bfb2df415563a19de"},"doc":{"_id":"testdoc_1","_rev":"3-825cb35de44c433bfb2df415563a19de"}},
{"id":"testdoc_2","key":"testdoc_2","value":{"rev":"1-c3d84a0ca6114a8e8fbef75dc8c7be00"},"doc":{"_id":"testdoc_2","_rev":"1-c3d84a0ca6114a8e8fbef75dc8c7be00","test":"test"}},
{"id":"testdoc_3","key":"testdoc_3","value":{"rev":"15-305afde91ffef71edcff06458e17c186"},"doc":{"_id":"testdoc_3","_rev":"15-305afde91ffef71edcff06458e17c186","test":"test","another":"test"}},
{"id":"testdoc_4","key":"testdoc_4","value":{"rev":"1-967a00dff5e02add41819138abb3284d"},"doc":{"_id":"testdoc_4","_rev":"1-967a00dff5e02add41819138abb3284d"}}
]}
    """.strip()

    with NamedTemporaryFile() as tmpfile1:
        filename1 = tmpfile1.name
        print(filename1)
    with NamedTemporaryFile() as tmpfile2:
        filename2 = tmpfile2.name
        print(filename2)

    with open(filename1, "w", encoding="utf8") as inf:
        inf.write(data)

    result = runner.invoke(app, ["export_from_all_docs_file", filename1, filename2])
    print(result.stdout)
    assert result.exit_code == 0

    expected_output = """
{"_id": "testdoc_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"}
{"_id": "testdoc_2", "_rev": "1-c3d84a0ca6114a8e8fbef75dc8c7be00", "test": "test"}
{"_id": "testdoc_3", "_rev": "15-305afde91ffef71edcff06458e17c186", "test": "test", "another": "test"}
{"_id": "testdoc_4", "_rev": "1-967a00dff5e02add41819138abb3284d"}
""".lstrip()

    with open(filename2, "r", encoding="utf8") as outf:
        assert outf.read() == expected_output


def test_filter():
    with NamedTemporaryFile() as tmpfile_filter:
        filename_filter = tmpfile_filter.name
        print(filename_filter)

    with NamedTemporaryFile() as tmpfile_outfile_1:
        filename_outfile_1 = tmpfile_outfile_1.name
        print(filename_outfile_1)

    with NamedTemporaryFile() as tmpfile_outfile_2:
        filename_outfile_2 = tmpfile_outfile_2.name
        print(filename_outfile_2)

    with NamedTemporaryFile() as tmpfile_infile:
        filename_infile = tmpfile_infile.name
        print(filename_infile)

    with open(filename_filter, "w", encoding="utf-8") as inf:
        filter_ = [
            {"filepath": filename_outfile_1, "regex_filters": ["^.*?_a_\\d+$"]},
            {
                "filepath": filename_outfile_2,
                "regex_filters": ["^.*?_b_\\d+$", "^.*?_c_\\d+$"],
            },
        ]
        inf.write(json.dumps(filter_, ensure_ascii=False, indent=4))

    with open(filename_infile, "w", encoding="utf-8") as inf:
        data = """
{"_id": "test_a_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"}
{"_id": "test_a_2", "_rev": "1-34333f0454a81dc3559c356a5df072fc", "test": "test_a"}
{"_id": "test_b_1", "_rev": "4-f6647f1364a5944f9dcd3b9bf77329bd", "test": "b"}
{"_id": "test_b_3", "_rev": "2-7051cbe5c8faecd085a3fa619e6e6337"}
{"_id": "test_c_1", "_rev": "1-967a00dff5e02add41819138abb3284d"}
""".strip()
        inf.write(data)

    result = runner.invoke(app, ["filter", filename_filter, filename_infile])

    print(result.stdout)
    assert result.exit_code == 0

    with open(filename_outfile_1, "r", encoding="utf-8") as outf1:
        expected_output_1 = """
{"_id": "test_a_1", "_rev": "3-825cb35de44c433bfb2df415563a19de"}
{"_id": "test_a_2", "_rev": "1-34333f0454a81dc3559c356a5df072fc", "test": "test_a"}
""".lstrip()
        assert expected_output_1 == outf1.read()

    with open(filename_outfile_2, "r", encoding="utf-8") as outf2:
        expected_output_2 = """
{"_id": "test_b_1", "_rev": "4-f6647f1364a5944f9dcd3b9bf77329bd", "test": "b"}
{"_id": "test_b_3", "_rev": "2-7051cbe5c8faecd085a3fa619e6e6337"}
{"_id": "test_c_1", "_rev": "1-967a00dff5e02add41819138abb3284d"}
""".lstrip()
        assert expected_output_2 == outf2.read()
