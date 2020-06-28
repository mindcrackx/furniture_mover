# Furniture_Mover
Program for easy export and import of a couchdb_database to and from files.


## Dev:
Install pre-commit-hooks
```
pre-commit install
```

Run before commit:
```
poetry run isort -rc furniture_mover
poetry run isort -rc tests

poetry run black furniture_mover
poetry run black tests

poetry run flake8 furniture_mover
poetry run flake8 tests

poetry run mypy furniture_mover
poetry run mypy tests
```

## Testing:
Test against a docker couchdb
```
docker run -p 5984:5984 -d --name furniture_mover_couchdb -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=adminadmin couchdb
```
```
poetry run pytest -v
```
