# Furniture_Mover
Program for easy export and import of a couchdb_database to and from files.


## Dev:
Install pre-commit-hooks
```
pre-commit install
```

## Testing:
Test against a docker couchdb
```
docker run -p 5984:5984 -d --name furniture_mover_couchdb -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=adminadmin couchdb
```
