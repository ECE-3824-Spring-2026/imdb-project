# Docker Setup

## Start the MongoDB container

```bash
docker-compose up -d
```

## Verify it's running

```bash
docker ps
```

You should see `imdb-mongo` in the list with status `Up`.

## Stop the container

```bash
docker-compose down
```

## Connect with the Mongo shell (optional)

```bash
docker exec -it imdb-mongo mongosh
```

Once inside:
```javascript
use imdb
db.movies.countDocuments()
db.people.countDocuments()
db.principals.countDocuments()
```
