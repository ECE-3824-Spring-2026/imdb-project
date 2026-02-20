from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "imdb"

print(f"Connecting to {MONGO_URI} ...", flush=True)
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
try:
    client.server_info()
except Exception as exc:
    print(f"ERROR: cannot reach MongoDB — {exc}")
    raise SystemExit(1)

db = client[DB_NAME]
titles = db.get_collection('titles')
names  = db.get_collection('names')
princs = db.get_collection('principals')

# 1. count all movies
print("\n" + "="*50)
n_movies = titles.count_documents({})
print(f"There are {n_movies} movies in the database")

# 2. list 10 movies from 2001
print("\n" + "="*50)
print("Ten movies from 2001 are:")
for i,m in enumerate(titles.find({'startYear':2001})):
    print("\t" + m['primaryTitle'])
    if i == 10: break

# 4. List all movies a given person appeard in, sorted by year
print("\n" + "="*50)
print("List all movies Jon Bernthal appeard in, sorted by year")
tb_id = names.find_one({'primaryName':'Jon Bernthal'})['nconst']
tb_princs = princs.find({'nconst':tb_id})
movies = {}
for tbp in tb_princs:
    movie_id = tbp['tconst']
    movie = titles.find_one({'tconst':movie_id})
    movies[movie['primaryTitle']] = movie['startYear']

for tt,yy in sorted(movies.items() , key=lambda item:item[1]):
    print(yy,tt)
