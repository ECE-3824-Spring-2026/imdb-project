from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["imdb"]

movies     = db["movies"]
principals = db["principals"]
people     = db["people"]

def header(n, title):
    print(f"\n{'='*55}")
    print(f"  Query {n}: {title}")
    print(f"{'='*55}")

# ----------------------------------------------------------
# Q1: How many movies are in the database?
# ----------------------------------------------------------
header(1, "Total number of movies")
count = movies.count_documents({})
print(f"  Total movies: {count:,}")

# ----------------------------------------------------------
# Q2: All movies released in 1994
# ----------------------------------------------------------
header(2, "Movies released in 1994")
results = movies.find({"year": 1994}, {"title": 1, "genres": 1, "_id": 0}).limit(15)
for r in results:
    genres = ", ".join(r.get("genres", [])) or "N/A"
    print(f"  {r['title']}  [{genres}]")

# ----------------------------------------------------------
# Q3: Top 10 most prolific actors/actresses
# ----------------------------------------------------------
header(3, "Top 10 most prolific actors/actresses")
pipeline = [
    {"$match": {"category": {"$in": ["actor", "actress"]}}},
    {"$group": {"_id": "$nconst", "credits": {"$sum": 1}}},
    {"$sort": {"credits": -1}},
    {"$limit": 10},
]
for doc in principals.aggregate(pipeline):
    person = people.find_one({"nconst": doc["_id"]}, {"name": 1})
    name = person["name"] if person else doc["_id"]
    print(f"  {name:30s}  {doc['credits']} credits")

# ----------------------------------------------------------
# Q4: All movies a given person appeared in (sorted by year)
# ----------------------------------------------------------
header(4, "Movies featuring Tom Hanks (sorted by year)")
person = people.find_one({"name": "Tom Hanks"})
if person:
    nconst = person["nconst"]
    credits = principals.find({"nconst": nconst}, {"tconst": 1})
    tconsts = [c["tconst"] for c in credits]
    films = movies.find(
        {"tconst": {"$in": tconsts}},
        {"title": 1, "year": 1, "_id": 0}
    ).sort("year", 1)
    for f in films:
        print(f"  {f.get('year', 'N/A')}  {f['title']}")
else:
    print("  Person not found.")

# ----------------------------------------------------------
# Q5: Movies directed by a specific director
# ----------------------------------------------------------
header(5, "Movies directed by Christopher Nolan")
person = people.find_one({"name": "Christopher Nolan"})
if person:
    nconst = person["nconst"]
    credits = principals.find({"nconst": nconst, "category": "director"}, {"tconst": 1})
    tconsts = [c["tconst"] for c in credits]
    films = movies.find(
        {"tconst": {"$in": tconsts}},
        {"title": 1, "year": 1, "_id": 0}
    ).sort("year", 1)
    for f in films:
        print(f"  {f.get('year', 'N/A')}  {f['title']}")
else:
    print("  Director not found.")

# ----------------------------------------------------------
# Q6: Most common genre in the database
# ----------------------------------------------------------
header(6, "Most common genres")
pipeline = [
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5},
]
for doc in movies.aggregate(pipeline):
    print(f"  {doc['_id']:20s}  {doc['count']:,} movies")

# ----------------------------------------------------------
# Q7: Movies that two specific actors both appeared in
# ----------------------------------------------------------
header(7, "Movies featuring both Matt Damon and Ben Affleck")
names = ["Matt Damon", "Ben Affleck"]
nconsts = []
for name in names:
    p = people.find_one({"name": name})
    if p:
        nconsts.append(p["nconst"])

if len(nconsts) == 2:
    sets = []
    for nc in nconsts:
        tconsts = set(c["tconst"] for c in principals.find({"nconst": nc}, {"tconst": 1}))
        sets.append(tconsts)
    shared = sets[0] & sets[1]
    films = movies.find(
        {"tconst": {"$in": list(shared)}},
        {"title": 1, "year": 1, "_id": 0}
    ).sort("year", 1)
    for f in films:
        print(f"  {f.get('year', 'N/A')}  {f['title']}")
else:
    print("  One or more people not found.")

# ----------------------------------------------------------
# Q8: Which year had the most movies released?
# ----------------------------------------------------------
header(8, "Top 10 years by number of movies released")
pipeline = [
    {"$match": {"year": {"$ne": None}}},
    {"$group": {"_id": "$year", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10},
]
for doc in movies.aggregate(pipeline):
    print(f"  {doc['_id']}  —  {doc['count']:,} movies")

print(f"\n{'='*55}")
print("  All queries complete.")
print(f"{'='*55}\n")