"""
queries.py — Run 8+ queries against the IMDB MongoDB database.

Usage:
    python queries.py
"""

from pymongo import MongoClient

# ── Connect (same style as the in-class activity) ─────────────────────────────
client = MongoClient('localhost', 27017)
db = client['imdb']

movies     = db['movies']
principals = db['principals']
people     = db['people']


def divider(n, title):
    print(f'\n{"─" * 60}')
    print(f'  Query {n}: {title}')
    print(f'{"─" * 60}')


# ── Query 1: How many movies are in the database? ────────────────────────────
divider(1, 'Total number of movies in the database')

count = movies.count_documents({})
print(f'  Total movies: {count:,}')


# ── Query 2: All movies released in 1994 ─────────────────────────────────────
divider(2, 'Movies released in 1994')

for m in movies.find({'year': 1994}).sort('title', 1).limit(20):
    genres = ', '.join(m['genres']) if m.get('genres') else 'N/A'
    print(f"  {m['title']}  ({genres})")

total_1994 = movies.count_documents({'year': 1994})
print(f'\n  (showing first 20 of {total_1994:,} total)')


# ── Query 3: Top 10 most prolific actors/actresses ───────────────────────────
divider(3, 'Top 10 most prolific actors/actresses by number of movie credits')

pipeline = [
    {'$match':  {'category': {'$in': ['actor', 'actress']}}},
    {'$group':  {'_id': '$nconst', 'credits': {'$sum': 1}}},
    {'$sort':   {'credits': -1}},
    {'$limit':  10},
]

for result in principals.aggregate(pipeline):
    person = people.find_one({'_id': result['_id']})
    name = person['name'] if person else result['_id']
    print(f"  {name:<30} {result['credits']} credits")


# ── Query 4: All movies a given actor appeared in, sorted by year ─────────────
divider(4, "All movies featuring Tom Hanks, sorted by year")

actor = people.find_one({'name': 'Tom Hanks'})
if actor:
    # Get all tconsts where Tom Hanks appears
    tconsts = [p['tconst'] for p in principals.find({'nconst': actor['_id']})]

    for m in movies.find({'_id': {'$in': tconsts}}).sort('year', 1):
        genres = ', '.join(m['genres']) if m.get('genres') else 'N/A'
        print(f"  {m['year']}  {m['title']}  ({genres})")
else:
    print('  Tom Hanks not found.')


# ── Query 5: All movies directed by a specific director ──────────────────────
divider(5, 'Movies directed by Christopher Nolan')

director = people.find_one({'name': 'Christopher Nolan'})
if director:
    tconsts = [p['tconst'] for p in principals.find({'nconst': director['_id'], 'category': 'director'})]

    for m in movies.find({'_id': {'$in': tconsts}}).sort('year', 1):
        print(f"  {m['year']}  {m['title']}")
else:
    print('  Christopher Nolan not found.')


# ── Query 6: Most common genre in the database ───────────────────────────────
divider(6, 'Most common genres')

pipeline = [
    {'$unwind': '$genres'},
    {'$match':  {'genres': {'$ne': None}}},
    {'$group':  {'_id': '$genres', 'count': {'$sum': 1}}},
    {'$sort':   {'count': -1}},
    {'$limit':  10},
]

for result in movies.aggregate(pipeline):
    print(f"  {result['_id']:<20} {result['count']:,} movies")


# ── Query 7: Movies two specific actors both appeared in ─────────────────────
divider(7, 'Movies featuring BOTH Matt Damon AND Ben Affleck')

person_a = people.find_one({'name': 'Matt Damon'})
person_b = people.find_one({'name': 'Ben Affleck'})

if person_a and person_b:
    tconsts_a = set(p['tconst'] for p in principals.find({'nconst': person_a['_id']}))
    tconsts_b = set(p['tconst'] for p in principals.find({'nconst': person_b['_id']}))

    shared = tconsts_a & tconsts_b

    for m in movies.find({'_id': {'$in': list(shared)}}).sort('year', 1):
        print(f"  {m['year']}  {m['title']}")
else:
    print('  One or both people not found.')


# ── Query 8: Average runtime by genre ────────────────────────────────────────
divider(8, 'Average runtime (minutes) by genre — longest first')

pipeline = [
    {'$unwind': '$genres'},
    {'$match':  {'genres': {'$ne': None}, 'runtime_min': {'$ne': None}}},
    {'$group':  {'_id': '$genres',
                 'avg_runtime': {'$avg': '$runtime_min'},
                 'count':       {'$sum': 1}}},
    {'$match':  {'count': {'$gte': 50}}},
    {'$sort':   {'avg_runtime': -1}},
]

for result in movies.aggregate(pipeline):
    print(f"  {result['_id']:<20} avg {result['avg_runtime']:.1f} min  ({result['count']:,} movies)")


# ── Query 9 (own question): Which year had the most movies released? ──────────
divider(9, 'Your own: Which years had the most movies released?')

pipeline = [
    {'$group': {'_id': '$year', 'count': {'$sum': 1}}},
    {'$sort':  {'count': -1}},
    {'$limit': 10},
]

for result in movies.aggregate(pipeline):
    print(f"  {result['_id']}  —  {result['count']:,} movies")


print(f'\n{"═" * 60}')
print('  All queries complete.')
print(f'{"═" * 60}\n')
