"""
ingest.py — Load IMDB TSV files into MongoDB.

Usage:
    python ingest.py

Expects these files in the current directory:
    title.basics.tsv
    title.principals.tsv
    name.basics.tsv

Safe to re-run — drops and recreates collections each time.
"""

import csv
import time
from pymongo import MongoClient

# ── Connect ───────────────────────────────────────────────────────────────────
client = MongoClient('localhost', 27017)
db = client['imdb']

movies     = db['movies']
principals = db['principals']
people     = db['people']

BATCH_SIZE = 5000


def clean(value):
    """Convert the \\N sentinel to None."""
    return None if value == r'\N' else value


def clean_list(value):
    """Convert a comma-separated string to a list, or None."""
    if value == r'\N':
        return None
    return value.split(',')


def load_tsv(filepath, collection, transform, label):
    """Read a TSV file and bulk-insert transformed rows into a collection."""
    collection.drop()

    batch = []
    total = 0
    start = time.time()

    with open(filepath, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            doc = transform(row)
            if doc is None:
                continue
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch = []
                print(f'  {label}: {total:,} rows...', end='\r')

    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)

    elapsed = time.time() - start
    print(f'  {label}: {total:,} rows loaded in {elapsed:.1f}s')


# ── Transformers ──────────────────────────────────────────────────────────────

def transform_movie(row):
    # Only keep movies (not TV shows, shorts, etc.) released after 1990
    if row['titleType'] != 'movie':
        return None
    year = clean(row['startYear'])
    if year is None or int(year) <= 1990:
        return None
    return {
        '_id':         row['tconst'],
        'title':       clean(row['primaryTitle']),
        'year':        int(year),
        'runtime_min': int(row['runtimeMinutes']) if clean(row['runtimeMinutes']) else None,
        'genres':      clean_list(row['genres']),
        'is_adult':    row['isAdult'] == '1',
    }


def transform_principal(row):
    # Only keep actors, actresses, and directors
    if row['category'] not in ('actor', 'actress', 'director'):
        return None
    return {
        'tconst':   row['tconst'],
        'nconst':   row['nconst'],
        'category': row['category'],
    }


def transform_person(row):
    return {
        '_id':        row['nconst'],
        'name':       clean(row['primaryName']),
        'birth_year': int(row['birthYear']) if clean(row['birthYear']) else None,
        'death_year': int(row['deathYear']) if clean(row['deathYear']) else None,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

print('=== IMDB Ingestion ===\n')

print('Loading movies...')
load_tsv('title.basics.tsv', movies, transform_movie, 'movies')

print('Loading principals...')
load_tsv('title.principals.tsv', principals, transform_principal, 'principals')

print('Loading people...')
load_tsv('name.basics.tsv', people, transform_person, 'people')

# Create indexes so queries aren't painfully slow
print('\nCreating indexes...')
principals.create_index('tconst')
principals.create_index('nconst')
movies.create_index('year')
movies.create_index('title')
people.create_index('name')
print('  Done.')

print('\n=== Done ===')
print(f'  movies:     {movies.count_documents({}):,}')
print(f'  principals: {principals.count_documents({}):,}')
print(f'  people:     {people.count_documents({}):,}')
