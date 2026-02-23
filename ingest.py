import csv
import time
from pymongo import MongoClient, UpdateOne

# --- Config ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "imdb"
BATCH_SIZE = 5000

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def null(val):
    # Convert \N to None, otherwise return value as-is
    return None if val == r"\N" else val

def int_or_none(val):
    v = null(val)
    try:
        return int(v) if v is not None else None
    except ValueError:
        return None

def load_tsv(path, parse_row, collection, key_field):
    col = db[collection]
    col.drop()  # safe to re-run — clears old data first

    print(f"\nLoading {path} into '{collection}'...")
    start = time.time()
    batch, total = [], 0

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            doc = parse_row(row)
            if doc:
                batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                col.insert_many(batch)
                total += len(batch)
                batch = []
                print(f"  {total:,} rows inserted...", end="\r")

    if batch:
        col.insert_many(batch)
        total += len(batch)

    elapsed = time.time() - start
    print(f"  Done! {total:,} rows in {elapsed:.1f}s")

    # Index on the key field for fast lookups
    col.create_index(key_field)
    print(f"  Index created on '{key_field}'")

# --- Row parsers ---

def parse_movie(row):
    return {
        "tconst":        row["tconst"],
        "title":         null(row["primaryTitle"]),
        "year":          int_or_none(row["startYear"]),
        "runtime":       int_or_none(row["runtimeMinutes"]),
        "genres":        [g for g in row["genres"].split(",") if g != r"\N"] if null(row["genres"]) else [],
    }

def parse_principal(row):
    cat = null(row["category"])
    if cat not in ("actor", "actress", "director"):
        return None  # skip irrelevant roles
    return {
        "tconst":   row["tconst"],
        "nconst":   row["nconst"],
        "category": cat,
    }

def parse_person(row):
    return {
        "nconst":    row["nconst"],
        "name":      null(row["primaryName"]),
        "birthYear": int_or_none(row["birthYear"]),
    }

# --- Run ---
if __name__ == "__main__":
    print("=== IMDB Ingestion ===")
    load_tsv("title.basics.tsv",     parse_movie,     "movies",     "tconst")
    load_tsv("title.principals.tsv", parse_principal, "principals", "tconst")
    db["principals"].create_index("nconst")  # second index on principals
    print("  Extra index created on 'nconst' for principals")
    load_tsv("name.basics.tsv",      parse_person,    "people",     "nconst")
    print("\n✅ All done! Collections: movies, principals, people")