#!/usr/bin/env python3
"""
ingest.py — Load IMDB TSV files into MongoDB.

Collections created inside the 'imdb' database:
  names       ← name.basics.tsv
  titles      ← title.basics.tsv
  principals  ← title.principals.tsv

Safe to re-run: each collection is dropped and rebuilt from scratch.
"""

import os
import time

from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "imdb"
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_SIZE = 10_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _null(value):
    """Return None for IMDB's missing-value sentinel, otherwise the raw string."""
    return None if value == r"\N" else value


def _elapsed(start):
    return f"{time.time() - start:.1f}s"


def _iter_tsv(filepath, transform=None):
    """Yield one dict per data row; applies transform if provided."""
    with open(filepath, "r", encoding="utf-8") as fh:
        headers = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            fields = line.rstrip("\n").split("\t")
            doc = {h: _null(v) for h, v in zip(headers, fields)}
            if transform:
                doc = transform(doc)
            yield doc


# ---------------------------------------------------------------------------
# Per-collection transforms (type coercions, list splitting)
# ---------------------------------------------------------------------------

def _transform_names(doc):
    for field in ("birthYear", "deathYear"):
        if doc[field] is not None:
            doc[field] = int(doc[field])
    if doc["primaryProfession"] is not None:
        doc["primaryProfession"] = doc["primaryProfession"].split(",")
    if doc["knownForTitles"] is not None:
        doc["knownForTitles"] = doc["knownForTitles"].split(",")
    return doc


def _transform_titles(doc):
    for field in ("startYear", "endYear", "runtimeMinutes"):
        if doc[field] is not None:
            doc[field] = int(doc[field])
    doc["isAdult"] = doc["isAdult"] == "1"
    if doc["genres"] is not None:
        doc["genres"] = doc["genres"].split(",")
    return doc


def _transform_principals(doc):
    if doc["ordering"] is not None:
        doc["ordering"] = int(doc["ordering"])
    return doc


# ---------------------------------------------------------------------------
# Core loader
# ---------------------------------------------------------------------------

def load_collection(collection, filepath, transform=None, index_fields=None):
    label = collection.name

    print(f"\n[{label}] dropping existing data ...", flush=True)
    collection.drop()

    t0 = time.time()
    batch = []
    total = 0

    for doc in _iter_tsv(filepath, transform):
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch, ordered=False)
            total += len(batch)
            batch.clear()
            print(f"  {total:>9,} rows  ({_elapsed(t0)})", end="\r", flush=True)

    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)

    print(f"  {total:>9,} rows  ({_elapsed(t0)})  ✓")

    for field in (index_fields or []):
        collection.create_index([(field, ASCENDING)])
        print(f"  index created on '{field}'")

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"Connecting to {MONGO_URI} ...", flush=True)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    try:
        client.server_info()
    except Exception as exc:
        print(f"ERROR: cannot reach MongoDB — {exc}")
        raise SystemExit(1)

    db = client[DB_NAME]
    print(f"Connected.  Database: '{DB_NAME}'")

    t_start = time.time()

    load_collection(
        db["names"],
        os.path.join(DATA_DIR, "name.basics.tsv"),
        transform=_transform_names,
        index_fields=["nconst"],
    )

    load_collection(
        db["titles"],
        os.path.join(DATA_DIR, "title.basics.tsv"),
        transform=_transform_titles,
        index_fields=["tconst"],
    )

    load_collection(
        db["principals"],
        os.path.join(DATA_DIR, "title.principals.tsv"),
        transform=_transform_principals,
        index_fields=["tconst", "nconst"],
    )

    print(f"\nAll done in {_elapsed(t_start)}.")


if __name__ == "__main__":
    main()
