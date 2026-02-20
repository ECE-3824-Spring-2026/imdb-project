#!/usr/bin/env python3
"""
Filter IMDB TSV files:
  - Keep only titles with startYear > 1990 and isAdult == 0
  - Keep only principals for those titles
  - Keep only names that appear in those principals
Output files go into ./filtered/
"""

import os
import sys

INPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(INPUT_DIR, "filtered")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TITLES_IN  = os.path.join(INPUT_DIR, "title.basics.tsv")
PRINC_IN   = os.path.join(INPUT_DIR, "title.principals.tsv")
NAMES_IN   = os.path.join(INPUT_DIR, "name.basics.tsv")

TITLES_OUT = os.path.join(OUTPUT_DIR, "title.basics.tsv")
PRINC_OUT  = os.path.join(OUTPUT_DIR, "title.principals.tsv")
NAMES_OUT  = os.path.join(OUTPUT_DIR, "name.basics.tsv")


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Pass 1 — title.basics: keep post-1990, non-adult titles
# ---------------------------------------------------------------------------
log("Pass 1/3  filtering title.basics.tsv ...")

valid_tconst = set()

with open(TITLES_IN, "r", encoding="utf-8") as fin, \
     open(TITLES_OUT, "w", encoding="utf-8") as fout:

    header = fin.readline()
    fout.write(header)

    # Locate column indices from header
    cols = header.rstrip("\n").split("\t")
    idx_isAdult    = cols.index("isAdult")
    idx_startYear  = cols.index("startYear")
    idx_tconst     = cols.index("tconst")
    idx_titleType  = cols.index("titleType")

    kept = 0
    total = 0
    for line in fin:
        total += 1
        fields = line.rstrip("\n").split("\t")
        is_adult    = fields[idx_isAdult]
        start_year  = fields[idx_startYear]
        title_type  = fields[idx_titleType]

        if title_type != "movie":
            continue
        if is_adult == "1":
            continue
        if start_year == r"\N" or not start_year.lstrip("-").isdigit():
            continue
        if int(start_year) <= 1990:
            continue

        valid_tconst.add(fields[idx_tconst])
        fout.write(line)
        kept += 1

log(f"  {kept:,} / {total:,} titles kept  ({len(valid_tconst):,} unique tconst)")


# ---------------------------------------------------------------------------
# Pass 2 — title.principals: keep rows whose tconst is in valid set
# ---------------------------------------------------------------------------
log("Pass 2/3  filtering title.principals.tsv ...")

valid_nconst = set()

with open(PRINC_IN, "r", encoding="utf-8") as fin, \
     open(PRINC_OUT, "w", encoding="utf-8") as fout:

    header = fin.readline()
    fout.write(header)

    cols = header.rstrip("\n").split("\t")
    idx_tconst   = cols.index("tconst")
    idx_nconst   = cols.index("nconst")
    idx_category = cols.index("category")

    KEEP_CATEGORIES = {"actor", "actress", "director"}

    kept = 0
    total = 0
    for line in fin:
        total += 1
        fields = line.rstrip("\n").split("\t")
        tconst = fields[idx_tconst]
        if tconst not in valid_tconst:
            continue
        if fields[idx_category] not in KEEP_CATEGORIES:
            continue
        valid_nconst.add(fields[idx_nconst])
        fout.write(line)
        kept += 1

log(f"  {kept:,} / {total:,} principal rows kept  ({len(valid_nconst):,} unique nconst)")


# ---------------------------------------------------------------------------
# Pass 3 — name.basics: keep rows whose nconst is in valid set
# ---------------------------------------------------------------------------
log("Pass 3/3  filtering name.basics.tsv ...")

with open(NAMES_IN, "r", encoding="utf-8") as fin, \
     open(NAMES_OUT, "w", encoding="utf-8") as fout:

    header = fin.readline()
    fout.write(header)

    cols = header.rstrip("\n").split("\t")
    idx_nconst = cols.index("nconst")

    kept = 0
    total = 0
    for line in fin:
        total += 1
        nconst = line.split("\t", 1)[0]   # nconst is always first column
        if nconst not in valid_nconst:
            continue
        fout.write(line)
        kept += 1

log(f"  {kept:,} / {total:,} name rows kept")

log(f"\nDone. Filtered files written to:  {OUTPUT_DIR}/")
