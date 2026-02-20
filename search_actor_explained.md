# How `search_actor` Works

Reference: `frontend/app.py:13`

---

## Step 1 — Find matching people (line 21)

```python
people = list(db.names.find(
    {"primaryName": {"$regex": name.strip(), "$options": "i"}},
    {"_id": 0}
).limit(10))
```

Queries the `names` collection for documents where `primaryName` contains the search string. `$options: "i"` makes it case-insensitive, so "tom" matches "Tom Hanks". Returns at most 10 people.

---

## Step 2 — For each person, run an aggregation pipeline (line 30)

```python
pipeline = [
    {"$match": {"nconst": nconst}},
    ...
]
credits = list(db.principals.aggregate(pipeline))
```

This runs on the `principals` collection — the join table that links people to movies. Each stage:

**`$match`** — filter to only this person's rows using their unique `nconst` ID (e.g. `nm0000001`).

**`$lookup`** — a left join from `principals` onto `titles`:
```python
{"$lookup": {
    "from": "titles",
    "localField": "tconst",      # field in principals
    "foreignField": "tconst",    # matching field in titles
    "as": "title",               # store result as array called "title"
}}
```
For each principal row, MongoDB finds the matching document in `titles` where the `tconst` values match, and attaches it as an array field called `title`.

**`$unwind`** — flattens the `title` array (which always has exactly one element) into a plain embedded object, so you can reference its fields directly.

**`$project`** — selects only the fields you want in the final output, renaming nested fields with `$`:
```python
"primaryTitle": "$title.primaryTitle",   # from the joined titles doc
"startYear":    "$title.startYear",
```

**`$sort`** — orders results by `startYear` descending (newest movies first).

---

## Step 3 — Clean up the `characters` field (line 54)

```python
raw.strip('[]"').replace('","', ", ")
```

The `characters` field in the raw TSV looks like `["Tony Stark"]` — a JSON array stored as a string. This just strips the brackets and quotes to get a readable display value like `Tony Stark`.

---

## The overall flow in one picture

```
names collection          principals collection       titles collection
─────────────────         ────────────────────────    ─────────────────
primaryName = "Tom Hanks" nconst = nm0000158  ──┐    tconst = tt0109830
nconst = nm0000158   ──►  tconst = tt0109830   └──►  primaryTitle = "Forrest Gump"
                          category = "actor"         startYear = 1994
```
