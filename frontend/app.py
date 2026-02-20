#!/usr/bin/env python3
"""
frontend/app.py — Flask GUI for the IMDB MongoDB database.

Run:
    cd frontend
    python app.py
Then open http://localhost:5000
"""

from flask import Flask, render_template, request
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "imdb"

app = Flask(__name__)
db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)[DB_NAME]


def search_actor(name: str) -> list[dict]:
    """
    Return one entry per person whose primaryName matches `name` (case-insensitive).
    Each entry contains the person's info plus a list of their movie credits.
    """
    # Find matching people
    people = list(db.names.find(
        {"primaryName": {"$regex": name.strip(), "$options": "i"}},
        {"_id": 0}
    ).limit(10))

    results = []
    for person in people:
        nconst = person["nconst"]

        # Aggregate credits: principals → titles
        pipeline = [
            {"$match": {"nconst": nconst}},
            {"$lookup": {
                "from": "titles",
                "localField": "tconst",
                "foreignField": "tconst",
                "as": "title",
            }},
            {"$unwind": "$title"},
            {"$project": {
                "_id": 0,
                "category": 1,
                "characters": 1,
                "primaryTitle": "$title.primaryTitle",
                "startYear": "$title.startYear",
                "genres": "$title.genres",
                "tconst": "$title.tconst",
            }},
            {"$sort": {"startYear": -1}},
        ]

        credits = list(db.principals.aggregate(pipeline))

        # Parse characters field (stored as JSON-like string e.g. '["Tony Stark"]')
        for c in credits:
            raw = c.get("characters") or ""
            # Strip surrounding brackets/quotes for display
            c["characters_display"] = (
                raw.strip('[]"').replace('","', ", ") if raw else ""
            )

        results.append({
            "person": person,
            "credits": credits,
        })

    return results


@app.route("/")
def index():
    query = request.args.get("q", "").strip()
    results = []
    error = None

    if query:
        try:
            results = search_actor(query)
        except Exception as exc:
            error = str(exc)

    return render_template("index.html", query=query, results=results, error=error)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
