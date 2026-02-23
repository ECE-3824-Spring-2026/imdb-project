from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient("mongodb://localhost:27017/")
db = client["imdb"]
movies     = db["movies"]
principals = db["principals"]
people     = db["people"]

def get_nconst(name):
    p = people.find_one({"name": name})
    return p["nconst"] if p else None

def get_movies_for_nconst(nconst):
    tconsts = [c["tconst"] for c in principals.find({"nconst": nconst}, {"tconst": 1})]
    return list(movies.find({"tconst": {"$in": tconsts}}, {"title": 1, "year": 1, "genres": 1, "_id": 0}).sort("year", 1))

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search/movies")
def search_movies():
    title = request.args.get("title", "").strip()
    year  = request.args.get("year", "").strip()
    query = {}
    if title:
        query["title"] = {"$regex": title, "$options": "i"}
    if year.isdigit():
        query["year"] = int(year)
    results = list(movies.find(query, {"title": 1, "year": 1, "genres": 1, "_id": 0}).limit(50))
    return render_template("index.html", tab="movies", results=results, title=title, year=year)

@app.route("/search/person")
def search_person():
    name = request.args.get("name", "").strip()
    results, error = [], None
    if name:
        nconst = get_nconst(name)
        if nconst:
            results = get_movies_for_nconst(nconst)
        else:
            error = f"No person found named '{name}'."
    return render_template("index.html", tab="person", results=results, name=name, error=error)

@app.route("/search/director")
def search_director():
    name = request.args.get("name", "").strip()
    results, error = [], None
    if name:
        nconst = get_nconst(name)
        if nconst:
            tconsts = [c["tconst"] for c in principals.find({"nconst": nconst, "category": "director"}, {"tconst": 1})]
            results = list(movies.find({"tconst": {"$in": tconsts}}, {"title": 1, "year": 1, "genres": 1, "_id": 0}).sort("year", 1))
        else:
            error = f"No director found named '{name}'."
    return render_template("index.html", tab="director", results=results, name=name, error=error)

@app.route("/search/shared")
def search_shared():
    name1 = request.args.get("name1", "").strip()
    name2 = request.args.get("name2", "").strip()
    results, error = [], None
    if name1 and name2:
        nc1, nc2 = get_nconst(name1), get_nconst(name2)
        if nc1 and nc2:
            s1 = set(c["tconst"] for c in principals.find({"nconst": nc1}, {"tconst": 1}))
            s2 = set(c["tconst"] for c in principals.find({"nconst": nc2}, {"tconst": 1}))
            shared = list(s1 & s2)
            results = list(movies.find({"tconst": {"$in": shared}}, {"title": 1, "year": 1, "genres": 1, "_id": 0}).sort("year", 1))
        else:
            error = "One or both people not found."
    return render_template("index.html", tab="shared", results=results, name1=name1, name2=name2, error=error)

if __name__ == "__main__":
    app.run(debug=True)