"""
frontend/app.py — Flask web frontend for the IMDB MongoDB database.
Usage: python frontend/app.py
Then open http://localhost:5000
"""

from flask import Flask, render_template_string, request, jsonify
from pymongo import MongoClient

app    = Flask(__name__)
client = MongoClient('localhost', 27017)
db     = client['imdb']

# ── HTML Template ─────────────────────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IMDB Explorer</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #111; color: #eee; min-height: 100vh; }
  header { background: #e8b000; color: #111; padding: 18px 32px; display: flex; align-items: center; gap: 16px; }
  header h1 { font-size: 1.6rem; font-weight: 800; letter-spacing: 1px; }
  header span { font-size: 0.95rem; opacity: 0.7; }
  .container { max-width: 1100px; margin: 32px auto; padding: 0 24px; }
  .tabs { display: flex; gap: 8px; margin-bottom: 24px; flex-wrap: wrap; }
  .tab-btn { background: #222; border: 1px solid #444; color: #ccc; padding: 8px 18px; border-radius: 6px;
             cursor: pointer; font-size: 0.9rem; transition: all .2s; }
  .tab-btn.active, .tab-btn:hover { background: #e8b000; color: #111; border-color: #e8b000; font-weight: 600; }
  .panel { display: none; background: #1a1a1a; border: 1px solid #333; border-radius: 10px; padding: 24px; }
  .panel.active { display: block; }
  .panel h2 { font-size: 1.1rem; margin-bottom: 16px; color: #e8b000; }
  .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: flex-end; margin-bottom: 16px; }
  label { font-size: 0.85rem; color: #aaa; display: block; margin-bottom: 4px; }
  input, select { background: #2a2a2a; border: 1px solid #444; color: #eee; padding: 9px 13px;
                  border-radius: 6px; font-size: 0.95rem; min-width: 220px; }
  input:focus, select:focus { outline: none; border-color: #e8b000; }
  button.go { background: #e8b000; color: #111; border: none; padding: 9px 22px; border-radius: 6px;
              font-weight: 700; cursor: pointer; font-size: 0.95rem; }
  button.go:hover { background: #ffd000; }
  .results { margin-top: 20px; }
  .count { font-size: 0.85rem; color: #888; margin-bottom: 10px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
  th { background: #2a2a2a; color: #e8b000; text-align: left; padding: 9px 12px; border-bottom: 2px solid #444; }
  td { padding: 8px 12px; border-bottom: 1px solid #2a2a2a; }
  tr:hover td { background: #222; }
  .tag { display: inline-block; background: #2a2a2a; border: 1px solid #444; border-radius: 4px;
         padding: 2px 7px; font-size: 0.78rem; margin: 1px; color: #ccc; }
  .loading { color: #888; font-style: italic; padding: 20px 0; }
  .error { color: #e05; padding: 12px; background: #2a0008; border-radius: 6px; }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-top: 8px; }
  .stat-card { background: #222; border: 1px solid #333; border-radius: 8px; padding: 20px; text-align: center; }
  .stat-card .num { font-size: 2rem; font-weight: 800; color: #e8b000; }
  .stat-card .lbl { font-size: 0.85rem; color: #888; margin-top: 4px; }
</style>
</head>
<body>
<header>
  <div>
    <h1>🎬 IMDB Explorer</h1>
    <span>ECE-3824 Spring 2026 · MongoDB Edition</span>
  </div>
</header>

<div class="container">
  <div class="tabs">
    <button class="tab-btn active" onclick="showTab('stats')">📊 Stats</button>
    <button class="tab-btn" onclick="showTab('search')">🔍 Search Movies</button>
    <button class="tab-btn" onclick="showTab('actor')">🎭 Actor Filmography</button>
    <button class="tab-btn" onclick="showTab('director')">🎬 Director Films</button>
    <button class="tab-btn" onclick="showTab('collab')">🤝 Collaborations</button>
    <button class="tab-btn" onclick="showTab('genres')">📈 Genre Stats</button>
  </div>

  <!-- STATS -->
  <div id="tab-stats" class="panel active">
    <h2>Database Overview</h2>
    <div id="stats-content" class="loading">Loading stats…</div>
  </div>

  <!-- SEARCH MOVIES -->
  <div id="tab-search" class="panel">
    <h2>Search Movies by Title and/or Year</h2>
    <div class="row">
      <div><label>Title contains</label><input id="s-title" placeholder="e.g. Dark Knight" /></div>
      <div><label>Year</label><input id="s-year" type="number" placeholder="e.g. 2008" style="min-width:100px"/></div>
      <div><label>Genre</label>
        <select id="s-genre">
          <option value="">Any genre</option>
          <option>Action</option><option>Comedy</option><option>Drama</option><option>Horror</option>
          <option>Romance</option><option>Thriller</option><option>Sci-Fi</option><option>Animation</option>
          <option>Adventure</option><option>Crime</option><option>Documentary</option><option>Fantasy</option>
        </select>
      </div>
      <button class="go" onclick="searchMovies()">Search</button>
    </div>
    <div id="search-results" class="results"></div>
  </div>

  <!-- ACTOR -->
  <div id="tab-actor" class="panel">
    <h2>Actor / Actress Filmography</h2>
    <div class="row">
      <div><label>Actor name</label><input id="actor-name" placeholder="e.g. Tom Hanks" /></div>
      <button class="go" onclick="actorFilms()">Find Films</button>
    </div>
    <div id="actor-results" class="results"></div>
  </div>

  <!-- DIRECTOR -->
  <div id="tab-director" class="panel">
    <h2>Director Filmography</h2>
    <div class="row">
      <div><label>Director name</label><input id="director-name" placeholder="e.g. Christopher Nolan" /></div>
      <button class="go" onclick="directorFilms()">Find Films</button>
    </div>
    <div id="director-results" class="results"></div>
  </div>

  <!-- COLLABORATIONS -->
  <div id="tab-collab" class="panel">
    <h2>Movies Two People Appeared In Together</h2>
    <div class="row">
      <div><label>Person A</label><input id="collab-a" placeholder="e.g. Matt Damon" /></div>
      <div><label>Person B</label><input id="collab-b" placeholder="e.g. Ben Affleck" /></div>
      <button class="go" onclick="collaborations()">Find Shared Movies</button>
    </div>
    <div id="collab-results" class="results"></div>
  </div>

  <!-- GENRE STATS -->
  <div id="tab-genres" class="panel">
    <h2>Genre Statistics</h2>
    <button class="go" onclick="genreStats()" style="margin-bottom:16px">Load Stats</button>
    <div id="genre-results" class="results"></div>
  </div>
</div>

<script>
function showTab(name) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
  if (name === 'stats') loadStats();
}

function renderTable(data, cols) {
  if (!data.length) return '<p style="color:#888">No results found.</p>';
  let html = `<table><tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr>`;
  data.forEach(row => {
    html += '<tr>' + cols.map(c => {
      let v = row[c];
      if (Array.isArray(v)) v = v.map(g=>`<span class="tag">${g}</span>`).join('');
      return `<td>${v ?? '—'}</td>`;
    }).join('') + '</tr>';
  });
  return html + '</table>';
}

async function api(endpoint, params={}) {
  const qs = new URLSearchParams(params).toString();
  const r  = await fetch(`/api/${endpoint}?${qs}`);
  return r.json();
}

async function loadStats() {
  const el = document.getElementById('stats-content');
  el.innerHTML = '<p class="loading">Loading…</p>';
  const d = await api('stats');
  el.innerHTML = `<div class="stat-grid">
    <div class="stat-card"><div class="num">${d.movies.toLocaleString()}</div><div class="lbl">Movies</div></div>
    <div class="stat-card"><div class="num">${d.principals.toLocaleString()}</div><div class="lbl">Credits</div></div>
    <div class="stat-card"><div class="num">${d.people.toLocaleString()}</div><div class="lbl">People</div></div>
    <div class="stat-card"><div class="num">${d.year_range}</div><div class="lbl">Year Range</div></div>
  </div>`;
}

async function searchMovies() {
  const el = document.getElementById('search-results');
  el.innerHTML = '<p class="loading">Searching…</p>';
  const d = await api('search', {
    title: document.getElementById('s-title').value,
    year:  document.getElementById('s-year').value,
    genre: document.getElementById('s-genre').value,
  });
  el.innerHTML = `<p class="count">${d.count.toLocaleString()} result(s) — showing first 50</p>` +
    renderTable(d.results, ['Title', 'Year', 'Genres', 'Runtime']);
}

async function actorFilms() {
  const el = document.getElementById('actor-results');
  el.innerHTML = '<p class="loading">Searching…</p>';
  const d = await api('actor', {name: document.getElementById('actor-name').value});
  if (d.error) { el.innerHTML = `<p class="error">${d.error}</p>`; return; }
  el.innerHTML = `<p class="count">${d.count} films for <strong>${d.name}</strong></p>` +
    renderTable(d.results, ['Year', 'Title', 'Genres']);
}

async function directorFilms() {
  const el = document.getElementById('director-results');
  el.innerHTML = '<p class="loading">Searching…</p>';
  const d = await api('director', {name: document.getElementById('director-name').value});
  if (d.error) { el.innerHTML = `<p class="error">${d.error}</p>`; return; }
  el.innerHTML = `<p class="count">${d.count} films directed by <strong>${d.name}</strong></p>` +
    renderTable(d.results, ['Year', 'Title', 'Genres']);
}

async function collaborations() {
  const el = document.getElementById('collab-results');
  el.innerHTML = '<p class="loading">Searching…</p>';
  const d = await api('collab', {
    a: document.getElementById('collab-a').value,
    b: document.getElementById('collab-b').value,
  });
  if (d.error) { el.innerHTML = `<p class="error">${d.error}</p>`; return; }
  el.innerHTML = `<p class="count">${d.count} shared films between <strong>${d.name_a}</strong> and <strong>${d.name_b}</strong></p>` +
    renderTable(d.results, ['Year', 'Title', 'Genres']);
}

async function genreStats() {
  const el = document.getElementById('genre-results');
  el.innerHTML = '<p class="loading">Loading…</p>';
  const d = await api('genres');
  el.innerHTML = renderTable(d.results, ['Genre', 'Movie Count', 'Avg Runtime (min)']);
}

// Auto-load stats on page load
loadStats();

// Allow Enter key to trigger search
document.addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    const active = document.querySelector('.panel.active').id;
    if (active === 'tab-search')    searchMovies();
    if (active === 'tab-actor')     actorFilms();
    if (active === 'tab-director')  directorFilms();
    if (active === 'tab-collab')    collaborations();
  }
});
</script>
</body>
</html>
"""

# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/stats")
def api_stats():
    yr = list(db.movies.aggregate([
        {"$group": {"_id": None,
                    "min": {"$min": "$year"},
                    "max": {"$max": "$year"}}}
    ]))
    year_range = f"{yr[0]['min']}–{yr[0]['max']}" if yr else "N/A"
    return jsonify({
        "movies":     db.movies.count_documents({}),
        "principals": db.principals.count_documents({}),
        "people":     db.people.count_documents({}),
        "year_range": year_range,
    })


@app.route("/api/search")
def api_search():
    query = {}
    title = request.args.get("title", "").strip()
    year  = request.args.get("year",  "").strip()
    genre = request.args.get("genre", "").strip()

    if title:
        query["title"] = {"$regex": title, "$options": "i"}
    if year.isdigit():
        query["year"] = int(year)
    if genre:
        query["genres"] = genre

    total   = db.movies.count_documents(query)
    results = list(db.movies.find(query, {"title": 1, "year": 1, "genres": 1, "runtime_min": 1})
                             .sort("year", -1).limit(50))
    return jsonify({
        "count":   total,
        "results": [{"Title": r["title"], "Year": r["year"],
                     "Genres": r.get("genres") or [],
                     "Runtime": r.get("runtime_min")} for r in results],
    })


def _films_for_person(name: str, category_filter=None):
    person = db.people.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
    if not person:
        return None, None, []
    q = {"nconst": person["_id"]}
    if category_filter:
        q["category"] = {"$in": category_filter}
    tconsts = [p["tconst"] for p in db.principals.find(q)]
    movies  = list(db.movies.find({"_id": {"$in": tconsts}},
                                  {"title": 1, "year": 1, "genres": 1}).sort("year", 1))
    return person["_id"], person["name"], movies


@app.route("/api/actor")
def api_actor():
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({"error": "Please enter a name."})
    _, display, movies = _films_for_person(name, ["actor", "actress"])
    if display is None:
        return jsonify({"error": f"Person '{name}' not found."})
    return jsonify({
        "name":    display,
        "count":   len(movies),
        "results": [{"Year": m["year"], "Title": m["title"],
                     "Genres": m.get("genres") or []} for m in movies],
    })


@app.route("/api/director")
def api_director():
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({"error": "Please enter a name."})
    _, display, movies = _films_for_person(name, ["director"])
    if display is None:
        return jsonify({"error": f"Director '{name}' not found."})
    return jsonify({
        "name":    display,
        "count":   len(movies),
        "results": [{"Year": m["year"], "Title": m["title"],
                     "Genres": m.get("genres") or []} for m in movies],
    })


@app.route("/api/collab")
def api_collab():
    a = request.args.get("a", "").strip()
    b = request.args.get("b", "").strip()
    if not a or not b:
        return jsonify({"error": "Please enter two names."})

    def tconsts(name):
        p = db.people.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
        if not p:
            return None, None, set()
        return p["_id"], p["name"], set(doc["tconst"] for doc in db.principals.find({"nconst": p["_id"]}))

    _, name_a, set_a = tconsts(a)
    _, name_b, set_b = tconsts(b)

    if name_a is None:
        return jsonify({"error": f"Person '{a}' not found."})
    if name_b is None:
        return jsonify({"error": f"Person '{b}' not found."})

    shared = set_a & set_b
    movies = list(db.movies.find({"_id": {"$in": list(shared)}},
                                 {"title": 1, "year": 1, "genres": 1}).sort("year", 1))
    return jsonify({
        "name_a":  name_a,
        "name_b":  name_b,
        "count":   len(movies),
        "results": [{"Year": m["year"], "Title": m["title"],
                     "Genres": m.get("genres") or []} for m in movies],
    })


@app.route("/api/genres")
def api_genres():
    pipeline = [
        {"$unwind": "$genres"},
        {"$match":  {"genres": {"$ne": None}}},
        {"$group":  {"_id": "$genres",
                     "count":       {"$sum": 1},
                     "avg_runtime": {"$avg": "$runtime_min"}}},
        {"$sort":   {"count": -1}},
        {"$project": {"_id": 0,
                      "Genre":            "$_id",
                      "Movie Count":      "$count",
                      "Avg Runtime (min)": {"$round": ["$avg_runtime", 1]}}},
    ]
    rows = list(db.movies.aggregate(pipeline))
    return jsonify({"results": rows})


if __name__ == "__main__":
    print("Starting IMDB Flask app on http://localhost:5000")
    app.run(debug=True, port=5000)
