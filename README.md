# IMDB Database Assignment

## Overview

You will build a small full-stack data application using a real-world dataset from IMDB. The dataset contains information about movies released after 1990, the actors and directors who worked on them, and basic biographical information about those people.

**Teams:** At most two people. Solo is fine.
**Due:** Friday, February 27th, end of day.
**Submission:** All code must be pushed to your team's repository in the class GitHub organization before the deadline.

---

## The Data

You are given three TSV (tab-separated) files:

| File | Contents |
|---|---|
| `title.basics.tsv` | One row per movie — title, year, genre, runtime |
| `title.principals.tsv` | One row per actor/actress/director credit |
| `name.basics.tsv` | One row per person — name, birth year, known-for titles |

Key fields to be aware of:
- `tconst` — unique movie ID (e.g. `tt0114709`)
- `nconst` — unique person ID (e.g. `nm0000001`)
- `category` — role type: `actor`, `actress`, or `director`

The files use `\N` to represent null/missing values.

---

## Getting the Data

The dataset files are provided separately as `.gz` archives. Download them 
[from here](https://www.dropbox.com/scl/fo/kekxhc760et6hh9ybc67l/AGn3DkO_SA26ItidL-cSIHw?rlkey=k3zu1iyn608x7hlwg9yxcsp4h&st=17v4rcs5&dl=0)
and unzip before starting:

```bash
gunzip title.basics.tsv.gz
gunzip title.principals.tsv.gz
gunzip name.basics.tsv.gz
```

> **Do not push `.gz` or `.tsv` files to your git repository.** Both file types are in `.gitignore` — keep it that way. The data files are large and do not belong in version control.

---

## Part 1 — Docker Database

Stand up a database using Docker. Choose **one**:

**Option A — MongoDB**
```bash
docker run -d --name imdb-mongo -p 27017:27017 mongo:latest
```

**Option B — PostgreSQL**
```bash
docker run -d --name imdb-postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD=password -e POSTGRES_DB=imdb postgres:latest
```

Your database must be running in Docker for full credit — do not use a local installation or a cloud service.

Include in your repo a short `docker_setup.md` (or a section in your README) with the exact command(s) needed to reproduce your setup from scratch.

---

## Part 2 — Data Ingestion

Write a Python script (`ingest.py` or similar) that reads the three TSV files and loads them into your database.

Requirements:
- Handle `\N` values gracefully (store as `null` / `NULL`, not as the string `\N`)
- Print progress so the user knows something is happening (row counts, elapsed time, etc.)
- The script should be safe to re-run (i.e., don't duplicate data on a second run)
- Loading the full dataset should complete in a reasonable time — add indexes or bulk-insert as needed

---

## Part 3 — Query Program

Write a Python script (`queries.py` or similar) that runs at least **eight queries** against your database, ranging from simple to complex. Print the results in a readable format.

Work your way up in difficulty. Here are examples to guide you — you do not have to use these exact queries, but your set should cover a similar range:

1. How many movies are in the database?
2. List all movies released in a specific year (e.g. 1994).
3. Find the 10 most prolific actors/actresses by number of movie credits.
4. List all movies a given person appeared in, sorted by year.
5. List all movies directed by a specific director.
6. Find the most common genre in the database.
7. Find all movies that two specific actors both appeared in.
8. *(Your own)* — come up with one interesting question the data can answer and write the query.

---

## Part 4 — GUI Front End

Build a simple graphical front end that lets a user run queries against the database without writing any code. This can be a desktop app or a web app — your choice.

**Minimum requirements:**
- At least one text input or dropdown that the user can use to specify a search
- Results displayed in a table or list
- Connects to the live Docker database

**Suggested approach:** A small web app using Flask or FastAPI (backend) + plain HTML/JS or a framework like Streamlit (if you want something even faster).

Feel free — and encouraged — to use an LLM (ChatGPT, Claude, Copilot, etc.) to help build the front end. The GUI is intentionally open-ended. Some example prompts to get you started:

> *"Write a Flask app with a search box that queries a PostgreSQL database for movies by title and displays the results in an HTML table."*

> *"Build a Streamlit app that connects to MongoDB and lets the user search for an actor by name, then shows all movies they appeared in."*

> *"Generate a simple HTML page with a dropdown for genre and a year range slider that submits a form to a Python backend."*

The front end does not need to be beautiful. It needs to work.

---

## Deliverables Checklist

Before the deadline, make sure your repository contains:

- [ ] `docker_setup.md` — instructions to start your database container
- [ ] `ingest.py` (or equivalent) — loads all three TSV files into the DB
- [ ] `queries.py` (or equivalent) — runs and prints results of 8+ queries
- [ ] Front-end code in its own folder (e.g. `frontend/`)
- [ ] A `requirements.txt` listing all Python dependencies
- [ ] A `README.md` explaining how to run each part

Everything must run on a fresh machine with only Docker and Python installed.

---

## Git & Submission

- Each team gets **one repository** in the class GitHub organization.
- Commit regularly — a repo with a single commit the night before will raise questions.
- Both team members should have commits in the history.
- Name your repo: `imdb-<lastname1>-<lastname2>` (or `imdb-<lastname>` for solo).


---

## Tips

- Start with ingestion — nothing else works until the data is in the database.
- **Add indexes on `tconst` and `nconst` early; queries will be painfully slow without them.** Without an index, every query does a full table scan — reading all 2.7 million rows in `title.principals.tsv` to find matches. An index lets the database jump directly to the relevant rows instead. Create them once, right after ingestion:

  *PostgreSQL:*
  ```sql
  CREATE INDEX ON title_principals(tconst);
  CREATE INDEX ON title_principals(nconst);
  ```
  *MongoDB:*
  ```python
  db.principals.create_index("tconst")
  db.principals.create_index("nconst")
  ```
- The `title.principals.tsv` file is the join table that links movies to people — spend time understanding it.
- LLMs are good at boilerplate (database connections, HTML forms, Docker commands). Use them. Just make sure you understand what you submit.
- If something isn't working the night before it's due, commit what you have and ask for help.
