#!/usr/bin/env python3
"""
Monthly discovery pipeline: scrape a handful of Québec cleaning-niche
hashtags for new Instagram accounts, verify each candidate against
Apify, and auto-add the ones that pass quality guardrails to config.json.

Requires APIFY_TOKEN env var. Runs on its own monthly cron — see
.github/workflows/discover.yml — separate from the weekly refresh so a
slow discovery run never delays the Sunday dashboard update.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config.json"

APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
if not APIFY_TOKEN:
    print("ERROR: APIFY_TOKEN env var required")
    sys.exit(1)

HASHTAG_SCRAPER = "apify~instagram-hashtag-scraper"
PROFILE_SCRAPER = "apify~instagram-scraper"

HASHTAGS = [
    "nettoyagequebec", "entretienmenager", "menagequebec",
    "cleaningmontreal", "nettoyagemontreal", "menagemontreal",
]

MIN_FOLLOWERS = 500
MAX_FOLLOWERS = 500_000
MIN_POSTS = 5
MAX_NEW_PER_RUN = 3
MAX_VERIFY_ATTEMPTS = 15  # bound worst-case Apify spend even if nothing qualifies

# Hashtag co-occurrence pulls in unrelated accounts (e.g. a nutritionist
# whose post happened to carry #entretienmenager). Require the bio or
# name to actually mention cleaning before auto-adding.
RELEVANCE_KEYWORDS = [
    "nettoyage", "ménage", "menage", "entretien ménager", "entretien menager",
    "cleaning", "clean", "maid", "housekeeping", "désinfection", "desinfection",
    "conciergerie", "janitorial", "entretien commercial",
]


def apify_post(path, body):
    url = f"https://api.apify.com/v2/{path}?token={APIFY_TOKEN}"
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def apify_get(path, **params):
    qs = urllib.parse.urlencode({**params, "token": APIFY_TOKEN})
    url = f"https://api.apify.com/v2/{path}?{qs}"
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r)


def start_actor(actor_id, body):
    return apify_post(f"acts/{actor_id}/runs", body)["data"]


def wait_for_run(run_id, label="run", interval=8, max_wait=600):
    start = time.time()
    while True:
        r = apify_get(f"actor-runs/{run_id}")["data"]
        status = r["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  [{label}] {status} after {int(time.time()-start)}s")
            return r
        if time.time() - start > max_wait:
            raise RuntimeError(f"{label} run {run_id} did not finish in {max_wait}s")
        time.sleep(interval)


def fetch_dataset(dataset_id):
    return apify_get(f"datasets/{dataset_id}/items", clean="true")


def load_config():
    return json.loads(CONFIG.read_text())


def save_config(config):
    CONFIG.write_text(json.dumps(config, indent=2) + "\n")


def find_hashtag_candidates(known_handles):
    print(f"scraping {len(HASHTAGS)} hashtags for candidate accounts…")
    run = start_actor(HASHTAG_SCRAPER, {
        "hashtags": HASHTAGS,
        "resultsType": "reels",
        "resultsLimit": 30,
    })
    run = wait_for_run(run["id"], "hashtag-scrape")
    if run["status"] != "SUCCEEDED":
        print(f"  ! hashtag scrape did not succeed ({run['status']}), skipping discovery this run")
        return []
    items = fetch_dataset(run["defaultDatasetId"])
    handles = set()
    for it in items:
        h = it.get("ownerUsername") or (it.get("owner") or {}).get("username")
        if h and h.lower() not in known_handles:
            handles.add(h)
    print(f"  found {len(handles)} candidate handle(s) not already tracked")
    return sorted(handles)


def verify_candidate(handle):
    """Fetch profile details and apply quality guardrails. Returns a
    creator dict ready for config.json, or None if it fails the bar."""
    run = start_actor(PROFILE_SCRAPER, {
        "directUrls": [f"https://www.instagram.com/{handle}/"],
        "resultsType": "details",
        "resultsLimit": 1,
    })
    run = wait_for_run(run["id"], f"verify-{handle}", max_wait=120)
    if run["status"] != "SUCCEEDED":
        return None
    items = fetch_dataset(run["defaultDatasetId"])
    if not items or items[0].get("error") or not items[0].get("username"):
        return None
    item = items[0]
    followers = item.get("followersCount") or 0
    posts = item.get("postsCount") or 0
    private = bool(item.get("private"))
    if private or not (MIN_FOLLOWERS <= followers <= MAX_FOLLOWERS) or posts < MIN_POSTS:
        print(f"  @{handle}: rejected (followers={followers}, posts={posts}, private={private})")
        return None
    haystack = f"{item.get('biography') or ''} {item.get('fullName') or ''}".lower()
    if not any(kw in haystack for kw in RELEVANCE_KEYWORDS):
        print(f"  @{handle}: rejected (bio doesn't mention cleaning — likely hashtag coincidence)")
        return None
    print(f"  @{handle}: accepted (followers={followers}, posts={posts})")
    return {
        "handle": item["username"],
        "name": f"{item.get('fullName') or item['username']} — découvert {date.today().isoformat()}",
        "followers": followers,
        "posts": posts,
        "verified": bool(item.get("verified")),
        "private": False,
        "profilePic": "",
    }


def main():
    config = load_config()
    known = {c["handle"].lower() for c in config["creators"]}
    candidates = find_hashtag_candidates(known)

    added = []
    attempts = 0
    for handle in candidates:
        if len(added) >= MAX_NEW_PER_RUN or attempts >= MAX_VERIFY_ATTEMPTS:
            break
        attempts += 1
        creator = verify_candidate(handle)
        if creator:
            config["creators"].append(creator)
            added.append(creator)

    if added:
        save_config(config)
        print(f"\nadded {len(added)} new creator(s): {[c['handle'] for c in added]}")
    else:
        print("\nno new creators added this run")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"added_count={len(added)}\n")
            f.write("added_handles=" + ",".join(c["handle"] for c in added) + "\n")


if __name__ == "__main__":
    main()
