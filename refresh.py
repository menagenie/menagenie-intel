#!/usr/bin/env python3
"""
Weekly refresh pipeline. Idempotent:
  1. Call Apify reel-scraper for all CREATORS (transcripts on)
  2. Poll until done
  3. Pull dataset, dedupe against existing data (don't re-transcribe known reels)
  4. Refresh each creator's profile details
  5. Extract local 3-sec hook clips for new reels (ffmpeg)
  6. Prune reels + clips older than 28 days
  7. Build the dashboard HTML

Requires APIFY_TOKEN env var. Designed to run in GitHub Actions weekly.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import analyze

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CLIPS = ROOT / "clips"
THUMBS = ROOT / "thumbs"

# Load CREATORS list straight from build.py (single source of truth)
import importlib.util
spec = importlib.util.spec_from_file_location("build_module", ROOT / "build.py")
build_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_module)
CREATORS = build_module.CREATORS

APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
if not APIFY_TOKEN:
    print("ERROR: APIFY_TOKEN env var required")
    sys.exit(1)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
WINDOW_DAYS = 28
CLIP_SECONDS = 3
REEL_SCRAPER = "apify~instagram-reel-scraper"
PROFILE_SCRAPER = "apify~instagram-scraper"


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
    # Retry with exponential backoff on transient 5xx errors (Apify's API
    # occasionally returns 502/503/504 under load).
    delays = [2, 5, 10, 20, 40]
    last_err = None
    for attempt, delay in enumerate([0] + delays):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                print(f"  ! apify_get {path} → HTTP {e.code}, retrying (attempt {attempt+1}/{len(delays)+1})")
                continue
            raise
        except urllib.error.URLError as e:
            last_err = e
            print(f"  ! apify_get {path} → URLError {e}, retrying (attempt {attempt+1}/{len(delays)+1})")
            continue
    raise RuntimeError(f"apify_get failed after retries: {last_err}") from last_err


def start_actor(actor_id, body):
    return apify_post(f"acts/{actor_id}/runs", body)["data"]


def wait_for_run(run_id, label="run", interval=8, max_wait=900):
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


def fetch_dataset(dataset_id, fields=None):
    params = {"clean": "true"}
    if fields:
        params["fields"] = ",".join(fields)
    return apify_get(f"datasets/{dataset_id}/items", **params)


def load_existing_posts(handle):
    p = DATA / f"{handle}_posts.json"
    if not p.exists():
        return []
    try:
        return json.load(open(p))["items"]
    except (json.JSONDecodeError, KeyError):
        return []


def save_posts(handle, items):
    DATA.mkdir(parents=True, exist_ok=True)
    with open(DATA / f"{handle}_posts.json", "w") as f:
        json.dump({"items": items}, f)


def save_details(handle, details):
    DATA.mkdir(parents=True, exist_ok=True)
    wrapper = {"dataset": {"previewItems": [details]}}
    with open(DATA / f"{handle}_details.json", "w") as f:
        json.dump(wrapper, f)


def ts_to_dt(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _apply_history(old, merged, today_str):
    """Track a rolling history of weekly snapshots on a post so the
    dashboard can tell whether a reel is still gaining traction, not
    just its current point-in-time count. Keeps the last 5 snapshots —
    enough to cover the 28-day window at a weekly cadence."""
    history = list(old.get("history") or [])
    plays = merged.get("videoPlayCount") or merged.get("videoViewCount") or 0
    if not history or history[-1]["date"] != today_str:
        history.append({
            "date": today_str,
            "plays": plays,
            "likes": merged.get("likesCount") or 0,
            "comments": merged.get("commentsCount") or 0,
        })
    merged["history"] = history[-5:]
    prev_plays = history[-2]["plays"] if len(history) >= 2 else None
    merged["still_climbing"] = bool(prev_plays and plays > prev_plays * 1.5)
    return merged


REEL_FIELDS = [
    "shortCode", "url", "caption", "timestamp", "videoDuration",
    "videoPlayCount", "videoViewCount", "likesCount", "commentsCount",
    "displayUrl", "videoUrl", "musicInfo", "transcript",
    "ownerUsername", "ownerFullName",
]

TIKTOK_SCRAPER = "clockworks~tiktok-scraper"


def _load_tiktok_creators():
    """Map of {ig_handle: tiktok_handle} for the targeted subset of
    creators that have a verified 'tiktok' field in config.json."""
    config_path = ROOT / "config.json"
    data = json.loads(config_path.read_text())
    return {
        c["handle"]: c["tiktok"]
        for c in data.get("creators", [])
        if c.get("tiktok")
    }


TIKTOK_CREATORS = _load_tiktok_creators()


def load_existing_tiktok_posts(tt_handle):
    p = DATA / f"{tt_handle}_tiktok_posts.json"
    if not p.exists():
        return []
    try:
        return json.load(open(p))["items"]
    except (json.JSONDecodeError, KeyError):
        return []


def save_tiktok_posts(tt_handle, items):
    DATA.mkdir(parents=True, exist_ok=True)
    with open(DATA / f"{tt_handle}_tiktok_posts.json", "w") as f:
        json.dump({"items": items}, f)


def save_tiktok_details(tt_handle, details):
    DATA.mkdir(parents=True, exist_ok=True)
    wrapper = {"dataset": {"previewItems": [details]}}
    with open(DATA / f"{tt_handle}_tiktok_details.json", "w") as f:
        json.dump(wrapper, f)


def _tt_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _normalize_tiktok_item(item):
    author = item.get("authorMeta") or {}
    video_meta = item.get("videoMeta") or {}
    return {
        "shortCode": item.get("id"),
        "url": item.get("webVideoUrl"),
        "caption": item.get("text") or "",
        "timestamp": item.get("createTimeISO"),
        "videoDuration": video_meta.get("duration") or 0,
        "videoPlayCount": _tt_int(item.get("playCount")),
        "likesCount": _tt_int(item.get("diggCount")),
        "commentsCount": _tt_int(item.get("commentCount")),
        "displayUrl": video_meta.get("coverUrl"),
        "ownerUsername": author.get("name"),
        "ownerFullName": author.get("nickName"),
        "platform": "tiktok",
    }


def pass1_tiktok_stats_refresh():
    """Cheap: scrape last 28 days for the targeted TikTok subset WITHOUT
    transcripts. Also captures profile details for free — each video
    item already carries full authorMeta when scraped by profile."""
    if not TIKTOK_CREATORS:
        return []
    handles = list(TIKTOK_CREATORS.values())
    print(f"PASS 1 (TikTok stats only) for {len(handles)} creators…")
    run = start_actor(TIKTOK_SCRAPER, {
        "profiles": handles,
        "resultsPerPage": 30,
        "profileSorting": "latest",
        "oldestPostDateUnified": f"{WINDOW_DAYS} days",
        "excludePinnedPosts": True,
        "downloadSubtitlesOptions": "NEVER_DOWNLOAD_SUBTITLES",
    })
    run = wait_for_run(run["id"], "tiktok-pass1-stats")
    if run["status"] != "SUCCEEDED":
        print(f"  ! TikTok pass1 did not succeed ({run['status']}), skipping TikTok this run")
        return []
    items = fetch_dataset(run["defaultDatasetId"])
    print(f"  scraped {len(items)} TikTok videos (stats only)")

    seen = set()
    for it in items:
        author = it.get("authorMeta") or {}
        tt_handle = author.get("name")
        if tt_handle and tt_handle not in seen:
            seen.add(tt_handle)
            save_tiktok_details(tt_handle, {
                "username": author.get("name"),
                "fullName": author.get("nickName") or "",
                "followersCount": author.get("fans") or 0,
                "postsCount": author.get("video") or 0,
                "verified": bool(author.get("verified")),
                "private": bool(author.get("privateAccount")),
                "profilePicUrlHD": author.get("avatar") or "",
            })
    if seen:
        print(f"  refreshed {len(seen)} TikTok profile record(s)")

    return [_normalize_tiktok_item(it) for it in items if it.get("id")]


def _find_new_tiktok_videos(scraped_items):
    existing_with_transcripts = set()
    for tt_handle in TIKTOK_CREATORS.values():
        for p in load_existing_tiktok_posts(tt_handle):
            if p.get("transcript") and p.get("shortCode"):
                existing_with_transcripts.add(p["shortCode"])
    new_urls = []
    for it in scraped_items:
        sc = it.get("shortCode")
        url = it.get("url")
        if sc and url and sc not in existing_with_transcripts:
            new_urls.append(url)
    return new_urls


def merge_tiktok_pass1(scraped_items):
    known_handles = set(TIKTOK_CREATORS.values())
    grouped = {}
    for it in scraped_items:
        u = it.get("ownerUsername")
        # Collab posts sometimes get attributed to the co-author's handle
        # instead of the tracked creator's — drop those instead of
        # spawning a data file for an account we never asked to track.
        if not u or u not in known_handles:
            continue
        grouped.setdefault(u, []).append(it)

    cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for tt_handle, fresh in grouped.items():
        existing = load_existing_tiktok_posts(tt_handle)
        by_sc = {p["shortCode"]: p for p in existing if p.get("shortCode")}
        new_count = 0
        for p in fresh:
            sc = p.get("shortCode")
            if not sc:
                continue
            old = by_sc.get(sc, {})
            merged = {**old, **p}
            if old.get("transcript") and not p.get("transcript"):
                merged["transcript"] = old["transcript"]
            merged = _apply_history(old, merged, today_str)
            if sc not in by_sc:
                new_count += 1
            by_sc[sc] = merged
        kept = [
            p for p in by_sc.values()
            if (dt := ts_to_dt(p.get("timestamp"))) and dt >= cutoff
        ]
        kept.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
        save_tiktok_posts(tt_handle, kept)
        print(f"  @{tt_handle} (TikTok): kept {len(kept)} videos in {WINDOW_DAYS}-day window ({new_count} new)")


def _fetch_transcript_text(url):
    if not url:
        return None
    # Apify key-value-store records are private by default — need the
    # token as a query param, same as every other Apify API call here.
    sep = "&" if "?" in url else "?"
    try:
        req = urllib.request.Request(f"{url}{sep}token={APIFY_TOKEN}", headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="ignore").strip() or None
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        print(f"  ! transcript fetch failed ({url}): {e}")
        return None


def pass2_tiktok_transcripts_for_new(new_urls):
    """Expensive: only for TikTok videos we don't already have transcripts
    for. transcriptionLink points at a text file in an Apify key-value
    store — fetch it separately to get the actual transcript text.
    Also requests a downloadable copy of the video (mediaUrls) so
    extract_all_clips() has something to cut a local hook clip from —
    without this, the dashboard modal falls back to an embed, and
    TikTok has no equivalent to Instagram's public post-embed iframe."""
    if not new_urls:
        print("PASS 2 (TikTok transcripts): no new videos — skipping ✓")
        return {}
    print(f"PASS 2 (TikTok transcripts) for {len(new_urls)} NEW videos…")
    run = start_actor(TIKTOK_SCRAPER, {
        "postURLs": new_urls,
        "downloadSubtitlesOptions": "DOWNLOAD_AND_TRANSCRIBE_VIDEOS_WITHOUT_SUBTITLES",
        "shouldDownloadVideos": True,
    })
    run = wait_for_run(run["id"], "tiktok-pass2-transcripts", max_wait=1800)
    if run["status"] != "SUCCEEDED":
        print(f"  ! TikTok pass2 did not succeed ({run['status']})")
        return {}
    items = fetch_dataset(run["defaultDatasetId"])
    out = {}
    for it in items:
        sc = it.get("id")
        if not sc:
            continue
        entry = {}
        link = (it.get("videoMeta") or {}).get("transcriptionLink")
        if link:
            text = _fetch_transcript_text(link)
            if text:
                entry["transcript"] = text
        media_urls = it.get("mediaUrls") or []
        if media_urls:
            entry["videoUrl"] = media_urls[0]
        if entry:
            out[sc] = entry
    print(f"  got data for {len(out)} new TikTok video(s)")
    return out


def merge_tiktok_pass2_transcripts(data_by_id):
    if not data_by_id:
        return
    for tt_handle in TIKTOK_CREATORS.values():
        existing = load_existing_tiktok_posts(tt_handle)
        updated = 0
        for p in existing:
            sc = p.get("shortCode")
            if sc in data_by_id:
                p.update(data_by_id[sc])
                updated += 1
        if updated:
            save_tiktok_posts(tt_handle, existing)
            print(f"  @{tt_handle} (TikTok): updated {updated} video(s)")


def pass1_stats_refresh():
    """Cheap: scrape last 28 days for all creators WITHOUT transcripts.
    Gives us current plays/likes/comments + identifies which reels are new."""
    print(f"PASS 1 (stats only, no transcripts) for {len(CREATORS)} creators…")
    run = start_actor(REEL_SCRAPER, {
        "username": list(CREATORS),
        "resultsLimit": 50,
        "onlyPostsNewerThan": f"{WINDOW_DAYS} days",
        "includeTranscript": False,  # cheap pass
        "skipPinnedPosts": True,
    })
    run = wait_for_run(run["id"], "pass1-stats")
    if run["status"] != "SUCCEEDED":
        raise RuntimeError(f"pass1 scrape failed: {run['status']}")
    items = fetch_dataset(run["defaultDatasetId"], fields=REEL_FIELDS)
    print(f"  scraped {len(items)} reels (stats only)")
    return items


def pass2_transcripts_for_new(new_reel_urls):
    """Expensive: only for reels we don't already have transcripts for.
    Uses direct reel URLs as input to avoid re-scanning the entire 28-day window."""
    if not new_reel_urls:
        print("PASS 2 (transcripts): no new reels — skipping ✓")
        return []
    print(f"PASS 2 (transcripts) for {len(new_reel_urls)} NEW reels…")
    run = start_actor(REEL_SCRAPER, {
        "username": new_reel_urls,  # field accepts direct reel URLs
        "resultsLimit": 1,
        "includeTranscript": True,
        "skipPinnedPosts": False,  # already specific URLs
    })
    run = wait_for_run(run["id"], "pass2-transcripts", max_wait=1800)
    if run["status"] != "SUCCEEDED":
        raise RuntimeError(f"pass2 scrape failed: {run['status']}")
    items = fetch_dataset(run["defaultDatasetId"], fields=REEL_FIELDS)
    print(f"  got transcripts for {len(items)} new reels")
    return items


def fetch_profile_details_for_all_creators():
    print(f"refreshing profile details for {len(CREATORS)} creators…")
    run = start_actor(PROFILE_SCRAPER, {
        "directUrls": [f"https://www.instagram.com/{h}/" for h in CREATORS],
        "resultsType": "details",
        "resultsLimit": 1,
    })
    run = wait_for_run(run["id"], "details")
    items = fetch_dataset(run["defaultDatasetId"])
    by_handle = {it.get("username"): it for it in items if it.get("username")}
    for handle in CREATORS:
        if handle in by_handle:
            save_details(handle, by_handle[handle])
    print(f"  refreshed {len(by_handle)} profile records")


def _find_new_reels(scraped_items):
    """Return list of reel URLs for reels we've never transcribed before."""
    existing_with_transcripts = set()
    for handle in CREATORS:
        for p in load_existing_posts(handle):
            if p.get("transcript") and p.get("shortCode"):
                existing_with_transcripts.add(p["shortCode"])
    new_urls = []
    for it in scraped_items:
        sc = it.get("shortCode")
        url = it.get("url")
        if sc and url and sc not in existing_with_transcripts:
            new_urls.append(url)
    return new_urls


def merge_pass1_stats(scraped_items):
    """Merge pass-1 (stats-only) results into existing per-creator JSON.
    Preserves transcripts from existing records; updates plays/likes/comments
    from the fresh scrape."""
    known_handles = set(CREATORS)
    grouped = {}
    for it in scraped_items:
        u = it.get("ownerUsername")
        # Collab posts sometimes get attributed to the co-author's handle
        # instead of the tracked creator's — drop those instead of
        # spawning a data file for an account we never asked to track.
        if not u or u not in known_handles:
            continue
        grouped.setdefault(u, []).append(it)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for handle, fresh in grouped.items():
        existing = load_existing_posts(handle)
        by_sc = {p["shortCode"]: p for p in existing if p.get("shortCode")}
        new_count = 0
        for p in fresh:
            sc = p.get("shortCode")
            if not sc:
                continue
            old = by_sc.get(sc, {})
            merged = {**old, **p}  # new stats overwrite
            # PRESERVE transcript if we already had it (pass 1 has no transcript)
            if old.get("transcript") and not p.get("transcript"):
                merged["transcript"] = old["transcript"]
            merged = _apply_history(old, merged, today_str)
            if sc not in by_sc:
                new_count += 1
            by_sc[sc] = merged
        cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
        kept = [
            p for p in by_sc.values()
            if (dt := ts_to_dt(p.get("timestamp"))) and dt >= cutoff
        ]
        kept.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
        save_posts(handle, kept)
        print(f"  @{handle}: kept {len(kept)} reels in {WINDOW_DAYS}-day window ({new_count} new)")


def merge_pass2_transcripts(transcribed_items):
    """Layer transcripts from pass 2 onto whatever's currently in per-creator JSON."""
    by_sc = {it["shortCode"]: it for it in transcribed_items if it.get("shortCode")}
    if not by_sc:
        return
    for handle in CREATORS:
        existing = load_existing_posts(handle)
        updated = 0
        for p in existing:
            sc = p.get("shortCode")
            if sc in by_sc and by_sc[sc].get("transcript"):
                p["transcript"] = by_sc[sc]["transcript"]
                # also pick up videoUrl in case pass 1's URL expired
                if by_sc[sc].get("videoUrl"):
                    p["videoUrl"] = by_sc[sc]["videoUrl"]
                updated += 1
        if updated:
            save_posts(handle, existing)
            print(f"  @{handle}: added transcripts to {updated} reels")


def fetch_image(args):
    url, dest = args
    if dest.exists() and dest.stat().st_size > 0:
        return True
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            dest.write_bytes(r.read())
        return True
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        print(f"  ! image fetch failed ({dest.name}): {e}")
        return False


def download_thumbnails():
    THUMBS.mkdir(parents=True, exist_ok=True)
    jobs = []
    for handle in CREATORS:
        d = DATA / f"{handle}_details.json"
        if d.exists():
            try:
                details = json.load(open(d))["dataset"]["previewItems"][0]
                purl = details.get("profilePicUrlHD") or details.get("profilePicUrl")
                if purl:
                    jobs.append((purl, THUMBS / f"_profile_{handle}.jpg"))
            except (KeyError, IndexError):
                pass
        for p in load_existing_posts(handle):
            sc = p.get("shortCode")
            url = p.get("displayUrl")
            if sc and url:
                jobs.append((url, THUMBS / f"{sc}.jpg"))
    for tt_handle in TIKTOK_CREATORS.values():
        for p in load_existing_tiktok_posts(tt_handle):
            sc = p.get("shortCode")
            url = p.get("displayUrl")
            if sc and url:
                jobs.append((url, THUMBS / f"{sc}.jpg"))
    pending = [(u, d) for (u, d) in jobs if not (d.exists() and d.stat().st_size > 0)]
    if pending:
        print(f"downloading {len(pending)} thumbnails…")
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(fetch_image, pending))


def extract_clip(post):
    sc = post.get("shortCode")
    url = post.get("videoUrl")
    if not sc or not url:
        return (sc or "?", "skip")
    out = CLIPS / f"{sc}.mp4"
    if out.exists() and out.stat().st_size > 0:
        return (sc, "have")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-user_agent", UA,
        "-i", url,
        "-t", str(CLIP_SECONDS),
        "-vf", "scale=-2:480",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "30",
        "-an", "-movflags", "+faststart",
        str(out),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=60)
        if r.returncode == 0 and out.exists() and out.stat().st_size > 0:
            return (sc, "ok")
        else:
            if out.exists():
                out.unlink()
            return (sc, "fail")
    except subprocess.TimeoutExpired:
        return (sc, "timeout")


def extract_all_clips():
    CLIPS.mkdir(parents=True, exist_ok=True)
    all_posts = []
    for handle in CREATORS:
        all_posts.extend(load_existing_posts(handle))
    for tt_handle in TIKTOK_CREATORS.values():
        all_posts.extend(load_existing_tiktok_posts(tt_handle))
    print(f"checking clips for {len(all_posts)} reels…")
    counts = {"ok": 0, "have": 0, "fail": 0, "skip": 0, "timeout": 0}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sc, status in ex.map(extract_clip, all_posts):
            counts[status] = counts.get(status, 0) + 1
    print(f"  clips: {counts}")


def prune_old_assets():
    """Delete thumbs/clips for reels no longer in our 28-day window."""
    keep = set()
    for handle in CREATORS:
        for p in load_existing_posts(handle):
            sc = p.get("shortCode")
            if sc:
                keep.add(sc)
    for tt_handle in TIKTOK_CREATORS.values():
        for p in load_existing_tiktok_posts(tt_handle):
            sc = p.get("shortCode")
            if sc:
                keep.add(sc)
    pruned = 0
    for f in CLIPS.glob("*.mp4"):
        if f.stem not in keep:
            f.unlink(); pruned += 1
    for f in THUMBS.glob("*.jpg"):
        if f.stem.startswith("_profile_"):
            continue
        if f.stem not in keep:
            f.unlink(); pruned += 1
    if pruned:
        print(f"  pruned {pruned} aged-out clip/thumb files")


def build_dashboard():
    print("running build.py …")
    r = subprocess.run([sys.executable, str(ROOT / "build.py")], capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout); print(r.stderr)
        raise RuntimeError("build.py failed")
    print(r.stdout.strip())


def classify_all_relevance():
    """Cheap: tag every post/video with is_relevant (True/False) so
    build.py can hide content that has nothing to do with cleaning —
    multi-topic lifestyle influencers post plenty of that even though
    the account itself was added as a cleaning-niche creator. Judged
    from caption + transcript combined, so it also covers posts with no
    transcript at all. No-ops per-item if ANTHROPIC_API_KEY isn't set."""
    tagged = 0
    for handle in CREATORS:
        posts = load_existing_posts(handle)
        changed = False
        for p in posts:
            if "is_relevant" not in p:
                p["is_relevant"] = analyze.is_relevant(p.get("caption"), p.get("transcript"))
                tagged += 1
                changed = True
        if changed:
            save_posts(handle, posts)
    for tt_handle in TIKTOK_CREATORS.values():
        posts = load_existing_tiktok_posts(tt_handle)
        changed = False
        for p in posts:
            if "is_relevant" not in p:
                p["is_relevant"] = analyze.is_relevant(p.get("caption"), p.get("transcript"))
                tagged += 1
                changed = True
        if changed:
            save_tiktok_posts(tt_handle, posts)
    if tagged:
        print(f"tagged relevance for {tagged} post(s)")


def classify_all_hooks():
    """Cheap: classify hook type for every reel/video that has a
    transcript but no hook_type yet, across every creator and platform.
    No-ops per-item if ANTHROPIC_API_KEY isn't set (see analyze.py)."""
    classified = 0
    for handle in CREATORS:
        posts = load_existing_posts(handle)
        changed = False
        for p in posts:
            if p.get("transcript") and not p.get("hook_type") and p.get("is_relevant", True):
                hook_type = analyze.classify_hook(p["transcript"])
                if hook_type:
                    p["hook_type"] = hook_type
                    classified += 1
                    changed = True
        if changed:
            save_posts(handle, posts)
    for tt_handle in TIKTOK_CREATORS.values():
        posts = load_existing_tiktok_posts(tt_handle)
        changed = False
        for p in posts:
            if p.get("transcript") and not p.get("hook_type") and p.get("is_relevant", True):
                hook_type = analyze.classify_hook(p["transcript"])
                if hook_type:
                    p["hook_type"] = hook_type
                    classified += 1
                    changed = True
        if changed:
            save_tiktok_posts(tt_handle, posts)
    if classified:
        print(f"classified {classified} hook(s)")


def generate_digest():
    """Generate a Ménagénie-adapted script for the week's top 7 outliers
    (data/_digest_top7.json, written by build.py) and write the digest
    body for the GitHub Actions step to turn into an issue. Adaptation
    generation is a no-op per-item if ANTHROPIC_API_KEY isn't set."""
    top7_file = DATA / "_digest_top7.json"
    if not top7_file.exists():
        return
    top7 = json.loads(top7_file.read_text())
    if not top7:
        print("no outliers this week — skipping digest")
        return

    for item in top7:
        handle = item["creator_handle"].replace("__tiktok", "")
        platform = item["platform"]
        sc = item["shortCode"]
        if platform == "tiktok":
            data_handle = TIKTOK_CREATORS.get(handle)
            if not data_handle:
                continue
            posts = load_existing_tiktok_posts(data_handle)
        else:
            data_handle = handle
            posts = load_existing_posts(handle)
        post = next((p for p in posts if p.get("shortCode") == sc), None)
        if not post or not post.get("transcript"):
            continue
        changed = False
        if not post.get("hook_type"):
            hook_type = analyze.classify_hook(post["transcript"])
            if hook_type:
                post["hook_type"] = hook_type
                changed = True
        if not post.get("adapted_script") and post.get("hook_type"):
            adapted = analyze.generate_adaptation(post["transcript"], post["hook_type"])
            if adapted:
                post["adapted_script"] = adapted
                changed = True
        item["hook_type"] = post.get("hook_type")
        item["adapted_script"] = post.get("adapted_script")
        if changed:
            if platform == "tiktok":
                save_tiktok_posts(data_handle, posts)
            else:
                save_posts(data_handle, posts)

    lines = [f"# Ménagénie Intel — Top {len(top7)} de la semaine\n"]
    for i, item in enumerate(top7, 1):
        lines.append(
            f"## {i}. @{item['creator_username']} ({item['platform']}) — "
            f"{item['ratio']:.1f}× · {item['band_label']}"
        )
        lines.append(f"**Hook** ({item.get('hook_type') or 'non classé'}) : {item['hook'] or '(pas de hook détecté)'}")
        if item.get("adapted_script"):
            lines.append(f"\n💡 **Adaptation Ménagénie :**\n> {item['adapted_script']}")
        lines.append(f"\n🔗 {item['url']}\n")
    (ROOT / "_digest_body.md").write_text("\n".join(lines))
    print(f"digest written for top {len(top7)} outlier(s)")


def main():
    print(f"=== refresh started · {datetime.now().isoformat()} ===")
    print(f"creators: {CREATORS}\n")

    # Pass 1: cheap stats refresh (no transcripts)
    scraped = pass1_stats_refresh()

    # Identify reels we don't have transcripts for yet
    new_reel_urls = _find_new_reels(scraped)
    print(f"\nidentified {len(new_reel_urls)} new reels needing transcripts")

    # Merge stats first (so pass 2 can layer transcripts on saved records)
    merge_pass1_stats(scraped)

    # Pass 2: transcripts ONLY for new reels (saves ~70% of Apify spend)
    transcribed = pass2_transcripts_for_new(new_reel_urls)
    merge_pass2_transcripts(transcribed)

    fetch_profile_details_for_all_creators()
    download_thumbnails()
    extract_all_clips()
    prune_old_assets()

    if TIKTOK_CREATORS:
        tt_scraped = pass1_tiktok_stats_refresh()
        tt_new_urls = _find_new_tiktok_videos(tt_scraped)
        print(f"\nidentified {len(tt_new_urls)} new TikTok videos needing transcripts")
        merge_tiktok_pass1(tt_scraped)
        tt_transcripts = pass2_tiktok_transcripts_for_new(tt_new_urls)
        merge_tiktok_pass2_transcripts(tt_transcripts)

    classify_all_relevance()
    classify_all_hooks()
    build_dashboard()
    generate_digest()
    build_dashboard()  # re-run so this week's freshly-generated adaptations render too

    print(f"\n=== refresh done · {datetime.now().isoformat()} ===")


if __name__ == "__main__":
    main()
