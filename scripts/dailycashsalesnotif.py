#!/usr/bin/env python3
import os, sys, time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ── Config from env
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID")

PUSHOVER_TOKEN    = os.getenv("PUSHOVER_TOKEN")
PUSHOVER_USER     = os.getenv("PUSHOVER_USER")
PUSHOVER_DEVICE   = os.getenv("PUSHOVER_DEVICE")    # optional
PUSHOVER_PRIORITY = os.getenv("PUSHOVER_PRIORITY")  # optional
PUSHOVER_SOUND    = os.getenv("PUSHOVER_SOUND")     # optional

# ── Fail fast if required secrets missing
def require(name, val):
    if not val:
        print(f"Missing {name}", file=sys.stderr)
        sys.exit(1)

require("NOTION_API_KEY", NOTION_API_KEY)
require("NOTION_DB_ID",   NOTION_DB_ID)
require("PUSHOVER_TOKEN", PUSHOVER_TOKEN)
require("PUSHOVER_USER",  PUSHOVER_USER)

def gh_mask(value: str | None) -> None:
    """
    Emit GitHub Actions mask directive only when running in Actions.
    No output elsewhere.
    """
    if not value:
        return
    if os.getenv("GITHUB_ACTIONS") == "true":
        try:
            print(f"::add-mask::{value}")
        except Exception:
            pass

# Mask secrets only
for v in [
    NOTION_API_KEY,
    NOTION_DB_ID,
    PUSHOVER_TOKEN,
    PUSHOVER_USER,
    os.getenv("PUSHOVER_DEVICE"),
    os.getenv("PUSHOVER_PRIORITY"),
    os.getenv("PUSHOVER_SOUND"),
]:
    gh_mask(v)

# ── Date window in Asia/Brunei
tz = ZoneInfo("Asia/Brunei")
now_local = datetime.now(tz)
start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
end   = start + timedelta(days=1)
start_iso = start.isoformat()
end_iso   = end.isoformat()

# ── Notion query
headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
query_url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"

# Filter: today only and payment_method contains "Cash"
payload = {
    "filter": {
        "and": [
            {"property": "created_at", "date": {"on_or_after": start_iso}},
            {"property": "created_at", "date": {"before": end_iso}},
            {"property": "payment_method", "multi_select": {"contains": "Cash"}},
        ]
    },
    "page_size": 100
}

def backoff(attempt):
    time.sleep(min(2 ** attempt, 10))

def send_pushover(title: str, message: str, timestamp: int) -> bool:
    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token": PUSHOVER_TOKEN,
        "user": PUSHOVER_USER,
        "title": title,
        "message": message,
        "timestamp": timestamp,
    }
    if PUSHOVER_DEVICE:
        data["device"] = PUSHOVER_DEVICE
    if PUSHOVER_PRIORITY:
        data["priority"] = PUSHOVER_PRIORITY
    if PUSHOVER_SOUND:
        data["sound"] = PUSHOVER_SOUND

    attempt = 0
    MAX_RETRIES = 5
    while True:
        try:
            r = requests.post(url, data=data, timeout=15)
            if r.status_code == 429:
                if attempt >= MAX_RETRIES:
                    print("Pushover rate limit exceeded", file=sys.stderr)
                    return False
                backoff(attempt); attempt += 1; continue
            r.raise_for_status()
            js = r.json()
            if js.get("status") != 1:
                print("Pushover error", file=sys.stderr)
                return False
            return True
        except requests.RequestException:
            if attempt >= MAX_RETRIES:
                print("Pushover request failed", file=sys.stderr)
                return False
            backoff(attempt); attempt += 1

# ── Run
total = 0.0
cursor = None
attempt = 0
MAX_RETRIES = 5

while True:
    body = dict(payload)
    if cursor:
        body["start_cursor"] = cursor
    try:
        resp = requests.post(query_url, headers=headers, json=body, timeout=30)
        if resp.status_code == 429:
            if attempt >= MAX_RETRIES:
                print("Notion rate limit exceeded", file=sys.stderr)
                sys.exit(2)
            backoff(attempt); attempt += 1; continue
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        print("Notion request failed", file=sys.stderr)
        sys.exit(2)

    for page in data.get("results", []):
        props = page.get("properties", {})
        actual = props.get("actual_money", {})
        val = None
        if actual.get("type") == "formula":
            f = actual.get("formula", {})
            if f.get("type") == "number":
                val = f.get("number")
        if val is None and actual.get("type") == "number":
            val = actual.get("number")
        if isinstance(val, (int, float)):
            total += float(val)

    if data.get("has_more"):
        cursor = data.get("next_cursor")
    else:
        break

final_str = f"{total:.2f}"

title = f"Total Cash Sales for {start.strftime('%b %d, %Y')}"
msg = f"{final_str}"

ok = send_pushover(title, msg, int(now_local.timestamp()))
if not ok:
    sys.exit(3)

print("Done")
