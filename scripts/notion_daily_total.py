#!/usr/bin/env python3
import os, sys, time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID")

PUSHOVER_TOKEN   = os.getenv("PUSHOVER_TOKEN")
PUSHOVER_USER    = os.getenv("PUSHOVER_USER")
PUSHOVER_DEVICE  = os.getenv("PUSHOVER_DEVICE")     # optional
PUSHOVER_PRIORITY= os.getenv("PUSHOVER_PRIORITY")   # optional, -2..2
PUSHOVER_SOUND   = os.getenv("PUSHOVER_SOUND")      # optional

if not NOTION_API_KEY or not NOTION_DB_ID:
    print("Missing NOTION_API_KEY or NOTION_DB_ID", file=sys.stderr)
    sys.exit(1)
if not PUSHOVER_TOKEN or not PUSHOVER_USER:
    print("Missing PUSHOVER_TOKEN or PUSHOVER_USER", file=sys.stderr)
    sys.exit(1)

tz = ZoneInfo("Asia/Brunei")
now_local = datetime.now(tz)
start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
end   = start + timedelta(days=1)
start_iso = start.isoformat()
end_iso   = end.isoformat()

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
query_url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"

payload = {
    "filter": {
        "and": [
            {"property": "created_at", "date": {"on_or_after": start_iso}},
            {"property": "created_at", "date": {"before": end_iso}},
            {"property": "payment_method", "multi_select": {"is_not_empty": True}},
        ]
    },
    "page_size": 100
}

def backoff(attempt):
    time.sleep(min(2 ** attempt, 10))

def send_pushover(title: str, message: str):
    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token": PUSHOVER_TOKEN,
        "user": PUSHOVER_USER,
        "title": title,
        "message": message,
        "timestamp": int(now_local.timestamp()),
    }
    if PUSHOVER_DEVICE:
        data["device"] = PUSHOVER_DEVICE
    if PUSHOVER_PRIORITY:
        data["priority"] = PUSHOVER_PRIORITY
    if PUSHOVER_SOUND:
        data["sound"] = PUSHOVER_SOUND

    attempt = 0
    while True:
        try:
            r = requests.post(url, data=data, timeout=15)
            if r.status_code == 429:
                backoff(attempt); attempt += 1; continue
            r.raise_for_status()
            js = r.json()
            if js.get("status") != 1:
                print(f"Pushover error: {js}", file=sys.stderr)
                return False
            return True
        except requests.RequestException as e:
            if attempt >= 3:
                print(f"Pushover request failed: {e}", file=sys.stderr)
                return False
            backoff(attempt); attempt += 1

total = 0.0
cursor = None
attempt = 0

while True:
    body = dict(payload)
    if cursor:
        body["start_cursor"] = cursor
    try:
        resp = requests.post(query_url, headers=headers, json=body, timeout=30)
        if resp.status_code == 429:
            backoff(attempt); attempt += 1; continue
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"Notion request failed: {e}", file=sys.stderr)
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
print(final_str)

title = "Daily total"
msg = f"Date: {start.date().isoformat()} Asia/Brunei\nTotal actual_money: {final_str}"
ok = send_pushover(title, msg)
if not ok:
    sys.exit(3)
