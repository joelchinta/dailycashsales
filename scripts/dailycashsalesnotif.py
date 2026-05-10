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

# WhatsApp deep link for the target group
WHATSAPP_URL       = os.getenv("WHATSAPP_URL", "https://chat.whatsapp.com/Futa4ZropmmG18DYnE5tmw")
WHATSAPP_URL_TITLE = os.getenv("WHATSAPP_URL_TITLE", "Open WhatsApp Group")

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
    if not value:
        return
    if os.getenv("GITHUB_ACTIONS") == "true":
        try:
            print(f"::add-mask::{value}", flush=True)
        except Exception:
            pass

# Mask secrets
for v in [NOTION_API_KEY, NOTION_DB_ID, PUSHOVER_TOKEN, PUSHOVER_USER]:
    gh_mask(v)

# ── Date window in Asia/Brunei
tz = ZoneInfo("Asia/Brunei")
now_local = datetime.now(tz)
start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
end   = start + timedelta(days=1)
start_iso = start.isoformat()
end_iso   = end.isoformat()

# ── Notion setup
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

def delete_page(page_id: str):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    try:
        r = requests.patch(url, headers=headers, json={"archived": True}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to delete page {page_id}: {e}", file=sys.stderr)
        return False

def send_pushover(title: str, message: str, timestamp: int) -> bool:
    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token": PUSHOVER_TOKEN,
        "user": PUSHOVER_USER,
        "title": title,
        "message": message,
        "timestamp": timestamp,
        "url": WHATSAPP_URL,
        "url_title": WHATSAPP_URL_TITLE,
    }
    if PUSHOVER_DEVICE: data["device"] = PUSHOVER_DEVICE
    if PUSHOVER_PRIORITY: data["priority"] = PUSHOVER_PRIORITY
    if PUSHOVER_SOUND: data["sound"] = PUSHOVER_SOUND

    attempt = 0
    MAX_RETRIES = 5
    while True:
        try:
            r = requests.post(url, data=data, timeout=15)
            if r.status_code == 429:
                if attempt >= MAX_RETRIES: return False
                backoff(attempt); attempt += 1; continue
            r.raise_for_status()
            return True
        except requests.RequestException:
            if attempt >= MAX_RETRIES: return False
            backoff(attempt); attempt += 1

# ── PASS 1: Deduplicate ALL entries in today's timeframe by receipt_number
#    Query without the Cash filter so we catch duplicates across all payment methods.
dedup_payload = {
    "filter": {
        "and": [
            {"property": "created_at", "date": {"on_or_after": start_iso}},
            {"property": "created_at", "date": {"before": end_iso}},
        ]
    },
    "page_size": 100
}

cursor = None
attempt = 0
MAX_RETRIES = 5

seen_receipts = {}  # receipt_number -> (created_at_val, page_id)
pages_to_delete = []

while True:
    body = dict(dedup_payload)
    if cursor:
        body["start_cursor"] = cursor
    try:
        resp = requests.post(query_url, headers=headers, json=body, timeout=30)
        if resp.status_code == 429:
            if attempt >= MAX_RETRIES: sys.exit(2)
            backoff(attempt); attempt += 1; continue
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        print("Notion request failed during dedup pass", file=sys.stderr)
        sys.exit(2)

    for page in data.get("results", []):
        page_id = page["id"]
        props = page.get("properties", {})

        # Get receipt_number (title property)
        receipt_val = None
        rn_prop = props.get("receipt_number", {})
        if rn_prop.get("type") == "title":
            parts = rn_prop.get("title", [])
            if parts:
                receipt_val = "".join(p.get("plain_text", "") for p in parts).strip()

        if not receipt_val:
            continue  # no receipt number → skip, can't deduplicate

        # Get creation time for comparison
        created_at_val = None
        ca_prop = props.get("created_at", {})
        if ca_prop.get("type") == "date":
            d = ca_prop.get("date")
            if d:
                created_at_val = d.get("start")
        if not created_at_val:
            created_at_val = page.get("created_time", "")

        if receipt_val in seen_receipts:
            prev_created_at, prev_page_id = seen_receipts[receipt_val]
            # Keep the older one, delete the newer one.
            # If timestamps are identical, just pick the current one to delete.
            if created_at_val < prev_created_at:
                # Current is older → keep current, delete previous
                pages_to_delete.append(prev_page_id)
                seen_receipts[receipt_val] = (created_at_val, page_id)
            else:
                # Current is newer or same → delete current
                pages_to_delete.append(page_id)
        else:
            seen_receipts[receipt_val] = (created_at_val, page_id)

    if data.get("has_more"):
        cursor = data.get("next_cursor")
    else:
        break

# Perform deletions
deleted = 0
for pid in pages_to_delete:
    print(f"Deleting duplicate page: {pid}")
    if delete_page(pid):
        deleted += 1

print(f"Dedup complete: {len(seen_receipts)} unique receipts, {deleted}/{len(pages_to_delete)} duplicates deleted.")

# ── PASS 2: Calculate Cash total (original logic, unchanged)
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
            if attempt >= MAX_RETRIES: sys.exit(2)
            backoff(attempt); attempt += 1; continue
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        print("Notion request failed during cash total pass", file=sys.stderr)
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

if pages_to_delete:
    msg += f"\n(Cleaned up {len(pages_to_delete)} duplicates)"

ok = send_pushover(title, msg, int(now_local.timestamp()))
if not ok:
    sys.exit(3)

print("Done")
