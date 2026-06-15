#!/usr/bin/env python3
import os, sys, time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ── Config from env
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID_ONLINE_SALES")

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

# ── Run
cursor = None
attempt = 0
MAX_RETRIES = 5

seen_receipts = {}  # receipt_number -> (created_at_val, page_id, amount)
pages_to_delete = []

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
        sys.exit(2)

    for page in data.get("results", []):
        page_id = page["id"]
        props = page.get("properties", {})
        
        # 1. Get receipt number (try common names if 'receipt_number' fails)
        receipt_val = None
        for prop_name in ["receipt_number", "Receipt Number", "Receipt #"]:
            prop = props.get(prop_name, {})
            p_type = prop.get("type")
            if p_type == "title":
                t = prop.get("title", [])
                if t: receipt_val = t[0].get("plain_text")
            elif p_type == "rich_text":
                r = prop.get("rich_text", [])
                if r: receipt_val = r[0].get("plain_text")
            elif p_type == "number":
                receipt_val = str(prop.get("number"))
            if receipt_val: break

        # 2. Get creation time for comparison
        created_at_val = None
        # Try 'created_at' property first
        ca_prop = props.get("created_at", {})
        if ca_prop.get("type") == "date":
            d = ca_prop.get("date", {})
            if d: created_at_val = d.get("start")
        # Fallback to system created_time
        if not created_at_val:
            created_at_val = page.get("created_time")

        # 3. Get amount
        actual = props.get("actual_money", {})
        val = None
        if actual.get("type") == "formula":
            f = actual.get("formula", {})
            if f.get("type") == "number": val = f.get("number")
        if val is None and actual.get("type") == "number":
            val = actual.get("number")
        amount = float(val) if val is not None else 0.0

        if receipt_val:
            if receipt_val in seen_receipts:
                prev_created_at, prev_page_id, prev_amount = seen_receipts[receipt_val]
                # "delete the duplicate on, the latest one, keep the older one"
                # If current is older than what we've seen, keep current and delete previous
                if created_at_val < prev_created_at:
                    pages_to_delete.append(prev_page_id)
                    seen_receipts[receipt_val] = (created_at_val, page_id, amount)
                else:
                    # Current is newer (or same), delete current
                    pages_to_delete.append(page_id)
            else:
                seen_receipts[receipt_val] = (created_at_val, page_id, amount)
        else:
            # No receipt number, treat as unique to be safe
            seen_receipts[f"unique_{page_id}"] = (created_at_val, page_id, amount)

    if data.get("has_more"):
        cursor = data.get("next_cursor")
    else:
        break

# ── Perform Deletions
for pid in pages_to_delete:
    print(f"Deleting duplicate page: {pid}")
    delete_page(pid)

# ── Final Calculation
total = sum(item[2] for item in seen_receipts.values())
final_str = f"{total:.2f}"

title = f"Total Cash Sales for {start.strftime('%b %d, %Y')}"
msg = f"{final_str}"

if pages_to_delete:
    msg += f"\n(Cleaned up {len(pages_to_delete)} duplicates)"

ok = send_pushover(title, msg, int(now_local.timestamp()))
if not ok:
    sys.exit(3)

print(f"Done. Processed {len(seen_receipts)} unique receipts. Deleted {len(pages_to_delete)} duplicates.")
