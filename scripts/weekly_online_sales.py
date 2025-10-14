#!/usr/bin/env python3
import os, sys, time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

# ── Config from env
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID   = os.getenv("NOTION_DB_ID_ONLINE_SALES")

PUSHOVER_TOKEN    = os.getenv("PUSHOVER_TOKEN")
PUSHOVER_USER     = os.getenv("PUSHOVER_USER")
PUSHOVER_DEVICE   = os.getenv("PUSHOVER_DEVICE")
PUSHOVER_PRIORITY = os.getenv("PUSHOVER_PRIORITY")
PUSHOVER_SOUND    = os.getenv("PUSHOVER_SOUND")

def require(name, val):
    if not val:
        sys.exit(1)

require("NOTION_API_KEY", NOTION_API_KEY)
require("NOTION_DB_ID_ONLINE_SALES", NOTION_DB_ID)
require("PUSHOVER_TOKEN", PUSHOVER_TOKEN)
require("PUSHOVER_USER", PUSHOVER_USER)

def gh_mask(value: str | None) -> None:
    if value and os.getenv("GITHUB_ACTIONS") == "true":
        try:
            print(f"::add-mask::{value}", flush=True)
        except Exception:
            pass

for v in [
    NOTION_API_KEY, NOTION_DB_ID,
    PUSHOVER_TOKEN, PUSHOVER_USER,
    PUSHOVER_DEVICE, PUSHOVER_PRIORITY, PUSHOVER_SOUND,
]:
    gh_mask(v)

# ── Compute previous week window (Mon–Sun) in Asia/Brunei
tz = ZoneInfo("Asia/Brunei")
today = datetime.now(tz)
last_monday = (today - timedelta(days=today.weekday() + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
last_sunday = last_monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
start_iso = last_monday.isoformat()
end_iso   = last_sunday.isoformat()

# ── Notion setup
headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
query_url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"

def backoff(attempt):
    time.sleep(min(2 ** attempt, 10))

def query_notion(source_name: str):
    payload = {
        "filter": {
            "and": [
                {"property": "Order Date", "date": {"on_or_after": start_iso}},
                {"property": "Order Date", "date": {"on_or_before": end_iso}},
                {"property": "Source", "title": {"equals": source_name}},
            ]
        },
        "page_size": 100
    }
    results, cursor, attempt = [], None, 0
    MAX_RETRIES = 5
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        try:
            r = requests.post(query_url, headers=headers, json=body, timeout=30)
            if r.status_code == 429:
                if attempt >= MAX_RETRIES:
                    sys.exit(2)
                backoff(attempt); attempt += 1; continue
            r.raise_for_status()
            data = r.json()
        except requests.RequestException:
            sys.exit(2)
        results.extend(data.get("results", []))
        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break
    return results

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

    attempt, MAX_RETRIES = 0, 5
    while True:
        try:
            r = requests.post(url, data=data, timeout=15)
            if r.status_code == 429:
                if attempt >= MAX_RETRIES:
                    return False
                backoff(attempt); attempt += 1; continue
            r.raise_for_status()
            if r.json().get("status") != 1:
                return False
            return True
        except requests.RequestException:
            if attempt >= MAX_RETRIES:
                return False
            backoff(attempt); attempt += 1

def aggregate(results, reduction_rate):
    daily_totals = defaultdict(float)
    for page in results:
        props = page.get("properties", {})
        date_prop = props.get("Order Date", {})
        amount_prop = props.get("Order Amount", {})
        if date_prop.get("type") != "date" or not date_prop.get("date"):
            continue
        date_str = date_prop["date"].get("start")
        if not date_str:
            continue
        date = datetime.fromisoformat(date_str).astimezone(tz)
        date_key = date.strftime("%d-%m-%Y")

        val = None
        if amount_prop.get("type") == "number":
            val = amount_prop.get("number")
        elif amount_prop.get("type") == "formula":
            f = amount_prop.get("formula", {})
            if f.get("type") == "number":
                val = f.get("number")
        if not isinstance(val, (int, float)):
            continue

        daily_totals[date_key] += val * (1 - reduction_rate)
    return dict(sorted(daily_totals.items()))

gomamam_data = query_notion("GoMamam")
heydomo_data = query_notion("HeyDomo")

gomamam_daily = aggregate(gomamam_data, 0.20)
heydomo_daily = aggregate(heydomo_data, 0.12)

gomamam_total = sum(gomamam_daily.values())
heydomo_total = sum(heydomo_daily.values())

def format_block(title, daily_map, total):
    lines = [f"{title}"]
    for date, val in daily_map.items():
        lines.append(f"{date} - ${val:,.2f}")
    lines.append(f"Total: ${total:,.2f}")
    return "\n".join(lines)

body = (
    f"{format_block('GoMamam Online Sales', gomamam_daily, gomamam_total)}\n\n"
    f"{format_block('HeyDomo Online Sales', heydomo_daily, heydomo_total)}"
)

title = f"Weekly Online Sales Summary ({last_monday.strftime('%d-%m-%Y')} to {last_sunday.strftime('%d-%m-%Y')})"

if not send_pushover(title, body, int(datetime.now(tz).timestamp())):
    sys.exit(3)
sys.exit(0)
