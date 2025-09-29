@@
- def gh_mask(value: str | None) -> None:
-     """
-     Emit GitHub Actions mask directive only when running in Actions.
-     Never print secrets in other environments.
-     """
-     if not value:
-         return
-     if os.getenv("GITHUB_ACTIONS") == "true":
-         # Do not print secrets even in GitHub Actions logs.
-         # If masking is needed, handle it in CI workflow, not here.
-         pass
+ def gh_mask(value: str | None) -> None:
+     """
+     Emit GitHub Actions mask directive only when running in Actions.
+     Safe: the runner interprets ::add-mask:: and does not echo the secret.
+     No output elsewhere.
+     """
+     if not value:
+         return
+     if os.getenv("GITHUB_ACTIONS") == "true":
+         try:
+             print(f"::add-mask::{value}")
+         except Exception:
+             pass
@@
- # Mask all sensitive values
+ # Mask all sensitive values (secrets only)
  for v in [NOTION_API_KEY, NOTION_DB_ID, PUSHOVER_TOKEN, PUSHOVER_USER,
            os.getenv("PUSHOVER_DEVICE"),
            os.getenv("PUSHOVER_PRIORITY"),
            os.getenv("PUSHOVER_SOUND")]:
     gh_mask(v)
@@
- # Filter: today only and payment_method contains Cash
+ # Filter: today only and payment_method non-empty
  payload = {
      "filter": {
          "and": [
              {"property": "created_at", "date": {"on_or_after": start_iso}},
              {"property": "created_at", "date": {"before": end_iso}},
-             {"property": "payment_method", "multi_select": {"contains": "Cash"}},
+             {"property": "payment_method", "multi_select": {"is_not_empty": True}},
          ]
      },
      "page_size": 100
  }
@@
- def send_pushover(title: str, message: str, timestamp: int) -> bool:
+ def send_pushover(title: str, message: str, timestamp: int) -> bool:
@@
-    # Mask message contents before any chance of appearing
-    gh_mask(title)
-    gh_mask(message)
+    # Do not mask or print non-secrets (title/message). We never log them.
@@
- attempt = 0
+ attempt = 0
+ MAX_RETRIES = 5
@@
-        except requests.RequestException:
-            if attempt >= 3:
+        except requests.RequestException:
+            if attempt >= MAX_RETRIES:
                 print("Pushover request failed", file=sys.stderr)
                 return False
             backoff(attempt); attempt += 1
@@
- attempt = 0
+ attempt = 0
+ MAX_RETRIES = 5
@@
-    try:
+    try:
         resp = requests.post(query_url, headers=headers, json=body, timeout=30)
         if resp.status_code == 429:
-            backoff(attempt); attempt += 1; continue
+            if attempt >= MAX_RETRIES:
+                print("Notion rate limit exceeded", file=sys.stderr)
+                sys.exit(2)
+            backoff(attempt); attempt += 1; continue
         resp.raise_for_status()
         data = resp.json()
@@
-# Mask the computed total so if anything ever echoes it, it is redacted
-gh_mask(final_str)
-
-# No print of the value
+# No print of the value
 title = f"Total Cash Sales for {start.strftime('%b %d, %Y')}"
 msg = f"{final_str}"
