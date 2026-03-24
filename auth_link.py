"""Google OAuth — gmail.readonly only"""
import os, httpx
from urllib.parse import urlencode
from db import save_gmail_account, get_gmail_accounts

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
BACKEND_URL = os.environ.get("BACKEND_URL", "")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

def get_auth_url(user_id: str) -> str:
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": user_id,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)

async def handle_callback(code: str, state: str) -> str:
    user_id = state
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={"code": code, "client_id": CLIENT_ID,
                      "client_secret": CLIENT_SECRET,
                      "redirect_uri": REDIRECT_URI,
                      "grant_type": "authorization_code"}
            )
            resp.raise_for_status()
            tokens = resp.json()

            user_resp = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {tokens['access_token']}"}
            )
            info = user_resp.json()

        success, msg = save_gmail_account(
            user_id, info["email"], info.get("name", ""),
            tokens["access_token"], tokens.get("refresh_token", "")
        )

        accounts = get_gmail_accounts(user_id)
        count = len(accounts)

        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": int(user_id),
                    "text": (
                        f"✅ *Gmail connected!*\n\n"
                        f"Account: `{info['email']}`\n\n"
                        f"I'll now read your bank alert emails. "
                        f"Type *sync* to fetch your transactions, "
                        f"or *summary* to see your spending!"
                        + (f"\n\nYou can connect up to 3 Gmail accounts. Connected: {count}/3"
                           if count < 3 else "")
                    ),
                    "parse_mode": "Markdown"
                }
            )

        return f"""<!DOCTYPE html><html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hisaab — Connected!</title>
<style>
body{{font-family:-apple-system,sans-serif;background:#f0fdf4;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}
.card{{background:white;border-radius:20px;padding:40px 32px;text-align:center;max-width:360px;width:100%;box-shadow:0 8px 32px rgba(0,0,0,0.08)}}
.icon{{font-size:56px;margin-bottom:16px}}
h1{{color:#15803d;font-size:24px;margin-bottom:10px}}
p{{color:#555;font-size:15px;line-height:1.6;margin-bottom:8px}}
.email{{background:#f0fdf4;border-radius:8px;padding:8px 12px;font-size:13px;color:#166534;margin:12px 0;word-break:break-all}}
.btn{{display:block;background:#229ED9;color:white;padding:14px 28px;border-radius:12px;text-decoration:none;font-weight:600;font-size:16px;margin-top:24px}}
</style></head>
<body><div class="card">
<div class="icon">✅</div>
<h1>Gmail Connected!</h1>
<p>Successfully connected:</p>
<div class="email">{info['email']}</div>
<p>Go back to Telegram and type <strong>sync</strong> to fetch your transactions!</p>
<a class="btn" href="https://t.me/">Open Telegram</a>
</div></body></html>"""

    except Exception as e:
        return f"""<!DOCTYPE html><html>
<head><meta charset="UTF-8"><title>Error</title>
<style>body{{font-family:sans-serif;text-align:center;padding:50px;background:#fef2f2}}h2{{color:#dc2626}}</style>
</head><body><h2>Something went wrong</h2><p>Please go back to Telegram and try again.</p></body></html>"""
