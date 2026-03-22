"""
Google OAuth — gmail.readonly only (no gmail.send)
Supports multiple Gmail accounts per user
"""
import os
import httpx
from urllib.parse import urlencode
from db import save_gmail_account, get_gmail_accounts

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
BACKEND_URL = os.environ.get("BACKEND_URL", "")

# gmail.readonly ONLY — removed gmail.send
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

def get_google_auth_url(user_id: str) -> str:
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

async def handle_google_callback(code: str, state: str) -> str:
    user_id = state
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code, "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "redirect_uri": REDIRECT_URI,
                    "grant_type": "authorization_code",
                }
            )
            resp.raise_for_status()
            tokens = resp.json()

            user_resp = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {tokens['access_token']}"}
            )
            user_info = user_resp.json()

        success, msg = save_gmail_account(
            user_id=user_id,
            email=user_info["email"],
            name=user_info.get("name", ""),
            access_token=tokens["access_token"],
            refresh_token=tokens.get("refresh_token", ""),
        )

        name = user_info.get("name", "").split()[0]
        email = user_info["email"]

        # Send Telegram confirmation
        accounts = get_gmail_accounts(user_id)
        account_count = len(accounts)
        add_more = f"\n\nYou can connect up to 3 Gmail accounts\. Connected: {account_count}/3" if account_count < 3 else ""

        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": int(user_id),
                    "text": f"✅ *Gmail connected\!*\n\nAccount: `{email}`\n\nI'll now read your bank alert emails from this account\. Type *summary* to see your spending\!{add_more}",
                    "parse_mode": "MarkdownV2"
                }
            )

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hisaab — Gmail Connected</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #f0fdf4; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }}
        .card {{ background: white; border-radius: 20px; padding: 40px 32px; text-align: center; max-width: 360px; width: 100%; box-shadow: 0 8px 32px rgba(0,0,0,0.08); }}
        .icon {{ font-size: 56px; margin-bottom: 16px; }}
        h1 {{ color: #15803d; font-size: 24px; margin-bottom: 10px; }}
        p {{ color: #555; font-size: 15px; line-height: 1.6; margin-bottom: 8px; }}
        .email {{ background: #f0fdf4; border-radius: 8px; padding: 8px 12px; font-size: 13px; color: #166534; margin: 12px 0; word-break: break-all; }}
        .btn {{ display: block; background: #229ED9; color: white; padding: 14px 28px; border-radius: 12px; text-decoration: none; font-weight: 600; font-size: 16px; margin-top: 24px; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="icon">✅</div>
        <h1>Gmail Connected!</h1>
        <p>Successfully connected:</p>
        <div class="email">{email}</div>
        <p>Go back to Telegram and type <strong>summary</strong> to see your spending!</p>
        <a class="btn" href="https://t.me/">Open Telegram</a>
    </div>
</body>
</html>"""

    except Exception as e:
        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Error</title>
<style>body{{font-family:sans-serif;text-align:center;padding:50px;background:#fef2f2}}h2{{color:#dc2626}}p{{color:#555;margin-top:10px}}</style>
</head>
<body><h2>Something went wrong</h2><p>Please go back to Telegram and try connecting again.</p></body></html>"""
