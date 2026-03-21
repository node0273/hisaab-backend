import os
import httpx
from urllib.parse import urlencode
from db import save_user_tokens

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
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
                    "code": code,
                    "client_id": CLIENT_ID,
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

        save_user_tokens(
            whatsapp_number=user_id,
            email=user_info["email"],
            name=user_info.get("name", ""),
            access_token=tokens["access_token"],
            refresh_token=tokens.get("refresh_token", ""),
        )

        name = user_info.get("name", "").split()[0]
        async with httpx.AsyncClient() as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": int(user_id),
                "text": f"✅ Gmail connected, {name}!\n\nI can now read your bank emails.\n\nType *summary* to see your spending overview!",
                "parse_mode": "Markdown"
            })

        return """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hisaab - Connected!</title>
    <style>
        body { font-family: -apple-system, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #f0fdf4; }
        .card { background: white; border-radius: 16px; padding: 40px 32px; text-align: center; max-width: 360px; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }
        .check { font-size: 48px; margin-bottom: 16px; }
        h1 { color: #15803d; font-size: 22px; margin: 0 0 10px; }
        p { color: #555; font-size: 15px; line-height: 1.5; margin: 0 0 24px; }
        .btn { display: inline-block; background: #229ED9; color: white; padding: 12px 28px; border-radius: 24px; text-decoration: none; font-weight: 600; }
    </style>
</head>
<body>
    <div class="card">
        <div class="check">✅</div>
        <h1>Gmail connected!</h1>
        <p>Go back to Telegram and start chatting with Hisaab!</p>
        <a class="btn" href="https://t.me/HisaabBot">Open Telegram</a>
    </div>
</body>
</html>"""

    except Exception as e:
        return f"""<!DOCTYPE html>
<html><body style="font-family:sans-serif;text-align:center;padding:50px">
<h2 style="color:#dc2626">Something went wrong</h2>
<p>Please go back to Telegram and try connecting again.</p>
<p style="font-size:12px;color:#aaa">{str(e)}</p>
</body></html>"""
