"""
Google OAuth flow for Hisaab
State = WhatsApp number (so we know who to save tokens for)
"""
import os
import requests
from urllib.parse import urlencode
from db import save_user_tokens
from twilio_helper import send_whatsapp

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

def get_google_auth_url(whatsapp_number: str) -> str:
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": whatsapp_number,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)

async def handle_google_callback(code: str, state: str) -> str:
    whatsapp_number = state
    try:
        resp = requests.post(
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

        user_resp = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {tokens['access_token']}"}
        )
        user_info = user_resp.json()

        save_user_tokens(
            whatsapp_number=whatsapp_number,
            email=user_info["email"],
            name=user_info.get("name", ""),
            access_token=tokens["access_token"],
            refresh_token=tokens.get("refresh_token", ""),
        )

        name = user_info.get("name", "").split()[0]
        send_whatsapp(
            whatsapp_number,
            f"✅ Gmail connected successfully, {name}!\n\nI can now read your bank emails. Type *summary* to see your spending overview, or ask me anything!"
        )

        return """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hisaab - Connected!</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #f0fdf4; }
        .card { background: white; border-radius: 16px; padding: 40px 32px; text-align: center; max-width: 360px; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }
        .check { width: 64px; height: 64px; background: #22c55e; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 20px; font-size: 28px; }
        h1 { color: #15803d; font-size: 22px; margin: 0 0 10px; }
        p { color: #555; font-size: 15px; line-height: 1.5; margin: 0 0 24px; }
        .btn { display: inline-block; background: #25D366; color: white; padding: 12px 28px; border-radius: 24px; text-decoration: none; font-weight: 600; font-size: 15px; }
    </style>
</head>
<body>
    <div class="card">
        <div class="check">✓</div>
        <h1>Gmail connected!</h1>
        <p>Go back to WhatsApp and start chatting with Hisaab to see your spending insights.</p>
        <a class="btn" href="whatsapp://send?phone=14155238886">Open WhatsApp</a>
    </div>
</body>
</html>"""

    except Exception as e:
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hisaab - Error</title>
    <style>
        body {{ font-family: sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #fef2f2; }}
        .card {{ background: white; border-radius: 16px; padding: 40px 32px; text-align: center; max-width: 360px; }}
        h1 {{ color: #dc2626; }}
        p {{ color: #555; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>Something went wrong</h1>
        <p>Please go back to WhatsApp and try connecting again.</p>
        <p style="font-size:12px;color:#aaa">{str(e)}</p>
    </div>
</body>
</html>"""
