"""
Google OAuth flow for Hisaab
State = WhatsApp number (so we know who to save tokens for)
"""
import os
import requests
from urllib.parse import urlencode
from db import save_user_tokens, get_user, create_user
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
        # Exchange code for tokens
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

        # Get user info
        user_resp = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {tokens['access_token']}"}
        )
        user_info = user_resp.json()

        # Save to database (encrypted)
        save_user_tokens(
            whatsapp_number=whatsapp_number,
            email=user_info["email"],
            name=user_info.get("name", ""),
            access_token=tokens["access_token"],
            refresh_token=tokens.get("refresh_token", ""),
        )

        # Notify user on WhatsApp
        name = user_info.get("name", "").split()[0]
        send_whatsapp(
            whatsapp_number,
            f"Gmail connected successfully! I can now read your bank emails.\n\nType *summary* to get your spending overview, or ask me anything like 'how much did I spend this month?'"
        )

        return """
        <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
            <h2>Gmail connected!</h2>
            <p>Go back to WhatsApp and start chatting with Hisaab.</p>
        </body></html>
        """
    except Exception as e:
        return f"""
        <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
            <h2>Something went wrong</h2>
            <p>{str(e)}</p>
        </body></html>
        """
