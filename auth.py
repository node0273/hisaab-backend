"""
auth.py — Google OAuth + token encryption for Hisaab
"""
import os
import base64
import requests
from urllib.parse import urlencode
from cryptography.fernet import Fernet

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

def get_fernet():
    key = ENCRYPTION_KEY
    if not key:
        raise ValueError("ENCRYPTION_KEY not set")
    # Ensure key is valid Fernet key (32 url-safe base64 bytes)
    key_bytes = key.encode() if isinstance(key, str) else key
    return Fernet(key_bytes)

def encrypt_token(token: str) -> str:
    """Encrypt a token before storing."""
    if not token:
        return ""
    f = get_fernet()
    return f.encrypt(token.encode()).decode()

def decrypt_token(encrypted: str) -> str:
    """Decrypt a stored token."""
    if not encrypted:
        return ""
    f = get_fernet()
    return f.decrypt(encrypted.encode()).decode()

def get_oauth_url(phone: str) -> str:
    """Generate Google OAuth URL with phone as state."""
    from urllib.parse import quote
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": quote(phone),
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)

def exchange_code(code: str) -> dict:
    """Exchange auth code for tokens."""
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        },
    )
    resp.raise_for_status()
    return resp.json()

def get_user_info(access_token: str) -> dict:
    """Get user profile from Google."""
    resp = requests.get(
        "https://www.googleapis.com/oauth2/v2/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    resp.raise_for_status()
    return resp.json()

def refresh_access_token(refresh_token_enc: str) -> str:
    """Refresh and return new access token."""
    refresh_token = decrypt_token(refresh_token_enc)
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def generate_encryption_key() -> str:
    """Generate a new Fernet encryption key. Run once and save as ENCRYPTION_KEY."""
    return Fernet.generate_key().decode()
