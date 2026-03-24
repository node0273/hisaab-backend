"""
Security — encryption, session, rate limiting, log sanitisation
"""
import os, base64, hashlib, re
from cryptography.fernet import Fernet
from datetime import datetime, timezone, timedelta

ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "")

def get_cipher():
    key = ENCRYPTION_KEY.encode()
    key_bytes = base64.urlsafe_b64encode(key[:32].ljust(32, b'0'))
    return Fernet(key_bytes)

def encrypt(text: str) -> str:
    if not text: return ""
    try: return get_cipher().encrypt(text.encode()).decode()
    except: return ""

def decrypt(text: str) -> str:
    if not text: return ""
    try: return get_cipher().decrypt(text.encode()).decode()
    except: return ""

def sanitise_log(text: str) -> str:
    text = re.sub(r'(token|key|secret|password)["\s:=]+[\w\-\.]+', r'\1=***', text, flags=re.IGNORECASE)
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '***@***.***', text)
    text = re.sub(r'\b\d{10}\b', '**********', text)
    return text

def check_session_active(last_active) -> bool:
    if not last_active: return False
    now = datetime.now(timezone.utc)
    if last_active.tzinfo is None:
        last_active = last_active.replace(tzinfo=timezone.utc)
    return (now - last_active) < timedelta(days=30)
