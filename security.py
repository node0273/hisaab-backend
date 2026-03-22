"""
Hisaab Security — encryption, token rotation, log sanitisation
"""
import os
import base64
import hashlib
import re
from cryptography.fernet import Fernet

ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "")

def get_cipher():
    key = ENCRYPTION_KEY.encode()
    key_bytes = base64.urlsafe_b64encode(key[:32].ljust(32, b'0'))
    return Fernet(key_bytes)

def encrypt(text: str) -> str:
    if not text:
        return ""
    return get_cipher().encrypt(text.encode()).decode()

def decrypt(text: str) -> str:
    if not text:
        return ""
    try:
        return get_cipher().decrypt(text.encode()).decode()
    except:
        return ""

def sanitise_log(text: str) -> str:
    """Remove sensitive data from log messages."""
    # Mask tokens
    text = re.sub(r'(token|key|secret|password|code)["\s:=]+[\w\-\.]+', r'\1=***', text, flags=re.IGNORECASE)
    # Mask amounts
    text = re.sub(r'₹[\d,]+', '₹***', text)
    # Mask phone numbers
    text = re.sub(r'\b\d{10}\b', '**********', text)
    # Mask email addresses
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '***@***.***', text)
    # Mask UPI VPAs
    text = re.sub(r'[\w\.\-]+@[a-zA-Z]+', '***@***', text)
    return text

def hash_user_id(user_id: str) -> str:
    """One-way hash for logging — can't reverse to get real ID."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:12]

def check_session_active(last_active) -> bool:
    """Returns False if user has been inactive for 30+ days."""
    from datetime import datetime, timezone, timedelta
    if not last_active:
        return False
    now = datetime.now(timezone.utc)
    if last_active.tzinfo is None:
        last_active = last_active.replace(tzinfo=timezone.utc)
    return (now - last_active) < timedelta(days=30)
