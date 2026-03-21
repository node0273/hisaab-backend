"""
Gmail reader — fetches and parses bank transaction emails
Auto-refreshes expired tokens
"""
import base64
import re
import requests
from datetime import datetime, timedelta
from db import queue_unknown_email, save_user_tokens

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = __import__('os').environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = __import__('os').environ.get("GOOGLE_CLIENT_SECRET")

def refresh_token_func(refresh_token: str) -> str:
    """Refresh expired access token."""
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "refresh_token": refresh_token,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "grant_type": "refresh_token",
        }
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def gmail_get(path, access_token, params=None):
    resp = requests.get(
        f"{GMAIL_API}/{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
    )
    resp.raise_for_status()
    return resp.json()

def get_header(msg, name):
    for h in msg.get("payload", {}).get("headers", []):
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""

def decode_body(data):
    return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")

def get_body(msg):
    payload = msg.get("payload", {})
    parts = payload.get("parts", [])

    def walk(parts):
        for part in parts:
            mime = part.get("mimeType", "")
            if mime == "text/plain" and part.get("body", {}).get("data"):
                return decode_body(part["body"]["data"])
            if mime == "text/html" and part.get("body", {}).get("data"):
                html = decode_body(part["body"]["data"])
                return re.sub(r"<[^>]+>", " ", html)
            if "parts" in part:
                result = walk(part["parts"])
                if result:
                    return result
        return ""

    body = ""
    if payload.get("body", {}).get("data"):
        body = decode_body(payload["body"]["data"])
    elif parts:
        body = walk(parts)
    return body + " " + msg.get("snippet", "")

def parse_amount(text):
    match = re.search(r"(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)", text, re.IGNORECASE)
    if match:
        return float(match.group(1).replace(",", ""))
    return None

def parse_hdfc(msg):
    subject = get_header(msg, "Subject")
    sender = get_header(msg, "From")
    body = get_body(msg)
    amount = parse_amount(subject + " " + body)
    merchant, mode = "Unknown", None

    if "upi" in subject.lower():
        mode = "UPI"
        m = re.search(r"VPA\s+([\w.\-@]+)\s+on", body)
        if m:
            vpa = m.group(1)
            merchant = vpa.split("@")[0].replace(".", " ").title()
        if "debited" not in body.lower():
            return None
    elif "credit card" in subject.lower():
        mode = "Credit Card"
        m = re.search(r"towards\s+(.+?)\s+on\s+\d", body)
        if m:
            merchant = m.group(1).strip()
        if "debited" not in body.lower():
            return None
    elif "reversal" in subject.lower():
        mode = "Reversal"
        m = re.search(r"From Merchant:\s*(.+?)\s*(Date|Time|\n)", body)
        if m:
            merchant = m.group(1).strip()
    elif "nach" in body.lower() or "ach" in body.lower():
        mode = "NACH"
        if "nachautoemailer" in sender:
            m = re.search(r"towards\s+(.+?)\s+with UMRN", body)
            if m:
                merchant = m.group(1).strip()
    else:
        return None

    if not amount:
        return None
    return {"bank": "HDFC", "mode": mode, "merchant": merchant, "amount": amount,
            "date": datetime.now().strftime("%d-%m-%Y")}

def parse_hsbc(msg):
    subject = get_header(msg, "Subject")
    sender = get_header(msg, "From")
    body = get_body(msg)
    amount = parse_amount(body)
    merchant, mode = "Unknown", None

    if "purchase" in subject.lower() and "hsbc" in subject.lower():
        mode = "Credit Card"
        m = re.search(r"payment to\s+(.+?)\s+on\s+\d", body)
        if m:
            merchant = m.group(1).strip()
        if "used" not in body.lower():
            return None
    elif "mandatehq" in sender:
        mode = "e-Mandate"
        m = re.search(r"Merchant\s+(.+?)[\n\r]", body)
        if m:
            merchant = m.group(1).strip()
    elif "excess balance" in subject.lower():
        mode = "Refund"
        merchant = "HSBC Credit Card Refund"
    else:
        return None

    if not amount:
        return None
    return {"bank": "HSBC", "mode": mode, "merchant": merchant, "amount": amount,
            "date": datetime.now().strftime("%d-%m-%Y")}

def get_transactions(access_token: str, refresh_token: str, days: int = 30, user_id: str = None) -> list:
    after = (datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")

    queries = [
        f"from:alerts@hdfcbank.net after:{after}",
        f"from:nachautoemailer@hdfcbank.net after:{after}",
        f"from:hsbc@hsbc.co.in after:{after}",
        f"from:hsbc@mandatehq.com after:{after}",
    ]

    # Try with current token, refresh if expired
    def try_gmail_get(path, token, params=None):
        try:
            return gmail_get(path, token, params), token
        except requests.HTTPError as e:
            if e.response.status_code in [401, 403] and refresh_token:
                new_token = refresh_token_func(refresh_token)
                # Save new token to DB if user_id provided
                if user_id:
                    try:
                        from db import get_conn
                        import os
                        from cryptography.fernet import Fernet
                        import base64
                        key = os.environ.get("ENCRYPTION_KEY", "").encode()
                        key_bytes = base64.urlsafe_b64encode(key[:32].ljust(32, b'0'))
                        cipher = Fernet(key_bytes)
                        enc = cipher.encrypt(new_token.encode()).decode()
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("UPDATE users SET access_token_enc = %s WHERE whatsapp_number = %s",
                                          (enc, user_id))
                                conn.commit()
                    except:
                        pass
                return gmail_get(path, new_token, params), new_token
            raise

    current_token = access_token
    transactions = []

    for query in queries:
        try:
            data, current_token = try_gmail_get("messages", current_token, {"q": query, "maxResults": 100})
            for m in data.get("messages", []):
                try:
                    full, current_token = try_gmail_get(f"messages/{m['id']}", current_token)
                    sender = get_header(full, "From")
                    if "hdfc" in sender.lower():
                        parsed = parse_hdfc(full)
                    elif "hsbc" in sender.lower() or "mandatehq" in sender.lower():
                        parsed = parse_hsbc(full)
                    else:
                        parsed = None
                    if parsed:
                        transactions.append(parsed)
                except:
                    continue
        except Exception:
            continue

    return transactions
