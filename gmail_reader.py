"""
Gmail reader — fetches and parses bank transaction emails
Uses rule-based parsing first, AI fallback for unknowns
"""
import base64
import re
import requests
from datetime import datetime, timedelta
from db import queue_unknown_email

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"

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

def get_transactions(access_token: str, refresh_token: str, days: int = 30) -> list:
    after = (datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")
    before = datetime.now().strftime("%Y/%m/%d")

    queries = [
        f"from:alerts@hdfcbank.net after:{after}",
        f"from:nachautoemailer@hdfcbank.net after:{after}",
        f"from:hsbc@hsbc.co.in after:{after}",
        f"from:hsbc@mandatehq.com after:{after}",
    ]

    transactions = []
    for query in queries:
        try:
            data = gmail_get("messages", access_token, {"q": query, "maxResults": 100})
            for m in data.get("messages", []):
                full = gmail_get(f"messages/{m['id']}", access_token)
                sender = get_header(full, "From")

                if "hdfc" in sender.lower():
                    parsed = parse_hdfc(full)
                elif "hsbc" in sender.lower() or "mandatehq" in sender.lower():
                    parsed = parse_hsbc(full)
                else:
                    parsed = None

                if parsed:
                    transactions.append(parsed)
        except Exception:
            continue

    return transactions
