"""
analysis.py — Fetch Gmail transactions and generate spending summaries.
"""
import base64
import re
from datetime import datetime, timedelta
import requests

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"

def gmail_get(path, access_token, params=None):
    resp = requests.get(
        f"{GMAIL_API}/{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
    )
    resp.raise_for_status()
    return resp.json()

def get_body(msg):
    def decode(data):
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
    payload = msg.get("payload", {})
    parts = payload.get("parts", [])
    def walk(parts):
        for part in parts:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                return decode(part["body"]["data"])
            if part.get("mimeType") == "text/html" and part.get("body", {}).get("data"):
                html = decode(part["body"]["data"])
                return re.sub(r"<[^>]+>", " ", html)
            if "parts" in part:
                r = walk(part["parts"])
                if r: return r
        return ""
    if payload.get("body", {}).get("data"):
        return decode(payload["body"]["data"])
    return walk(parts) + " " + msg.get("snippet", "")

def get_header(msg, name):
    for h in msg.get("payload", {}).get("headers", []):
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""

def get_date(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z"]:
        try:
            return datetime.strptime(date_str[:31].strip(), fmt)
        except: pass
    return datetime.now()

def parse_transaction(msg):
    subject = get_header(msg, "Subject")
    sender = get_header(msg, "From")
    body = get_body(msg)
    date = get_date(msg)
    amount, merchant, mode, bank = None, "Unknown", "Other", "Unknown"

    # HDFC UPI
    if "alerts@hdfcbank.net" in sender and ("UPI" in subject or "upi" in body.lower()):
        if "debited" not in body.lower(): return None
        mode, bank = "UPI", "HDFC"
        m = re.search(r"Rs\.?\s*([\d,]+\.?\d*)", body)
        if m: amount = float(m.group(1).replace(",", ""))
        m2 = re.search(r"VPA\s+([\w.\-@]+)\s+on", body)
        if m2:
            vpa = m2.group(1)
            merchant = vpa.split("@")[0].replace(".", " ").title()

    # HDFC Credit Card
    elif "alerts@hdfcbank.net" in sender and "Credit Card" in subject:
        if "debited" not in body.lower(): return None
        mode, bank = "Credit Card", "HDFC"
        m = re.search(r"Rs\.?\s*([\d,]+\.?\d*)", subject + " " + body)
        if m: amount = float(m.group(1).replace(",", ""))
        m2 = re.search(r"towards\s+(.+?)\s+on\s+\d", body)
        if m2: merchant = m2.group(1).strip()

    # HDFC NACH
    elif "nachautoemailer@hdfcbank.net" in sender:
        mode, bank = "NACH", "HDFC"
        m = re.search(r"Rs\.?\s*([\d,]+\.?\d*)", body)
        if m: amount = float(m.group(1).replace(",", ""))
        m2 = re.search(r"towards\s+(.+?)\s+with UMRN", body)
        if m2: merchant = m2.group(1).strip()

    # HSBC Credit Card
    elif ("hsbc@hsbc.co.in" in sender or "hsbc" in sender.lower()) and "Credit Card" in subject:
        if "used" not in body.lower(): return None
        mode, bank = "Credit Card", "HSBC"
        m = re.search(r"INR\s*([\d,]+\.?\d*)|Rs\.?\s*([\d,]+\.?\d*)", body)
        if m: amount = float((m.group(1) or m.group(2)).replace(",", ""))
        m2 = re.search(r"payment to\s+(.+?)\s+on\s+\d", body)
        if m2: merchant = m2.group(1).strip()

    # HSBC eMandate
    elif "mandatehq.com" in sender:
        mode, bank = "e-Mandate", "HSBC"
        m = re.search(r"INR\s*([\d,]+\.?\d*)|Rs\.?\s*([\d,]+\.?\d*)", body)
        if m: amount = float((m.group(1) or m.group(2)).replace(",", ""))
        m2 = re.search(r"Merchant\s+(.+?)[\n\r]", body)
        if m2: merchant = m2.group(1).strip()

    else:
        return None

    if amount is None:
        return None

    return {
        "date": date.strftime("%d-%m-%Y"),
        "amount": amount,
        "merchant": merchant,
        "mode": mode,
        "bank": bank,
    }

async def fetch_transactions(access_token: str, days: int = 30) -> list:
    """Fetch and parse bank transactions from Gmail."""
    from_date = (datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")
    to_date = datetime.now().strftime("%Y/%m/%d")

    queries = [
        f"from:alerts@hdfcbank.net after:{from_date} before:{to_date}",
        f"from:nachautoemailer@hdfcbank.net after:{from_date} before:{to_date}",
        f"from:hsbc@hsbc.co.in after:{from_date} before:{to_date}",
        f"from:hsbc@mandatehq.com after:{from_date} before:{to_date}",
        f"subject:HSBC Credit Card after:{from_date} before:{to_date}",
    ]

    transactions = []
    seen_ids = set()

    for q in queries:
        try:
            data = gmail_get("messages", access_token, {"q": q, "maxResults": 100})
            for m in data.get("messages", []):
                if m["id"] in seen_ids:
                    continue
                seen_ids.add(m["id"])
                full = gmail_get(f"messages/{m['id']}", access_token)
                txn = parse_transaction(full)
                if txn:
                    transactions.append(txn)
        except Exception:
            continue

    return sorted(transactions, key=lambda x: x["date"], reverse=True)

async def get_spending_summary(whatsapp_number: str, access_token: str, days: int = 30) -> str:
    """Generate a WhatsApp-friendly spending summary."""
    try:
        transactions = await fetch_transactions(access_token, days=days)

        if not transactions:
            return (f"📭 No bank transactions found for the last {days} days.\n\n"
                    f"Make sure your HDFC or HSBC alert emails are in your Gmail inbox.")

        total = sum(t["amount"] for t in transactions)
        merchant_totals = {}
        mode_totals = {}
        bank_totals = {}

        for t in transactions:
            m = t.get("merchant", "Unknown")
            merchant_totals[m] = merchant_totals.get(m, 0) + t["amount"]
            mode_totals[t["mode"]] = mode_totals.get(t["mode"], 0) + t["amount"]
            bank_totals[t["bank"]] = bank_totals.get(t["bank"], 0) + t["amount"]

        top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]

        period = "this month" if days == 30 else f"last {days} days"
        lines = [f"💰 *Your spending — {period}*\n"]
        lines.append(f"*Total: ₹{round(total):,}* across {len(transactions)} transactions\n")

        lines.append("🏆 *Top merchants:*")
        for merchant, amt in top_merchants:
            pct = round(amt / total * 100)
            lines.append(f"  • {merchant}: ₹{round(amt):,} ({pct}%)")

        lines.append("\n💳 *By payment mode:*")
        for mode, amt in sorted(mode_totals.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {mode}: ₹{round(amt):,}")

        if len(bank_totals) > 1:
            lines.append("\n🏦 *By bank:*")
            for bank, amt in sorted(bank_totals.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"  • {bank}: ₹{round(amt):,}")

        lines.append("\n_Ask me anything — 'How much on food?' or 'Any big transactions?'_")
        return "\n".join(lines)

    except Exception as e:
        return f"❌ Couldn't fetch your transactions right now. Please try again in a moment."

async def run_initial_analysis(whatsapp_number: str):
    """Run after first Gmail connection — send initial summary."""
    from db import get_tokens
    from twilio_handler import send_whatsapp
    import asyncio
    await asyncio.sleep(5)  # Give tokens time to save
    access_token, _ = get_tokens(whatsapp_number)
    if access_token:
        summary = await get_spending_summary(whatsapp_number, access_token, days=30)
        send_whatsapp(whatsapp_number, summary)
