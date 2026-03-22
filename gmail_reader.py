"""
Gmail Reader v2 — incremental sync, 15 banks, 2-day buffer
"""
import base64, re, os, requests
from datetime import datetime, timedelta
from db import get_last_sync, update_sync_log, save_transactions, update_access_token
from merchant_resolver import resolve_merchant
from security import sanitise_log

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

# Bank sender map
BANK_SENDERS = {
    "alerts@hdfcbank.net":          "HDFC",
    "nachautoemailer@hdfcbank.net": "HDFC",
    "alerts@icicibank.com":         "ICICI",
    "axisindiaalerts@axisbank.com": "Axis",
    "sbialerts@sbi.co.in":          "SBI",
    "alerts@sbicard.com":           "SBI",
    "alerts@kotak.com":             "Kotak",
    "alerts@yesbank.in":            "Yes Bank",
    "alerts@idfcfirstbank.com":     "IDFC First",
    "alerts@indusind.com":          "IndusInd",
    "hsbc@hsbc.co.in":              "HSBC",
    "hsbc@mandatehq.com":           "HSBC",
    "alerts@federalbank.co.in":     "Federal Bank",
    "alerts@pnb.co.in":             "PNB",
    "alerts@bankofbaroda.com":      "Bank of Baroda",
    "alerts@rblbank.com":           "RBL",
    "alerts@canarabank.in":         "Canara Bank",
}

def refresh_access_token(refresh_token: str) -> str:
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "refresh_token": refresh_token,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    })
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]

def gmail_get(path, access_token, params=None):
    resp = requests.get(
        f"{GMAIL_API}/{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
    )
    if resp.status_code in [401, 403]:
        raise requests.HTTPError(response=resp)
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
    def walk(parts):
        for part in parts:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                return decode_body(part["body"]["data"])
            if "parts" in part:
                r = walk(part["parts"])
                if r: return r
        return ""
    body = ""
    if payload.get("body", {}).get("data"):
        body = decode_body(payload["body"]["data"])
    elif payload.get("parts"):
        body = walk(payload["parts"])
    return body + " " + msg.get("snippet", "")

def parse_amount(text):
    m = re.search(r"(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)", text, re.IGNORECASE)
    return float(m.group(1).replace(",", "")) if m else None

def parse_date(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"]:
        try:
            return datetime.strptime(date_str[:35].strip(), fmt).date()
        except:
            pass
    return datetime.now().date()

def parse_with_ai(msg, bank: str, gmail_account: str) -> dict:
    """AI-based parsing for banks without specific rules."""
    from merchant_resolver import ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        return None

    body = get_body(msg)[:1500]
    subject = get_header(msg, "Subject")

    prompt = f"""Extract transaction details from this {bank} bank alert email.

Subject: {subject}
Body: {body}

Return ONLY valid JSON or null if not a debit transaction:
{{"amount": 1234.56, "merchant": "merchant name", "vpa": "vpa@bank or empty", "mode": "UPI/Credit Card/NACH/Debit Card", "is_debit": true}}

Rules:
- Only extract DEBIT transactions (money going OUT)
- Return null for credit, refund, balance alerts, OTP emails
- Amount must be a number, not null"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 200,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=5
        )
        resp.raise_for_status()
        import json
        text = resp.json()["content"][0]["text"].strip()
        text = re.sub(r'```json|```', '', text).strip()
        if text.lower() == 'null':
            return None
        data = json.loads(text)
        if not data.get("is_debit") or not data.get("amount"):
            return None

        merchant = data.get("merchant", "")
        vpa = data.get("vpa", "")
        amount = float(data["amount"])
        canonical, category, treatment, person_name, ai_cls = resolve_merchant(merchant, vpa, amount)

        return {
            "bank": bank, "mode": data.get("mode", "Unknown"),
            "amount": amount, "merchant_raw": merchant,
            "merchant_canonical": canonical, "category": category,
            "treatment": treatment, "vpa": vpa, "person_name": person_name,
            "date": parse_date(msg),
            "msg_id": get_header(msg, "Message-ID") or msg.get("id", ""),
            "gmail_account": gmail_account, "ai_classified": ai_cls,
        }
    except:
        return None

def parse_hdfc(msg, gmail_account: str) -> dict:
    subject = get_header(msg, "Subject").lower()
    sender = get_header(msg, "From").lower()
    body = get_body(msg)
    amount = parse_amount(subject + " " + body)
    merchant, mode, vpa = "", "", ""

    # Credit card payment — settlement, exclude from spend
    if "credit card" in subject and ("payment" in subject or "paid" in subject):
        return {"bank": "HDFC", "mode": "Credit Card Payment", "amount": amount or 0,
                "merchant_raw": "HDFC Credit Card", "merchant_canonical": "HDFC Credit Card",
                "category": "Credit Card Payment", "treatment": "settlement",
                "vpa": "", "person_name": "", "date": parse_date(msg),
                "msg_id": get_header(msg, "Message-ID") or msg.get("id", ""),
                "gmail_account": gmail_account, "ai_classified": False}

    if "upi" in subject:
        mode = "UPI"
        m = re.search(r"VPA\s+([\w.\-@]+)\s+on", body, re.IGNORECASE)
        if m: vpa = m.group(1)
        m2 = re.search(r"to\s+(.+?)\s+(?:on|via|from)", body, re.IGNORECASE)
        if m2: merchant = m2.group(1).strip()
        if "debited" not in body.lower() and "debit" not in subject: return None
    elif "credit card" in subject and "used" in body.lower():
        mode = "Credit Card"
        m = re.search(r"(?:at|towards)\s+(.+?)\s+(?:on|for)\s+\d", body, re.IGNORECASE)
        if m: merchant = m.group(1).strip()
    elif "nach" in body.lower() or "nachautoemailer" in sender:
        mode = "NACH"
        m = re.search(r"towards\s+(.+?)\s+with UMRN", body, re.IGNORECASE)
        if m: merchant = m.group(1).strip()
    elif "reversal" in subject:
        mode = "Reversal"
        return None  # Refunds — skip for now
    else:
        return None

    if not amount: return None

    canonical, category, treatment, person_name, ai_cls = resolve_merchant(merchant, vpa, amount)
    return {
        "bank": "HDFC", "mode": mode, "amount": amount,
        "merchant_raw": merchant, "merchant_canonical": canonical,
        "category": category, "treatment": treatment,
        "vpa": vpa, "person_name": person_name,
        "date": parse_date(msg),
        "msg_id": get_header(msg, "Message-ID") or msg.get("id", ""),
        "gmail_account": gmail_account, "ai_classified": ai_cls,
    }

def parse_hsbc(msg, gmail_account: str) -> dict:
    subject = get_header(msg, "Subject").lower()
    sender = get_header(msg, "From").lower()
    body = get_body(msg)
    amount = parse_amount(body)
    merchant, mode = "", ""

    if "purchase" in subject or "used" in body.lower():
        mode = "Credit Card"
        m = re.search(r"payment to\s+(.+?)\s+on\s+\d", body, re.IGNORECASE)
        if m: merchant = m.group(1).strip()
        if "used" not in body.lower(): return None
    elif "mandatehq" in sender:
        mode = "e-Mandate"
        m = re.search(r"Merchant\s+(.+?)[\n\r]", body)
        if m: merchant = m.group(1).strip()
    elif "credit card" in subject and "payment" in subject:
        return {"bank": "HSBC", "mode": "Credit Card Payment", "amount": amount or 0,
                "merchant_raw": "HSBC Credit Card", "merchant_canonical": "HSBC Credit Card",
                "category": "Credit Card Payment", "treatment": "settlement",
                "vpa": "", "person_name": "", "date": parse_date(msg),
                "msg_id": get_header(msg, "Message-ID") or msg.get("id", ""),
                "gmail_account": gmail_account, "ai_classified": False}
    else:
        return None

    if not amount: return None

    canonical, category, treatment, person_name, ai_cls = resolve_merchant(merchant, "", amount)
    return {
        "bank": "HSBC", "mode": mode, "amount": amount,
        "merchant_raw": merchant, "merchant_canonical": canonical,
        "category": category, "treatment": treatment,
        "vpa": "", "person_name": person_name,
        "date": parse_date(msg),
        "msg_id": get_header(msg, "Message-ID") or msg.get("id", ""),
        "gmail_account": gmail_account, "ai_classified": ai_cls,
    }

def sync_gmail_account(user_id: str, gmail_account: str, access_token: str, refresh_token: str) -> tuple:
    """
    Sync one Gmail account. Returns (new_transactions, banks_found).
    Uses 2-day buffer before last sync to catch missed emails.
    """
    last_sync = get_last_sync(user_id, gmail_account)

    if last_sync:
        # 2-day buffer before last sync to catch gaps
        after = (last_sync - timedelta(days=2)).strftime("%Y/%m/%d")
    else:
        after = (datetime.now() - timedelta(days=90)).strftime("%Y/%m/%d")

    # Build queries for all known bank senders
    queries = [f"from:{sender} after:{after}" for sender in BANK_SENDERS.keys()]

    current_token = access_token
    transactions = []
    banks_found = set()
    emails_processed = 0

    def safe_get(path, params=None):
        nonlocal current_token
        try:
            return gmail_get(path, current_token, params)
        except requests.HTTPError as e:
            if e.response.status_code in [401, 403]:
                # Rotate token
                current_token = refresh_access_token(refresh_token)
                update_access_token(user_id, gmail_account, current_token)
                return gmail_get(path, current_token, params)
            raise

    for query in queries:
        try:
            data = safe_get("messages", {"q": query, "maxResults": 200})
            for m in data.get("messages", []):
                try:
                    full = safe_get(f"messages/{m['id']}")
                    sender_raw = get_header(full, "From").lower()

                    # Identify bank
                    bank = None
                    for sender_key, bank_name in BANK_SENDERS.items():
                        if sender_key in sender_raw:
                            bank = bank_name
                            break

                    if not bank:
                        continue

                    banks_found.add(bank)

                    # Parse by bank
                    parsed = None
                    if bank == "HDFC":
                        parsed = parse_hdfc(full, gmail_account)
                    elif bank == "HSBC":
                        parsed = parse_hsbc(full, gmail_account)
                    else:
                        # AI parsing for all other banks
                        parsed = parse_with_ai(full, bank, gmail_account)

                    if parsed and parsed.get("amount", 0) > 0:
                        transactions.append(parsed)

                    emails_processed += 1
                except:
                    continue
        except:
            continue

    if transactions:
        save_transactions(user_id, transactions)

    update_sync_log(user_id, gmail_account, emails_processed, len(transactions))
    return len(transactions), banks_found

def sync_all_gmail(user_id: str, gmail_accounts: list) -> dict:
    """Sync all Gmail accounts for a user."""
    total_new = 0
    all_banks = set()

    for account in gmail_accounts:
        try:
            new_txns, banks = sync_gmail_account(
                user_id,
                account["email"],
                account["access_token"],
                account["refresh_token"]
            )
            total_new += new_txns
            all_banks.update(banks)
        except Exception as e:
            pass  # Continue with other accounts

    return {"new_transactions": total_new, "banks_found": list(all_banks)}

def get_transactions(user_id: str, gmail_accounts: list, days: int = 30,
                     start_date=None, end_date=None) -> list:
    """Main entry point — sync then return from DB."""
    sync_all_gmail(user_id, gmail_accounts)
    from db import get_transactions_from_db
    return get_transactions_from_db(user_id, days=days, start_date=start_date, end_date=end_date)
