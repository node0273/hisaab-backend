"""
Gmail Reader v3 — Smart approach
- Broad search for ANY bank transaction email
- AI filters and extracts transaction details
- Learns sender addresses automatically
- No hardcoded sender list needed
"""
import base64, re, os, requests, json
from datetime import datetime, timedelta
from db import get_last_sync, update_sync_log, save_transactions, update_access_token, get_conn
from security import sanitise_log

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# ── Known senders (fast lookup, grows automatically) ──────────
KNOWN_SENDERS = {
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
    "hsbc@mail.hsbc.co.in":         "HSBC",
    "hsbc@mandatehq.com":           "HSBC",
    "alerts@federalbank.co.in":     "Federal Bank",
    "alerts@pnb.co.in":             "PNB",
    "alerts@bankofbaroda.com":      "Bank of Baroda",
    "alerts@rblbank.com":           "RBL",
    "alerts@canarabank.in":         "Canara Bank",
}

# ── Gmail helpers ──────────────────────────────────────────────

def refresh_access_token(refresh_token: str) -> str:
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "refresh_token": refresh_token,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "grant_type": "refresh_token",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def gmail_get(path, access_token, params=None):
    resp = requests.get(
        f"{GMAIL_API}/{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params or {},
        timeout=15
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
    # Combine body + snippet for best coverage
    return (body + " " + msg.get("snippet", ""))[:2000]

def parse_date_from_msg(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S %Z", "%d %b %Y %H:%M:%S %Z"]:
        try:
            return datetime.strptime(date_str[:35].strip(), fmt).date()
        except:
            pass
    return datetime.now().date()

def extract_sender_email(from_header: str) -> str:
    """Extract clean email from From header like 'HDFC Bank <alerts@hdfcbank.net>'"""
    m = re.search(r'<([^>]+)>', from_header)
    if m:
        return m.group(1).lower().strip()
    return from_header.lower().strip()

# ── Learned senders DB ─────────────────────────────────────────

def get_learned_senders() -> dict:
    """Load sender addresses learned from previous syncs."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sender_email, bank_name FROM learned_senders
                    WHERE is_active = TRUE
                """)
                return {row[0]: row[1] for row in cur.fetchall()}
    except:
        return {}

def save_learned_sender(sender_email: str, bank_name: str):
    """Save a newly discovered bank sender."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS learned_senders (
                        id SERIAL PRIMARY KEY,
                        sender_email VARCHAR(255) UNIQUE NOT NULL,
                        bank_name VARCHAR(100) NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        discovered_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO learned_senders (sender_email, bank_name)
                    VALUES (%s, %s)
                    ON CONFLICT (sender_email) DO NOTHING
                """, (sender_email, bank_name))
                conn.commit()
    except:
        pass

def identify_bank(sender_email: str, learned: dict) -> str:
    """Identify bank from sender — checks hardcoded + learned senders."""
    # Check known senders
    for known_sender, bank in KNOWN_SENDERS.items():
        if known_sender in sender_email or sender_email in known_sender:
            return bank
    # Check learned senders
    if sender_email in learned:
        return learned[sender_email]
    return None

# ── AI extraction ──────────────────────────────────────────────

def ai_extract_transaction(subject: str, body: str, sender: str, bank: str) -> dict:
    """
    Use Claude Haiku to extract transaction from email.
    Returns dict or None if not a debit transaction.
    """
    if not ANTHROPIC_API_KEY:
        return None

    prompt = f"""Extract transaction details from this bank alert email.

Bank: {bank}
From: {sender}
Subject: {subject}
Body: {body[:1500]}

Rules:
- Only extract DEBIT transactions (money going OUT from account)
- Skip: credit/refund/OTP/login/balance/statement/welcome emails
- For UPI: extract the VPA (like name@upi or phone@paytm)
- For credit card: extract merchant name
- Amount must be a positive number

Return ONLY valid JSON or the word null:
{{"amount": 1234.56, "merchant": "merchant or person name", "vpa": "upi@handle or empty string", "mode": "UPI/Credit Card/NACH/Debit Card/Net Banking", "is_debit": true}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=8
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        text = re.sub(r'```json|```', '', text).strip()
        if text.lower() == 'null' or not text:
            return None
        data = json.loads(text)
        if not data.get("is_debit") or not data.get("amount"):
            return None
        return data
    except Exception as e:
        print(f"AI extraction error: {str(e)[:100]}")
        return None

def ai_identify_bank_sender(subject: str, body: str, sender: str) -> str:
    """Use AI to identify if this is a bank transaction email and which bank."""
    if not ANTHROPIC_API_KEY:
        return None

    prompt = f"""Is this a bank transaction alert email from an Indian bank?

From: {sender}
Subject: {subject}
Snippet: {body[:300]}

If yes, return ONLY the bank name (e.g. "HDFC", "ICICI", "SBI", "Axis", "Kotak", "Yes Bank", "HSBC", "IDFC First", "IndusInd", "Federal Bank", "PNB", "Bank of Baroda", "RBL", "Canara Bank").
If no, return ONLY the word "null"."""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 20,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=5
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if text.lower() == 'null' or not text:
            return None
        return text
    except:
        return None

# ── Main sync ──────────────────────────────────────────────────

def sync_gmail_account(user_id: str, gmail_email: str, access_token: str, refresh_token: str) -> tuple:
    """
    Smart sync for one Gmail account.
    Returns (new_transactions_count, banks_found_set)
    """
    last_sync = get_last_sync(user_id, gmail_email)

    if last_sync:
        # 2-day buffer to catch missed emails
        after = (last_sync - timedelta(days=2)).strftime("%Y/%m/%d")
    else:
        # First sync — go back 90 days
        after = (datetime.now() - timedelta(days=90)).strftime("%Y/%m/%d")

    # Load learned senders
    learned_senders = get_learned_senders()
    all_senders = {**KNOWN_SENDERS, **learned_senders}

    # Build search queries
    # Query 1: Known sender addresses (fast, cheap)
    known_sender_query = " OR ".join([f"from:{s}" for s in all_senders.keys()])
    # Query 2: Broad transaction keyword search (catches unknown senders)
    broad_query = f"(debited OR \"UPI transaction\" OR \"credit card\" OR \"spent\" OR \"transaction alert\") after:{after}"

    queries = [
        f"({known_sender_query}) after:{after}",
        broad_query,
    ]

    current_token = access_token

    def safe_get(path, params=None):
        nonlocal current_token
        try:
            return gmail_get(path, current_token, params)
        except requests.HTTPError as e:
            if e.response.status_code in [401, 403]:
                current_token = refresh_access_token(refresh_token)
                update_access_token(user_id, gmail_email, current_token)
                return gmail_get(path, current_token, params)
            raise

    transactions = []
    banks_found = set()
    emails_processed = 0
    seen_msg_ids = set()

    for query in queries:
        try:
            data = safe_get("messages", {"q": query, "maxResults": 200})
            messages = data.get("messages", [])

            for m in messages:
                if m['id'] in seen_msg_ids:
                    continue
                seen_msg_ids.add(m['id'])

                try:
                    full = safe_get(f"messages/{m['id']}")
                    sender_raw = get_header(full, "From")
                    sender_email = extract_sender_email(sender_raw)
                    subject = get_header(full, "Subject")
                    body = get_body(full)
                    msg_id = get_header(full, "Message-ID") or m['id']

                    # Step 1: Try to identify bank from known/learned senders
                    bank = identify_bank(sender_email, learned_senders)

                    # Step 2: If unknown sender, use AI to identify bank
                    if not bank:
                        bank = ai_identify_bank_sender(subject, body, sender_email)
                        if bank:
                            # Save this sender for future fast lookups
                            save_learned_sender(sender_email, bank)
                            learned_senders[sender_email] = bank
                            print(f"Learned new sender: {sender_email} → {bank}")

                    if not bank:
                        emails_processed += 1
                        continue

                    banks_found.add(bank)

                    # Step 3: Extract transaction with AI
                    extracted = ai_extract_transaction(subject, body, sender_email, bank)
                    if not extracted:
                        emails_processed += 1
                        continue

                    amount = float(extracted["amount"])
                    merchant_raw = extracted.get("merchant", "")
                    vpa = extracted.get("vpa", "")
                    mode = extracted.get("mode", "Unknown")

                    # Step 4: Resolve merchant
                    from merchant_resolver import resolve_merchant
                    canonical, category, treatment, person_name, ai_cls = resolve_merchant(
                        merchant_raw, vpa, amount
                    )

                    transactions.append({
                        "bank": bank,
                        "mode": mode,
                        "amount": amount,
                        "merchant_raw": merchant_raw,
                        "merchant_canonical": canonical,
                        "category": category,
                        "treatment": treatment,
                        "vpa": vpa,
                        "person_name": person_name,
                        "date": parse_date_from_msg(full),
                        "msg_id": msg_id,
                        "gmail_account": gmail_email,
                        "ai_classified": ai_cls,
                    })
                    emails_processed += 1

                except Exception as e:
                    print(f"Error processing email: {str(e)[:100]}")
                    emails_processed += 1
                    continue

        except Exception as e:
            print(f"Query error: {str(e)[:100]}")
            continue

    if transactions:
        save_transactions(user_id, transactions)

    update_sync_log(user_id, gmail_email, emails_processed, len(transactions))
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
            print(f"Sync error for {account.get('email', '?')}: {str(e)[:100]}")
            continue

    return {"new_transactions": total_new, "banks_found": list(all_banks)}


def get_transactions(user_id: str, gmail_accounts: list, days: int = 30,
                     start_date=None, end_date=None) -> list:
    """Main entry point — sync then return from DB."""
    sync_all_gmail(user_id, gmail_accounts)
    from db import get_transactions_from_db
    return get_transactions_from_db(user_id, days=days, start_date=start_date, end_date=end_date)
