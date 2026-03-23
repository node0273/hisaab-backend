"""
Gmail Reader v4 — Smart, rule-based first, AI only for unknowns
Architecture:
1. Search Gmail for all bank emails (known senders + broad keywords)
2. Identify bank from sender (rule-based, instant)
3. Extract transaction details (rule-based for known banks, AI for others)
4. Resolve merchant from VPA/name (rule-based first, AI only for unknowns)
5. Store everything — never re-process same email
"""
import base64, re, os, requests, json
from datetime import datetime, timedelta
from bank_config import BANK_SENDERS, BANK_KEYWORDS, MODE_KEYWORDS, VPA_MERCHANT_MAP, NACH_MERCHANT_MAP
from db import get_last_sync, update_sync_log, save_transactions, update_access_token, get_conn
from merchant_resolver import resolve_merchant

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# ── Gmail API helpers ─────────────────────────────────────────

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
    return (body + " " + msg.get("snippet", ""))[:2500]

def extract_sender_email(from_header: str) -> str:
    m = re.search(r'<([^>]+)>', from_header)
    return (m.group(1) if m else from_header).lower().strip()

def parse_email_date(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S %Z", "%d %b %Y %H:%M:%S %Z"]:
        try:
            return datetime.strptime(date_str[:35].strip(), fmt).date()
        except:
            pass
    return datetime.now().date()

# ── Bank identification ───────────────────────────────────────

def identify_bank(sender_email: str, subject: str, body: str) -> str:
    """Identify bank from sender email, then subject/body keywords."""
    # 1. Exact sender match
    if sender_email in BANK_SENDERS:
        return BANK_SENDERS[sender_email]

    # 2. Partial sender match
    for known_sender, bank in BANK_SENDERS.items():
        if known_sender in sender_email:
            return bank

    # 3. Subject/body keyword match
    text = (subject + " " + body[:500]).lower()
    for bank, keywords in BANK_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return bank

    return None

# ── Rule-based transaction extraction ─────────────────────────

def extract_amount(text: str) -> float:
    """Extract rupee amount from text."""
    patterns = [
        r'(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
        r'([\d,]+\.?\d*)\s*(?:rupees|rs\.?)',
        r'amount[:\s]+(?:Rs\.?|INR|₹)?\s*([\d,]+\.?\d*)',
        r'debited.*?(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except:
                pass
    return None

def detect_mode(subject: str, body: str) -> str:
    """Detect transaction mode from email content."""
    text = (subject + " " + body[:500]).lower()
    for mode, keywords in MODE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return mode
    return "Unknown"

def is_debit(subject: str, body: str) -> bool:
    """Check if transaction is a debit (money going out)."""
    text = (subject + " " + body[:500]).lower()
    debit_keywords = ["debited", "debit", "spent", "payment of", "paid", "purchase", "withdrawn", "transaction of"]
    credit_keywords = ["credited", "credit", "received", "refund", "cashback", "salary", "deposit"]
    
    debit_score = sum(1 for kw in debit_keywords if kw in text)
    credit_score = sum(1 for kw in credit_keywords if kw in text)
    
    # If more credit keywords, it's not a debit
    if credit_score > debit_score:
        return False
    return True

def extract_vpa(text: str) -> str:
    """Extract UPI VPA from text."""
    patterns = [
        r'VPA\s+([\w.\-@]+)',
        r'to\s+([\w.\-]+@[\w.\-]+)',
        r'UPI\s+ID[:\s]+([\w.\-@]+)',
        r'([\w.\-]+@(?:okaxis|okhdfcbank|okicici|oksbi|paytm|gpay|ybl|axl|pty|ptyes|ibl|digikhata|jiopay|hdfcbank|icici|sbi|axisbank))',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip().lower()
    return ""

def resolve_vpa_merchant(vpa: str) -> tuple:
    """
    Resolve VPA to merchant name and category.
    Returns (canonical, category, treatment, person_name) or None
    """
    if not vpa:
        return None
    
    handle = vpa.split("@")[0].lower()
    handle_clean = re.sub(r'[^a-z0-9]', '', handle)
    
    # Check VPA_MERCHANT_MAP
    for keyword, result in VPA_MERCHANT_MAP.items():
        if keyword in handle_clean or keyword in handle:
            if result is None:
                # Payment app — need further resolution
                return None
            return result[0], result[1], result[2], ""
    
    # Person VPA detection
    is_person = bool(
        re.match(r'^\d{10}$', handle) or  # Phone number
        re.match(r'^[a-z]+[.\-][a-z]+\d{0,4}$', handle)  # firstname.lastname
    )
    
    if is_person:
        # Extract person name
        name = re.sub(r'[._\-]', ' ', handle)
        name = re.sub(r'\d+$', '', name).strip().title()
        if re.match(r'^\d{10}$', handle):
            name = f"****{handle[-4:]}"
        return None, None, None, name  # Signal: person VPA
    
    return None

def extract_nach_merchant(body: str) -> tuple:
    """Extract merchant from NACH/mandate email."""
    body_lower = body.lower()
    
    for keyword, result in NACH_MERCHANT_MAP.items():
        if keyword in body_lower:
            return result[0], result[1], result[2]
    
    # Try to extract company name
    patterns = [
        r'towards\s+([A-Z][A-Za-z\s]+?)\s+(?:with|for|on|dated)',
        r'mandate.*?for\s+([A-Z][A-Za-z\s]+?)\s*(?:\n|with|for)',
        r'company[:\s]+([A-Z][A-Za-z\s]+?)[\n,]',
    ]
    for pattern in patterns:
        m = re.search(pattern, body, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            return name, "Other", "spend"
    
    return "NACH Payment", "Other", "spend"

def rule_based_extract(subject: str, body: str, bank: str) -> dict:
    """
    Rule-based extraction — handles 80% of transactions.
    Returns dict or None.
    """
    text = subject + " " + body
    
    # Skip non-transaction emails
    skip_keywords = [
        "otp", "one time password", "login", "sign in", "statement",
        "account opening", "welcome", "kyc", "update your", "verify",
        "reminder", "due date", "minimum due", "bill generated",
        "credit limit", "reward points", "offers", "cashback earned",
        "balance is", "available balance", "account balance"
    ]
    text_lower = text.lower()
    if any(kw in text_lower for kw in skip_keywords):
        return None
    
    # Must be a debit
    if not is_debit(subject, body):
        return None
    
    amount = extract_amount(text)
    if not amount or amount <= 0:
        return None
    
    mode = detect_mode(subject, body)
    vpa = ""
    merchant_raw = ""
    merchant_canonical = "Unknown"
    category = "Other"
    treatment = "spend"
    person_name = ""
    
    if mode == "UPI":
        vpa = extract_vpa(text)
        result = resolve_vpa_merchant(vpa) if vpa else None
        
        if result and result[0] is not None:
            # Business VPA resolved
            merchant_canonical, category, treatment, person_name = result
            merchant_raw = vpa
        elif result and result[0] is None and result[3]:
            # Person VPA
            person_name = result[3]
            merchant_canonical = f"P2P - {person_name}"
            category = "Daily Spend" if amount < 500 else "P2P Transfer"
            treatment = "spend"
            merchant_raw = vpa
        else:
            # Unknown VPA — use full VPA as raw name for AI resolution
            merchant_raw = vpa
            merchant_canonical = None  # Signal AI needed
    
    elif mode == "Credit Card":
        # Extract merchant from "at MERCHANT_NAME on DATE" pattern
        patterns = [
            r'(?:at|towards|for)\s+([A-Z][A-Za-z0-9\s\*\-\.]+?)\s+(?:on|for|dated|\d)',
            r'purchase.*?at\s+([A-Z][A-Za-z0-9\s\*\-\.]+)',
            r'spent.*?at\s+([A-Z][A-Za-z0-9\s\*\-\.]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                merchant_raw = m.group(1).strip()
                break
        merchant_canonical = None  # Will be resolved by merchant_resolver
    
    elif mode == "NACH":
        merchant_raw, category, treatment = extract_nach_merchant(body)
        merchant_canonical = merchant_raw
    
    elif mode == "Debit Card":
        patterns = [
            r'(?:at|pos)\s+([A-Z][A-Za-z0-9\s\*\-\.]+?)\s+(?:on|for|\d)',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                merchant_raw = m.group(1).strip()
                break
        merchant_canonical = None
    
    return {
        "amount": amount,
        "mode": mode,
        "vpa": vpa,
        "merchant_raw": merchant_raw,
        "merchant_canonical": merchant_canonical,
        "category": category,
        "treatment": treatment,
        "person_name": person_name,
        "needs_ai": merchant_canonical is None,
    }

def ai_extract_transaction(subject: str, body: str, bank: str) -> dict:
    """AI extraction — only called when rule-based fails."""
    if not ANTHROPIC_API_KEY:
        return None
    
    prompt = f"""Extract transaction from this {bank} bank alert email.

Subject: {subject}
Body: {body[:1200]}

Rules:
- Only extract DEBIT transactions (money going OUT)
- Skip: OTP, login, balance alerts, statements, welcome emails
- For UPI: extract the full VPA handle (e.g. zomato@icici)
- Amount must be positive number

Return ONLY valid JSON or null:
{{"amount": 1234.56, "merchant": "name or VPA", "vpa": "vpa@bank or empty", "mode": "UPI/Credit Card/NACH/Debit Card/Net Banking", "is_debit": true}}"""

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
        if text.lower() == 'null':
            return None
        data = json.loads(text)
        if not data.get("is_debit") or not data.get("amount"):
            return None
        return data
    except Exception as e:
        print(f"AI extraction error: {str(e)[:100]}")
        return None

# ── Learned senders ───────────────────────────────────────────

def ensure_learned_senders_table():
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
                conn.commit()
    except:
        pass

def get_learned_senders() -> dict:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sender_email, bank_name FROM learned_senders WHERE is_active = TRUE")
                return {row[0]: row[1] for row in cur.fetchall()}
    except:
        return {}

def save_learned_sender(sender_email: str, bank_name: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO learned_senders (sender_email, bank_name)
                    VALUES (%s, %s) ON CONFLICT (sender_email) DO NOTHING
                """, (sender_email, bank_name))
                conn.commit()
        print(f"Learned sender: {sender_email} → {bank_name}")
    except:
        pass

# ── Main sync ─────────────────────────────────────────────────

def sync_gmail_account(user_id: str, gmail_email: str, access_token: str, refresh_token: str) -> tuple:
    """Sync one Gmail account. Returns (new_transactions, banks_found)."""
    
    ensure_learned_senders_table()
    last_sync = get_last_sync(user_id, gmail_email)
    
    if last_sync:
        after = (last_sync - timedelta(days=2)).strftime("%Y/%m/%d")
    else:
        after = (datetime.now() - timedelta(days=90)).strftime("%Y/%m/%d")
    
    learned_senders = get_learned_senders()
    all_senders = {**BANK_SENDERS, **learned_senders}
    
    # Build Gmail search queries
    # Query 1: All known bank senders (fast, targeted)
    sender_list = " OR ".join([f"from:{s}" for s in list(all_senders.keys())[:50]])
    # Query 2: Broad debit/transaction keywords (catches unknown senders)
    broad_query = f"(debited OR \"UPI transaction\" OR \"transaction alert\" OR \"spent on\") after:{after} -from:me"
    
    queries = [
        f"({sender_list}) after:{after}",
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
    ai_calls = 0
    
    for query in queries:
        try:
            data = safe_get("messages", {"q": query, "maxResults": 200})
            for m in data.get("messages", []):
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
                    
                    # Step 1: Identify bank
                    bank = identify_bank(sender_email, subject, body)
                    
                    # Step 2: If unknown sender, try AI bank identification (once per sender)
                    if not bank and sender_email not in learned_senders:
                        if ai_calls < 20:  # Limit AI calls per sync
                            bank = ai_identify_bank(subject, body[:300], sender_email)
                            ai_calls += 1
                            if bank:
                                save_learned_sender(sender_email, bank)
                                learned_senders[sender_email] = bank
                    
                    if not bank:
                        emails_processed += 1
                        continue
                    
                    banks_found.add(bank)
                    
                    # Step 3: Rule-based extraction first
                    extracted = rule_based_extract(subject, body, bank)
                    
                    # Step 4: AI extraction only if rule-based fails
                    if not extracted and ai_calls < 50:
                        ai_data = ai_extract_transaction(subject, body, bank)
                        ai_calls += 1
                        if ai_data:
                            extracted = {
                                "amount": float(ai_data["amount"]),
                                "mode": ai_data.get("mode", "Unknown"),
                                "vpa": ai_data.get("vpa", ""),
                                "merchant_raw": ai_data.get("merchant", ""),
                                "merchant_canonical": None,
                                "category": "Other",
                                "treatment": "spend",
                                "person_name": "",
                                "needs_ai": True,
                            }
                    
                    if not extracted:
                        emails_processed += 1
                        continue
                    
                    amount = extracted["amount"]
                    merchant_raw = extracted.get("merchant_raw", "")
                    vpa = extracted.get("vpa", "")
                    
                    # Step 5: Resolve merchant (rule-based first, AI if needed)
                    if extracted.get("merchant_canonical"):
                        # Already resolved by rule-based
                        canonical = extracted["merchant_canonical"]
                        category = extracted["category"]
                        treatment = extracted["treatment"]
                        person_name = extracted.get("person_name", "")
                        ai_cls = False
                    else:
                        # Use merchant_resolver (handles DB lookup + AI)
                        canonical, category, treatment, person_name, ai_cls = resolve_merchant(
                            merchant_raw, vpa, amount
                        )
                        if ai_cls:
                            ai_calls += 1
                    
                    transactions.append({
                        "bank": bank,
                        "mode": extracted["mode"],
                        "amount": amount,
                        "merchant_raw": merchant_raw or vpa,
                        "merchant_canonical": canonical,
                        "category": category,
                        "treatment": treatment,
                        "vpa": vpa,
                        "person_name": person_name,
                        "date": parse_email_date(full),
                        "msg_id": msg_id,
                        "gmail_account": gmail_email,
                        "ai_classified": ai_cls,
                    })
                    emails_processed += 1
                    
                except Exception as e:
                    print(f"Email processing error: {str(e)[:100]}")
                    emails_processed += 1
                    continue
                    
        except Exception as e:
            print(f"Query error: {str(e)[:100]}")
            continue
    
    print(f"Sync done: {emails_processed} emails, {len(transactions)} transactions, {ai_calls} AI calls")
    
    if transactions:
        save_transactions(user_id, transactions)
    
    update_sync_log(user_id, gmail_email, emails_processed, len(transactions))
    return len(transactions), banks_found

def ai_identify_bank(subject: str, body: str, sender: str) -> str:
    """AI bank identification — only for unknown senders."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 20,
                "messages": [{"role": "user", "content": f"Is this a bank transaction alert email? From: {sender}\nSubject: {subject}\nBody: {body}\n\nReturn ONLY bank name (HDFC/ICICI/SBI/Axis/Kotak/HSBC/etc) or null"}],
            },
            timeout=5
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        return None if text.lower() == "null" else text
    except:
        return None

def sync_all_gmail(user_id: str, gmail_accounts: list) -> dict:
    total_new = 0
    all_banks = set()
    for account in gmail_accounts:
        try:
            new_txns, banks = sync_gmail_account(
                user_id, account["email"],
                account["access_token"], account["refresh_token"]
            )
            total_new += new_txns
            all_banks.update(banks)
        except Exception as e:
            print(f"Sync error for {account.get('email','?')}: {str(e)[:100]}")
    return {"new_transactions": total_new, "banks_found": list(all_banks)}

def get_transactions(user_id: str, gmail_accounts: list, days: int = 30,
                     start_date=None, end_date=None) -> list:
    sync_all_gmail(user_id, gmail_accounts)
    from db import get_transactions_from_db
    return get_transactions_from_db(user_id, days=days, start_date=start_date, end_date=end_date)
