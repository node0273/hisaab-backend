"""
Gmail Reader v5 — Fully DB-driven, no hardcoding
Architecture:
1. Read ALL emails broadly (no hardcoded sender list)
2. Check negative_rules table → skip promotional/spam senders forever
3. Check bank_senders table → identify bank (known senders)
4. If unknown sender → AI decides: bank transaction / promotional / irrelevant
   - Bank transaction → extract details, save sender to bank_senders
   - Promotional → save to negative_rules, never read again
   - Irrelevant → save to negative_rules
5. Extract transaction: rule-based first (vpa_rules, nach_rules), AI if no rule
6. New rule learned → saved to DB permanently
"""
import base64, re, os, requests, json
from datetime import datetime, timedelta
from db import get_last_sync, update_sync_log, save_transactions, update_access_token, get_conn

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# ── Gmail helpers ─────────────────────────────────────────────

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
    return (body + " " + msg.get("snippet", ""))[:3000]

def extract_sender_email(from_header: str) -> str:
    m = re.search(r'<([^>]+)>', from_header)
    return (m.group(1) if m else from_header).lower().strip()

def parse_email_date(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S %Z"]:
        try:
            return datetime.strptime(date_str[:35].strip(), fmt).date()
        except:
            pass
    return datetime.now().date()

# ── DB helpers for rules ──────────────────────────────────────

def ensure_tables():
    """Ensure all required tables exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bank_senders (
                    id SERIAL PRIMARY KEY,
                    sender_email VARCHAR(255) UNIQUE NOT NULL,
                    bank_name VARCHAR(100) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    source VARCHAR(20) DEFAULT 'ai',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS negative_rules (
                    id SERIAL PRIMARY KEY,
                    sender_email VARCHAR(255) UNIQUE NOT NULL,
                    reason VARCHAR(255),
                    source VARCHAR(20) DEFAULT 'ai',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS vpa_rules (
                    id SERIAL PRIMARY KEY,
                    keyword VARCHAR(255) UNIQUE NOT NULL,
                    merchant_canonical VARCHAR(255) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    treatment VARCHAR(20) DEFAULT 'spend',
                    is_active BOOLEAN DEFAULT TRUE,
                    source VARCHAR(20) DEFAULT 'manual',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS nach_rules (
                    id SERIAL PRIMARY KEY,
                    keyword VARCHAR(255) UNIQUE NOT NULL,
                    merchant_canonical VARCHAR(255) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    treatment VARCHAR(20) DEFAULT 'spend',
                    is_active BOOLEAN DEFAULT TRUE,
                    source VARCHAR(20) DEFAULT 'manual',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS learned_senders (
                    id SERIAL PRIMARY KEY,
                    sender_email VARCHAR(255) UNIQUE NOT NULL,
                    bank_name VARCHAR(100) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    discovered_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()

def get_all_bank_senders() -> dict:
    """Load all known bank senders from DB."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sender_email, bank_name FROM bank_senders WHERE is_active = TRUE")
                return {r[0]: r[1] for r in cur.fetchall()}
    except:
        return {}

def get_negative_senders() -> set:
    """Load all promotional/spam senders to skip."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sender_email FROM negative_rules")
                return {r[0] for r in cur.fetchall()}
    except:
        return set()

def save_bank_sender(sender_email: str, bank_name: str, source: str = 'ai'):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bank_senders (sender_email, bank_name, source)
                    VALUES (%s, %s, %s) ON CONFLICT (sender_email) DO NOTHING
                """, (sender_email, bank_name, source))
                conn.commit()
        print(f"Learned bank sender: {sender_email} → {bank_name}")
    except Exception as e:
        print(f"save_bank_sender error: {e}")

def save_negative_rule(sender_email: str, reason: str, source: str = 'ai'):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO negative_rules (sender_email, reason, source)
                    VALUES (%s, %s, %s) ON CONFLICT (sender_email) DO NOTHING
                """, (sender_email, reason, source))
                conn.commit()
        print(f"Negative rule saved: {sender_email} → {reason}")
    except Exception as e:
        print(f"save_negative_rule error: {e}")

def seed_bank_senders():
    """Seed DB with known bank senders if empty."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bank_senders")
            if cur.fetchone()[0] > 0:
                return

    # Seed from bank_config fallback
    try:
        from bank_config import BANK_SENDERS
        with get_conn() as conn:
            with conn.cursor() as cur:
                for email, bank in BANK_SENDERS.items():
                    cur.execute("""
                        INSERT INTO bank_senders (sender_email, bank_name, source)
                        VALUES (%s, %s, 'seed') ON CONFLICT DO NOTHING
                    """, (email, bank))
                conn.commit()
        print(f"Seeded {len(BANK_SENDERS)} bank senders from config")
    except Exception as e:
        print(f"Seed error: {e}")

def get_merchant_rules() -> dict:
    """Load all merchant rules from unified merchant_rules table."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT keyword, merchant_canonical, category, treatment, applies_to
                    FROM merchant_rules WHERE is_active = TRUE
                """)
                return {r[0]: (r[1], r[2], r[3], r[4]) for r in cur.fetchall()}
    except:
        return {}

def seed_merchant_rules():
    """Seed merchant_rules from bank_config if empty."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM merchant_rules")
                if cur.fetchone()[0] > 0:
                    return
        from bank_config import VPA_MERCHANT_MAP, NACH_MERCHANT_MAP
        with get_conn() as conn:
            with conn.cursor() as cur:
                for kw, result in VPA_MERCHANT_MAP.items():
                    if result:
                        cur.execute("""
                            INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
                            VALUES (%s, %s, %s, %s, '{upi,all}', 'seed') ON CONFLICT DO NOTHING
                        """, (kw, result[0], result[1], result[2]))
                for kw, result in NACH_MERCHANT_MAP.items():
                    cur.execute("""
                        INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
                        VALUES (%s, %s, %s, %s, '{nach}', 'seed') ON CONFLICT DO NOTHING
                    """, (kw, result[0], result[1], result[2]))
                conn.commit()
        print("Seeded merchant_rules from bank_config")
    except Exception as e:
        print(f"Seed merchant_rules error: {e}")

def save_merchant_rule(keyword: str, merchant: str, category: str, treatment: str, mode: str = 'all'):
    """Save a newly learned merchant rule to DB."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
                    VALUES (%s, %s, %s, %s, %s, 'ai') ON CONFLICT (keyword) DO NOTHING
                """, (keyword.lower(), merchant, category, treatment, [mode]))
                conn.commit()
        print(f"New merchant rule learned: {keyword} → {merchant} ({category}) [{mode}]")
    except:
        pass

# ── Amount extraction ─────────────────────────────────────────

def extract_amount(text: str) -> float:
    """Extract amount — handles Rs., INR, ₹ formats."""
    patterns = [
        r'(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
        r'for\s+INR\s+([\d,]+\.?\d*)',
        r'([\d,]+\.?\d*)\s*(?:rupees|rs\.?)',
        r'amount[:\s]+(?:Rs\.?|INR|₹)?\s*([\d,]+\.?\d*)',
        r'debited.*?(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
        r'(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)\s+(?:has been|was)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 0:
                    return val
            except:
                pass
    return None

def extract_vpa(text: str) -> str:
    """Extract UPI VPA handle."""
    patterns = [
        r'VPA\s+([\w.\-@]+)',
        r'to\s+([\w.\-]+@[\w.\-]+)',
        r'UPI\s+ID[:\s]+([\w.\-@]+)',
        r'([\w.\-]+@(?:okaxis|okhdfcbank|okicici|oksbi|paytm|gpay|ybl|axl|pty|ptyes|ibl|digikhata|jiopay|hdfcbank|icici|sbi|axisbank|kotak|indus|federal|rbl|yesbank))',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip().lower()
    return ""

def clean_merchant(name: str) -> str:
    """Strip payment gateway prefixes: PYU*ZOMATO → ZOMATO."""
    if not name:
        return name
    name = re.sub(r'^[A-Z]{2,4}\*', '', name).strip()
    name = re.sub(r'\s+[A-Z]{2,}\s+IN$', '', name).strip()
    return name

def is_debit(subject: str, body: str) -> bool:
    text = (subject + " " + body[:500]).lower()
    credits = ["credited", "credit", "received", "refund", "cashback", "salary",
               "otp", "one time password", "login", "sign in", "statement",
               "balance is", "available balance", "reward points", "offer",
               "due date", "minimum due", "bill generated", "welcome"]
    debits = ["debited", "debit", "spent", "payment of", "paid", "purchase",
              "withdrawn", "transaction of", "used for", "has been used",
              "has been debited", "amount of", "inr ", "rs.", "₹",
              "transaction alert", "card has been used"]
    c = sum(1 for kw in credits if kw in text)
    d = sum(1 for kw in debits if kw in text)
    return d > 0 and d >= c

def detect_mode(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["upi", "vpa", "bhim", "gpay", "phonepe", "paytm upi"]): return "UPI"
    if any(k in t for k in ["credit card", " cc ", "visa", "mastercard", "rupay", "amex"]): return "Credit Card"
    if any(k in t for k in ["debit card", "atm", " pos "]): return "Debit Card"
    if any(k in t for k in ["nach", "ach", "mandate", "ecs", "auto debit"]): return "NACH"
    if any(k in t for k in ["neft", "rtgs", "imps", "net banking"]): return "Net Banking"
    return "Unknown"

# ── VPA merchant resolution ───────────────────────────────────

def resolve_merchant_name(name: str, mode: str, amount: float, merchant_rules: dict) -> tuple:
    """
    Resolve any merchant name/VPA using unified merchant_rules.
    Works for UPI VPA handles, CC merchant names, NACH company names.
    Returns (canonical, category, treatment, person_name) or None
    """
    if not name:
        return None

    name_clean = re.sub(r'[^a-z0-9]', '', name.lower())
    name_lower = name.lower()

    # Check merchant_rules — match keyword against name
    for keyword, result in merchant_rules.items():
        canonical, category, treatment, applies_to = result
        # Check if this rule applies to this mode
        if applies_to and 'all' not in applies_to and mode not in applies_to:
            continue
        kw_clean = re.sub(r'[^a-z0-9]', '', keyword.lower())
        if kw_clean in name_clean or keyword.lower() in name_lower:
            return canonical, category, treatment, ""

    # Person detection for UPI VPAs
    if mode == "upi":
        handle = name.split("@")[0].lower() if "@" in name else name.lower()
        is_phone = bool(re.match(r'^\d{10}$', handle))
        is_name = bool(re.match(r'^[a-z]+[.\-][a-z]+\d{0,4}$', handle))
        if is_phone or is_name:
            person = f"****{handle[-4:]}" if is_phone else re.sub(r'[._\-]', ' ', handle).title()
            cat = "Daily Spend" if amount < 500 else "P2P Transfer"
            return f"P2P - {person}", cat, "spend", person

    return None

# Keep resolve_vpa as alias for backward compatibility
def resolve_vpa(vpa: str, amount: float, vpa_rules: dict) -> tuple:
    return resolve_merchant_name(vpa, "upi", amount, vpa_rules)

def resolve_nach(body: str, merchant_rules: dict) -> tuple:
    """Resolve NACH merchant using unified merchant_rules."""
    body_lower = body.lower()
    for keyword, result in merchant_rules.items():
        canonical, category, treatment, applies_to = result
        if applies_to and 'all' not in applies_to and 'nach' not in applies_to:
            continue
        if keyword.lower() in body_lower:
            return canonical, category, treatment
    m = re.search(r'towards\s+([A-Z][A-Za-z\s]+?)\s+(?:with|for|on|dated)', body, re.IGNORECASE)
    if m:
        return m.group(1).strip(), "Other", "spend"
    return "NACH Payment", "Other", "spend"

# ── AI calls ──────────────────────────────────────────────────

def ai_step1_classify_sender(sender: str, subject: str) -> str:
    """
    Step 1 — Cheap call. Sender + subject only. No email body.
    Returns: "bank" / "promotional" / "irrelevant"
    Cost: ~50 tokens per email.
    """
    if not ANTHROPIC_API_KEY:
        return "irrelevant"

    prompt = f"""Classify this email sender.

From: {sender}
Subject: {subject}

Reply with ONLY one word:
- "bank" if this looks like a transaction alert from any Indian bank or financial institution
- "promotional" if this is marketing, offers, newsletters, deals from any company
- "irrelevant" if this is OTP, login alert, account statement, or non-financial"""

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
                "max_tokens": 10,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=5
        )
        resp.raise_for_status()
        result = resp.json()["content"][0]["text"].strip().lower()
        if "bank" in result: return "bank"
        if "promo" in result: return "promotional"
        return "irrelevant"
    except Exception as e:
        print(f"AI step1 error: {str(e)[:80]}")
        return "irrelevant"


def ai_step2_extract_transaction(subject: str, body: str, sender: str, bank: str) -> dict:
    """
    Step 2 — Full call. Only runs if step 1 returned "bank".
    Sends full email body to extract transaction details.
    Returns dict with transaction details or None if not a debit.
    """
    if not ANTHROPIC_API_KEY:
        return None

    prompt = f"""Extract transaction details from this {bank} bank alert email.

From: {sender}
Subject: {subject}
Body: {body[:2000]}

Rules:
- Only extract DEBIT transactions (money going OUT)
- Skip credits, refunds, OTPs, balance alerts

Return ONLY valid JSON or null:
{{"is_debit": true, "amount": 1234.56, "mode": "UPI or Credit Card or Debit Card or NACH or Net Banking", "merchant": "merchant name", "vpa": "upi@handle or empty string"}}"""

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
            timeout=10
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if "null" in text.lower() and "{" not in text:
            return None
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            if data.get("is_debit") and data.get("amount"):
                return data
    except Exception as e:
        print(f"AI step2 error: {str(e)[:80]}")
    return None

def ai_classify_merchant(merchant_raw: str, vpa: str, amount: float) -> tuple:
    """AI classifies unknown merchant. Returns (canonical, category, treatment)."""
    if not ANTHROPIC_API_KEY:
        return merchant_raw, "Other", "spend"

    prompt = f"""Classify this Indian payment transaction merchant.

Merchant/VPA: "{merchant_raw or vpa}"
Amount: ₹{amount}

Return ONLY JSON:
{{"canonical": "Clean merchant name", "category": "category name", "treatment": "spend or investment or settlement"}}

Categories: Food & Dining, Groceries, Shopping, Travel & Transport, Fuel, Entertainment & OTT, Health & Medical, Utilities & Bills, Subscriptions, Education, Rent, P2P Transfer, Daily Spend, Insurance, EMI & Loans, Credit Card Payment, Investments & Finance, Other
Treatment: spend (regular purchase), investment (SIP/MF/stocks), settlement (CC payment/EMI)"""

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
                "max_tokens": 150,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=8
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        m = re.search(r'{.*}', text, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            canonical = data.get("canonical", merchant_raw)
            category = data.get("category", "Other")
            treatment = data.get("treatment", "spend")
            # Save to merchant_rules for all modes
            if canonical and category != "Other":
                keyword = vpa.split("@")[0].lower() if vpa else merchant_raw.lower()[:50]
                save_merchant_rule(keyword, canonical, category, treatment, "all")
            return canonical, category, treatment
    except Exception as e:
        print(f"AI merchant error: {str(e)[:100]}")
    return merchant_raw or "Unknown", "Other", "spend"



# ── Main sync ─────────────────────────────────────────────────

def sync_gmail_account(user_id: str, gmail_email: str, access_token: str, refresh_token: str) -> tuple:
    """
    Sync one Gmail account.
    Steps:
    1. Fetch ALL emails from known bank senders (DB) + broad financial keywords
    2. Skip negative senders immediately
    3. Known bank sender → rule-based extract → AI for unknowns
    4. Unknown sender → AI classifies → save to bank_senders or negative_rules
    5. Store transactions with full details
    Returns (new_transactions_count, banks_found_set)
    """
    ensure_tables()
    seed_bank_senders()
    seed_merchant_rules()

    last_sync = get_last_sync(user_id, gmail_email)
    if last_sync:
        after = (last_sync - timedelta(days=2)).strftime("%Y/%m/%d")
    else:
        after = (datetime.now() - timedelta(days=90)).strftime("%Y/%m/%d")

    # Load rules from DB
    bank_senders = get_all_bank_senders()
    negative_senders = get_negative_senders()
    merchant_rules = get_merchant_rules()

    print(f"Sync start: {len(bank_senders)} bank senders, {len(negative_senders)} negative, {len(merchant_rules)} merchant rules")

    # Build search query — known senders + broad financial keywords
    sender_list = " OR ".join([f"from:{s}" for s in list(bank_senders.keys())[:60]])
    queries = [f"({sender_list}) after:{after}"] if sender_list else []
    # Broad query for unknown senders with strong debit signals
    queries.append(f"(\"has been debited\" OR \"has been used\" OR \"UPI transaction alert\" OR \"Credit Card transaction\") after:{after}")

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
    ai_calls = 0
    seen_msg_ids = set()

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
                    sender = extract_sender_email(sender_raw)
                    subject = get_header(full, "Subject")
                    body = get_body(full)
                    msg_id = get_header(full, "Message-ID") or m['id']
                    text = subject + " " + body

                    # STEP 1: Skip negative senders immediately
                    if sender in negative_senders:
                        emails_processed += 1
                        continue

                    # STEP 2: Identify bank
                    bank = bank_senders.get(sender)
                    if not bank:
                        # Partial match
                        for known, bname in bank_senders.items():
                            if known in sender:
                                bank = bname
                                break
                    # Normalise bank names
                    if bank:
                        bank = bank.replace("HDFC Bank", "HDFC").replace("ICICI Bank", "ICICI").strip()

                    # STEP 3: Unknown sender — two-step AI classification
                    if not bank and ai_calls < 30:
                        # Step 1: cheap call — sender + subject only, no body
                        sender_type = ai_step1_classify_sender(sender, subject)
                        ai_calls += 1

                        if sender_type == "promotional":
                            save_negative_rule(sender, "promotional", "ai")
                            negative_senders.add(sender)
                            emails_processed += 1
                            continue
                        elif sender_type == "irrelevant":
                            save_negative_rule(sender, "irrelevant", "ai")
                            negative_senders.add(sender)
                            emails_processed += 1
                            continue
                        elif sender_type == "bank":
                            # Step 2: full call — extract transaction from body
                            txn = ai_step2_extract_transaction(subject, body, sender, "Unknown Bank")
                            ai_calls += 1
                            if txn:
                                bank = txn.get("bank", "Unknown Bank")
                                save_bank_sender(sender, bank, "ai")
                                bank_senders[sender] = bank
                                amount = float(txn["amount"])
                                merchant_raw = clean_merchant(txn.get("merchant", ""))
                                vpa = txn.get("vpa", "")
                                mode = txn.get("mode", "Unknown")

                                resolved = resolve_merchant_name(vpa, "upi", amount, merchant_rules) if vpa else None
                                if resolved:
                                    canonical, category, treatment, person_name = resolved
                                    ai_cls = False
                                else:
                                    canonical, category, treatment = ai_classify_merchant(merchant_raw, vpa, amount)
                                    person_name = ""
                                    ai_cls = True
                                    ai_calls += 1

                                transactions.append({
                                    "bank": bank, "mode": mode, "amount": amount,
                                    "merchant_raw": merchant_raw, "merchant_canonical": canonical,
                                    "category": category, "treatment": treatment,
                                    "vpa": vpa, "person_name": person_name,
                                    "date": parse_email_date(full),
                                    "msg_id": msg_id, "gmail_account": gmail_email,
                                    "ai_classified": ai_cls,
                                })
                                banks_found.add(bank)
                        emails_processed += 1
                        continue

                    # STEP 4: Known bank sender → check if debit
                    if not is_debit(subject, body):
                        emails_processed += 1
                        continue

                    banks_found.add(bank)
                    amount = extract_amount(text)
                    if not amount:
                        emails_processed += 1
                        continue

                    mode = detect_mode(text)
                    merchant_raw = ""
                    vpa = ""
                    canonical = "Unknown"
                    category = "Other"
                    treatment = "spend"
                    person_name = ""
                    ai_cls = False

                    # STEP 4b: If rule-based amount extraction failed → AI fallback
                    if not amount and ai_calls < 50:
                        txn = ai_step2_extract_transaction(subject, body, sender, bank)
                        ai_calls += 1
                        if txn and txn.get("is_debit") and txn.get("amount"):
                            amount = float(txn["amount"])
                            merchant_raw = clean_merchant(txn.get("merchant", ""))
                            vpa = txn.get("vpa", "")
                            mode = txn.get("mode", mode)
                            resolved = resolve_merchant_name(vpa, "upi", amount, merchant_rules) if vpa else None
                            if resolved:
                                canonical, category, treatment, person_name = resolved
                                ai_cls = False
                            else:
                                canonical, category, treatment = ai_classify_merchant(merchant_raw, vpa, amount)
                                person_name = ""
                                ai_cls = True
                                ai_calls += 1
                            transactions.append({
                                "bank": bank, "mode": mode, "amount": amount,
                                "merchant_raw": merchant_raw, "merchant_canonical": canonical,
                                "category": category, "treatment": treatment,
                                "vpa": vpa, "person_name": person_name,
                                "date": parse_email_date(full),
                                "msg_id": msg_id, "gmail_account": gmail_email,
                                "ai_classified": True,
                            })
                            banks_found.add(bank)
                        emails_processed += 1
                        continue

                    if not amount:
                        emails_processed += 1
                        continue

                    # STEP 5: Extract merchant by mode
                    if mode == "UPI":
                        vpa = extract_vpa(text)
                        resolved = resolve_merchant_name(vpa, "upi", amount, merchant_rules) if vpa else None
                        if resolved:
                            canonical, category, treatment, person_name = resolved
                            merchant_raw = vpa
                        else:
                            merchant_raw = vpa
                            # AI classifies unknown VPA
                            if ai_calls < 50:
                                canonical, category, treatment = ai_classify_merchant(merchant_raw, vpa, amount)
                                ai_cls = True
                                ai_calls += 1

                    elif mode == "Credit Card":
                        # Multiple patterns for different bank formats
                        cc_patterns = [
                            r'payment to\s+([A-Z0-9][A-Za-z0-9\s\*\-\.]+?)\s+on\s+\d',
                            r'used for.*?(?:payment to|at)\s+([A-Z0-9][A-Za-z0-9\s\*\-\.]+?)\s+on\s+\d',
                            r'INR\s+[\d,\.]+\s+for\s+(?:payment to\s+)?([A-Z][A-Za-z0-9\s\*\-\.]+?)\s+on\s+\d',
                            r'(?:at|towards)\s+([A-Z][A-Za-z0-9\s\*\-\.]+?)\s+(?:on|for|dated|\d)',
                        ]
                        for pattern in cc_patterns:
                            mm = re.search(pattern, text, re.IGNORECASE)
                            if mm:
                                merchant_raw = clean_merchant(mm.group(1).strip())
                                break
                        if merchant_raw and ai_calls < 50:
                            canonical, category, treatment = ai_classify_merchant(merchant_raw, "", amount)
                            ai_cls = True
                            ai_calls += 1
                        elif not merchant_raw and ai_calls < 50:
                            # No merchant found by rules — send full email to AI
                            txn = ai_step2_extract_transaction(subject, body, sender, bank)
                            ai_calls += 1
                            if txn and txn.get("merchant"):
                                merchant_raw = clean_merchant(txn.get("merchant", ""))
                                canonical, category, treatment = ai_classify_merchant(merchant_raw, "", amount)
                                ai_cls = True
                                ai_calls += 1
                        elif not merchant_raw and ai_calls < 50:
                            # Regex failed — send full email to AI to extract merchant
                            txn = ai_step2_extract_transaction(subject, body, sender, bank)
                            ai_calls += 1
                            if txn and txn.get("merchant"):
                                merchant_raw = clean_merchant(txn.get("merchant", ""))
                                vpa = txn.get("vpa", "") or vpa
                                canonical, category, treatment = ai_classify_merchant(merchant_raw, vpa, amount)
                                ai_cls = True
                                ai_calls += 1

                    elif mode == "NACH":
                        result = resolve_nach(body, nach_rules)
                        if result:
                            merchant_raw, category, treatment = result
                            canonical = merchant_raw
                        else:
                            # Unknown NACH — send to AI
                            if ai_calls < 50:
                                txn = ai_step2_extract_transaction(subject, body, sender, bank)
                                ai_calls += 1
                                if txn and txn.get("merchant"):
                                    merchant_raw = clean_merchant(txn.get("merchant", ""))
                                    canonical, category, treatment = ai_classify_merchant(merchant_raw, "", amount)
                                    ai_cls = True
                                    ai_calls += 1
                                    # Save to nach_rules for future
                                    if merchant_raw and category != "Other":
                                        try:
                                            with get_conn() as c:
                                                with c.cursor() as cc:
                                                    cc.execute("""
                                                        INSERT INTO nach_rules (keyword, merchant_canonical, category, treatment, source)
                                                        VALUES (%s, %s, %s, %s, 'ai') ON CONFLICT DO NOTHING
                                                    """, (merchant_raw.lower()[:50], canonical, category, treatment))
                                                    c.commit()
                                        except:
                                            pass

                    else:
                        # Net Banking / Debit Card / Unknown mode
                        mm = re.search(r'(?:at|to)\s+([A-Z][A-Za-z0-9\s\-\.]+?)\s+(?:on|\d)', text, re.IGNORECASE)
                        if mm:
                            merchant_raw = clean_merchant(mm.group(1).strip())
                        if merchant_raw and ai_calls < 50:
                            canonical, category, treatment = ai_classify_merchant(merchant_raw, "", amount)
                            ai_cls = True
                            ai_calls += 1
                        elif not merchant_raw and ai_calls < 50:
                            # No merchant found by rules — AI extracts full transaction
                            txn = ai_step2_extract_transaction(subject, body, sender, bank)
                            ai_calls += 1
                            if txn and txn.get("merchant"):
                                merchant_raw = clean_merchant(txn.get("merchant", ""))
                                vpa = txn.get("vpa", "") or vpa
                                mode = txn.get("mode", mode)
                                canonical, category, treatment = ai_classify_merchant(merchant_raw, vpa, amount)
                                ai_cls = True
                                ai_calls += 1

                    transactions.append({
                        "bank": bank, "mode": mode, "amount": amount,
                        "merchant_raw": merchant_raw, "merchant_canonical": canonical or merchant_raw,
                        "category": category, "treatment": treatment,
                        "vpa": vpa, "person_name": person_name,
                        "date": parse_email_date(full),
                        "msg_id": msg_id, "gmail_account": gmail_email,
                        "ai_classified": ai_cls,
                    })
                    emails_processed += 1

                except Exception as e:
                    print(f"Email error: {str(e)[:80]}")
                    emails_processed += 1
                    continue

        except Exception as e:
            print(f"Query error: {str(e)[:80]}")
            continue

    print(f"Sync done: {emails_processed} emails, {len(transactions)} transactions, {ai_calls} AI calls")

    if transactions:
        save_transactions(user_id, transactions)

    update_sync_log(user_id, gmail_email, emails_processed, len(transactions))
    return len(transactions), banks_found


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
            print(f"Account sync error {account.get('email','?')}: {str(e)[:100]}")
    return {"new_transactions": total_new, "banks_found": list(all_banks)}


def get_transactions(user_id: str, gmail_accounts: list, days: int = 30,
                     start_date=None, end_date=None) -> list:
    """Sync then return from DB."""
    sync_all_gmail(user_id, gmail_accounts)
    from db import get_transactions_from_db
    return get_transactions_from_db(user_id, days=days, start_date=start_date, end_date=end_date)
