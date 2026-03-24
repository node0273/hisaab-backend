"""
Gmail Reader — AI-first, rules as cache
Pipeline:
1. Check negative_rules → skip spam forever
2. Check bank_senders → identify bank
3. Unknown sender → AI Step1 (sender+subject only) → classify
4. Check parsing_rules → extract fields (free)
5. No rule → AI Step2 (full body) → extract → save parsing_rule pending
6. Resolve merchant from merchant_rules (free)
7. Unknown merchant → AI Step3 → classify → save pending
8. Store transaction
"""
import base64, re, os, requests, json
from datetime import datetime, timedelta
from db import (get_last_sync, update_sync_log, save_transactions,
                update_access_token, get_bank_senders, get_negative_senders,
                get_merchant_rules, get_parsing_rules, save_bank_sender,
                save_negative_rule, save_merchant_rule, save_parsing_rule)

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID", "8130140084")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

# ── UPI App detection ─────────────────────────────────────────
UPI_APP_MAP = {
    # Google Pay
    "okaxis": "Google Pay", "okhdfcbank": "Google Pay",
    "okicici": "Google Pay", "oksbi": "Google Pay",
    "okbizaxis": "Google Pay",
    # PhonePe
    "ybl": "PhonePe", "ibl": "PhonePe", "axl": "PhonePe",
    "ptyes": "PhonePe", "pingpay": "PhonePe",
    # Paytm
    "paytm": "Paytm", "pthdfc": "Paytm", "ptsbi": "Paytm", "pty": "Paytm",
    # Amazon Pay
    "apl": "Amazon Pay", "rapl": "Amazon Pay",
    # Others
    "jiopay": "JioPay", "freecharge": "Freecharge",
    "digikhata": "DigiKhata", "hdfcbank": "HDFC Pay",
    "icici": "ICICI Pay", "sbi": "SBI Pay",
    "kotak": "Kotak Pay", "indus": "IndusInd Pay",
    "aubank": "AU Pay", "mahb": "MahaBank Pay",
}

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

def gmail_get(path, token, params=None):
    resp = requests.get(
        f"{GMAIL_API}/{path}",
        headers={"Authorization": f"Bearer {token}"},
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
        for p in parts:
            if p.get("mimeType") == "text/plain" and p.get("body", {}).get("data"):
                return decode_body(p["body"]["data"])
            if "parts" in p:
                r = walk(p["parts"])
                if r: return r
        return ""
    body = ""
    if payload.get("body", {}).get("data"):
        body = decode_body(payload["body"]["data"])
    elif payload.get("parts"):
        body = walk(payload["parts"])
    return (body + " " + msg.get("snippet", ""))[:3000]

def extract_sender(from_header: str) -> str:
    m = re.search(r'<([^>]+)>', from_header)
    return (m.group(1) if m else from_header).lower().strip()

def parse_date(msg):
    date_str = get_header(msg, "Date")
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S %Z"]:
        try: return datetime.strptime(date_str[:35].strip(), fmt).date()
        except: pass
    return datetime.now().date()

# ── UPI helpers ───────────────────────────────────────────────

def get_upi_app(vpa: str) -> str:
    if not vpa or "@" not in vpa:
        return ""
    suffix = vpa.split("@")[1].lower()
    return UPI_APP_MAP.get(suffix, suffix.title())

def is_person_vpa(vpa: str) -> bool:
    if not vpa or "@" not in vpa:
        return False
    handle = vpa.split("@")[0].lower()
    # Phone number
    if re.match(r'^\d{10}$', handle):
        return True
    # firstname.lastname or firstname-lastname
    if re.match(r'^[a-z]+[.\-][a-z]+\d{0,4}$', handle):
        return True
    return False

def extract_person_name(vpa: str) -> str:
    handle = vpa.split("@")[0]
    if re.match(r'^\d{10}$', handle):
        return f"****{handle[-4:]}"
    name = re.sub(r'[._\-]', ' ', handle)
    name = re.sub(r'\d+$', '', name).strip()
    return name.title()

def clean_merchant(name: str) -> str:
    """Strip payment gateway prefixes: PYU*ZOMATO → ZOMATO"""
    if not name: return name
    name = re.sub(r'^[A-Z]{2,4}\*', '', name).strip()
    name = re.sub(r'\s+[A-Z]{2,}\s+IN$', '', name).strip()
    return name

# ── Amount extraction ─────────────────────────────────────────

def extract_amount(text: str) -> float:
    patterns = [
        r'(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
        r'for\s+INR\s+([\d,]+\.?\d*)',
        r'([\d,]+\.?\d*)\s*(?:rupees|rs\.?)',
        r'amount[:\s]+(?:Rs\.?|INR|₹)?\s*([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 0: return val
            except: pass
    return None

def detect_mode(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["upi", "vpa", "bhim"]): return "UPI"
    if any(k in t for k in ["credit card", "cc ", "visa", "mastercard", "rupay", "amex"]): return "Credit Card"
    if any(k in t for k in ["nach", "ach", "mandate", "ecs", "auto debit", "e-mandate"]): return "NACH"
    if any(k in t for k in ["debit card", "atm", " pos "]): return "Debit Card"
    if any(k in t for k in ["neft", "rtgs", "imps", "net banking"]): return "Net Banking"
    return "Unknown"

def is_debit(subject: str, body: str) -> bool:
    text = (subject + " " + body[:500]).lower()
    skips = ["otp", "one time password", "login alert", "password reset",
             "statement", "welcome", "account opening", "kyc",
             "reward points", "cashback earned", "offer expires",
             "minimum due", "payment due", "bill generated"]
    if any(k in text for k in skips): return False
    credits = ["credited", "received", "refund", "cashback", "salary"]
    debits = ["debited", "debit", "spent", "used for", "has been used",
              "payment of", "paid to", "purchase", "transaction of",
              "has been debited", "card has been used", "withdrawn"]
    c = sum(1 for k in credits if k in text)
    d = sum(1 for k in debits if k in text)
    return d > 0 and d >= c

# ── Merchant resolution ───────────────────────────────────────

def resolve_merchant(raw_name: str, merchant_rules: dict) -> tuple:
    """
    Match raw merchant name against merchant_rules.
    Returns (canonical, category, treatment) or None
    """
    if not raw_name: return None
    name_clean = re.sub(r'[^a-z0-9]', '', raw_name.lower())
    name_lower = raw_name.lower()
    for keyword, result in merchant_rules.items():
        kw_clean = re.sub(r'[^a-z0-9]', '', keyword.lower())
        if kw_clean in name_clean or keyword.lower() in name_lower:
            return result[0], result[1], result[2]
    return None

# ── AI calls ──────────────────────────────────────────────────

def ai_call(prompt: str, max_tokens: int = 300) -> str:
    if not ANTHROPIC_API_KEY: return ""
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001",
                  "max_tokens": max_tokens,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()
    except Exception as e:
        print(f"AI call error: {str(e)[:80]}")
        return ""

def parse_json(text: str) -> dict:
    try:
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m: return json.loads(m.group(0))
    except: pass
    return {}

def ai_step1_classify_sender(sender: str, subject: str) -> str:
    """Cheap call — sender + subject only. Returns: bank/promotional/irrelevant"""
    result = ai_call(f"""Classify this email sender with ONE word only.

From: {sender}
Subject: {subject}

Reply ONLY with one of:
- bank (transaction alert from any Indian bank or financial institution)
- promotional (marketing, offers, deals, newsletters)
- irrelevant (OTP, login alert, statement, non-financial)""", max_tokens=10)
    if "bank" in result.lower(): return "bank"
    if "promo" in result.lower(): return "promotional"
    return "irrelevant"

def ai_step2_extract_transaction(subject: str, body: str, bank: str) -> dict:
    """Full body extraction — only called for bank emails with no parsing rule."""
    result = ai_call(f"""Extract transaction from this {bank} bank alert email.

Subject: {subject}
Body: {body[:2000]}

Return JSON or null if not a debit:
{{"is_debit": true, "amount": 1234.56, "mode": "UPI or Credit Card or NACH or Debit Card or Net Banking", "merchant": "merchant name or empty", "vpa": "upi@handle or empty"}}

Rules:
- Only extract DEBIT (money going OUT)
- For UPI: extract the full VPA handle
- For Credit Card: extract merchant name (strip PYU* prefix)
- For NACH: extract company name
- Return null for credits, OTPs, balance alerts""", max_tokens=200)
    if not result or "null" in result.lower() and "{" not in result:
        return None
    data = parse_json(result)
    if data.get("is_debit") and data.get("amount"):
        return data
    return None

def ai_step3_classify_merchant(raw_name: str, mode: str, amount: float) -> tuple:
    """Classify unknown merchant. Returns (canonical, category, treatment)"""
    categories = [
        "Food & Dining", "Groceries", "Shopping", "Travel & Transport",
        "Fuel", "Entertainment & OTT", "Health & Medical", "Utilities & Bills",
        "Subscriptions", "Education", "Rent", "Insurance",
        "Investments & Finance", "EMI & Loans", "Credit Card Payment",
        "P2P Transfer", "Daily Spend", "Income & Salary", "Other"
    ]
    result = ai_call(f"""Classify this Indian payment merchant.

Merchant: "{raw_name}"
Mode: {mode}
Amount: ₹{amount}

Return JSON:
{{"canonical": "Clean short name", "category": "one category", "treatment": "spend or investment or settlement or excluded"}}

Categories: {', '.join(categories)}
Treatment: spend=regular purchase, investment=SIP/MF/stocks, settlement=CC payment/EMI, excluded=salary/cashback""", max_tokens=150)
    data = parse_json(result)
    return (
        data.get("canonical", raw_name),
        data.get("category", "Other"),
        data.get("treatment", "spend")
    )

def notify_admin(message: str):
    """Send Telegram alert to admin."""
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_TELEGRAM_ID, "text": message, "parse_mode": "Markdown"},
            timeout=3
        )
    except: pass

# ── Rule-based extraction ─────────────────────────────────────

def rule_based_extract(subject: str, body: str, bank: str,
                       mode: str, parsing_rules: list) -> dict:
    """Try to extract using stored parsing rules."""
    text = subject + " " + body
    for rule in parsing_rules:
        try:
            amount = None
            merchant = ""
            vpa = ""
            if rule.get("amount_pattern"):
                m = re.search(rule["amount_pattern"], text, re.IGNORECASE)
                if m: amount = float(m.group(1).replace(",", ""))
            if rule.get("merchant_pattern"):
                m = re.search(rule["merchant_pattern"], text, re.IGNORECASE)
                if m: merchant = m.group(1).strip()
            if rule.get("vpa_pattern") and mode == "UPI":
                m = re.search(rule["vpa_pattern"], text, re.IGNORECASE)
                if m: vpa = m.group(1).strip()
            if amount and amount > 0:
                return {"amount": amount, "merchant": merchant, "vpa": vpa, "from_rule": True}
        except: continue
    return None

# ── Main sync ─────────────────────────────────────────────────

def sync_gmail_account(user_id: str, gmail_email: str,
                       access_token: str, refresh_token: str) -> tuple:
    """Full sync pipeline for one Gmail account."""
    last_sync = get_last_sync(user_id, gmail_email)
    after = (last_sync - timedelta(days=2)).strftime("%Y/%m/%d") if last_sync \
            else (datetime.now() - timedelta(days=90)).strftime("%Y/%m/%d")

    # Load all rules fresh
    bank_senders = get_bank_senders()
    negative_senders = get_negative_senders()
    merchant_rules = get_merchant_rules()

    print(f"Sync: {len(bank_senders)} senders, {len(negative_senders)} negative, {len(merchant_rules)} merchant rules")

    # Build search queries — known senders + strong debit keywords
    sender_query = " OR ".join([f"from:{s}" for s in list(bank_senders.keys())[:60]])
    queries = []
    if sender_query:
        queries.append(f"({sender_query}) after:{after}")
    queries.append(f'("has been debited" OR "has been used" OR "UPI transaction alert" OR "Credit Card transaction alert") after:{after}')

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
    seen = set()

    for query in queries:
        try:
            data = safe_get("messages", {"q": query, "maxResults": 200})
            for m in data.get("messages", []):
                if m['id'] in seen: continue
                seen.add(m['id'])

                try:
                    full = safe_get(f"messages/{m['id']}")
                    sender = extract_sender(get_header(full, "From"))
                    subject = get_header(full, "Subject")
                    body = get_body(full)
                    msg_id = get_header(full, "Message-ID") or m['id']
                    text = subject + " " + body

                    # Stage 1 — Skip negative senders
                    if sender in negative_senders:
                        emails_processed += 1
                        continue

                    # Stage 2 — Identify bank
                    bank = bank_senders.get(sender)
                    if not bank:
                        for known, bname in bank_senders.items():
                            if known in sender:
                                bank = bname
                                break

                    # Unknown sender — AI Step 1 (cheap)
                    if not bank and ai_calls < 30:
                        sender_type = ai_step1_classify_sender(sender, subject)
                        ai_calls += 1
                        if sender_type == "bank":
                            bank = "Unknown Bank"
                            save_bank_sender(sender, bank, "ai")
                            bank_senders[sender] = bank
                        else:
                            save_negative_rule(sender, sender_type)
                            negative_senders.add(sender)
                            emails_processed += 1
                            continue

                    if not bank:
                        emails_processed += 1
                        continue

                    # Normalise bank name
                    bank = bank.replace("HDFC Bank", "HDFC").strip()
                    banks_found.add(bank)

                    # Check if debit
                    if not is_debit(subject, body):
                        emails_processed += 1
                        continue

                    # Detect mode
                    mode = detect_mode(text)
                    amount = None
                    merchant_raw = ""
                    vpa = ""

                    # Stage 3 — Try parsing rules first
                    parsing_rules_list = get_parsing_rules(bank, mode)
                    extracted = rule_based_extract(subject, body, bank, mode, parsing_rules_list)

                    if extracted:
                        amount = extracted["amount"]
                        merchant_raw = extracted.get("merchant", "")
                        vpa = extracted.get("vpa", "")
                    else:
                        # AI Step 2 — full body extraction
                        if ai_calls < 50:
                            txn = ai_step2_extract_transaction(subject, body, bank)
                            ai_calls += 1
                            if not txn:
                                emails_processed += 1
                                continue
                            amount = float(txn["amount"])
                            mode = txn.get("mode", mode)
                            merchant_raw = clean_merchant(txn.get("merchant", ""))
                            vpa = txn.get("vpa", "")
                            # Save parsing rule as pending
                            rule_id = save_parsing_rule(
                                bank, mode,
                                r'(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)',
                                "",  # merchant pattern to be refined
                                r'VPA\s+([\w.\-@]+)' if mode == "UPI" else "",
                                subject[:200]
                            )
                            if rule_id:
                                notify_admin(
                                    f"📧 *New parsing rule pending #{rule_id}*\n\n"
                                    f"Bank: {bank}\nMode: {mode}\n"
                                    f"Subject: `{subject[:100]}`\n\n"
                                    f"/approve_rule {rule_id}\n/reject_rule {rule_id}"
                                )

                    if not amount:
                        amount = extract_amount(text)
                    if not amount or amount <= 0:
                        emails_processed += 1
                        continue

                    # Stage 4 — Resolve merchant
                    canonical = None
                    category = "Other"
                    treatment = "spend"
                    person_name = ""
                    upi_app = ""

                    if mode == "UPI":
                        upi_app = get_upi_app(vpa) if vpa else ""
                        if vpa and is_person_vpa(vpa):
                            # P2P transaction
                            person_name = extract_person_name(vpa)
                            canonical = "P2P Transfer"
                            category = "Daily Spend" if amount < 500 else "P2P Transfer"
                            treatment = "spend"
                        else:
                            # Merchant UPI
                            name_to_resolve = merchant_raw or (vpa.split("@")[0] if vpa else "")
                            result = resolve_merchant(name_to_resolve, merchant_rules)
                            if result:
                                canonical, category, treatment = result
                            elif name_to_resolve and ai_calls < 60:
                                canonical, category, treatment = ai_step3_classify_merchant(
                                    name_to_resolve, mode, amount
                                )
                                ai_calls += 1
                                rule_id = save_merchant_rule(
                                    name_to_resolve.lower()[:100],
                                    canonical, category, treatment, "ai"
                                )
                                if rule_id:
                                    notify_admin(
                                        f"🏪 *New merchant pending #{rule_id}*\n\n"
                                        f"Raw: `{name_to_resolve}`\n"
                                        f"Canonical: *{canonical}*\n"
                                        f"Category: {category}\n"
                                        f"Treatment: {treatment}\n\n"
                                        f"/approve_merchant {rule_id}\n"
                                        f"/reject_merchant {rule_id} NewCategory spend"
                                    )
                    else:
                        # Credit Card / NACH / Debit / Net Banking
                        name_to_resolve = clean_merchant(merchant_raw)
                        result = resolve_merchant(name_to_resolve, merchant_rules)
                        if result:
                            canonical, category, treatment = result
                        elif name_to_resolve and ai_calls < 60:
                            canonical, category, treatment = ai_step3_classify_merchant(
                                name_to_resolve, mode, amount
                            )
                            ai_calls += 1
                            rule_id = save_merchant_rule(
                                name_to_resolve.lower()[:100],
                                canonical, category, treatment, "ai"
                            )
                            if rule_id:
                                notify_admin(
                                    f"🏪 *New merchant pending #{rule_id}*\n\n"
                                    f"Raw: `{name_to_resolve}`\n"
                                    f"Canonical: *{canonical}*\n"
                                    f"Category: {category}\n"
                                    f"Treatment: {treatment}\n\n"
                                    f"/approve_merchant {rule_id}\n"
                                    f"/reject_merchant {rule_id} NewCategory spend"
                                )

                    if not canonical:
                        canonical = merchant_raw or vpa or "Unknown"

                    transactions.append({
                        "bank": bank,
                        "mode": mode,
                        "amount": amount,
                        "merchant_canonical": canonical,
                        "category": category,
                        "treatment": treatment,
                        "date": parse_date(full),
                        "vpa": vpa if mode == "UPI" else None,
                        "person_name": person_name,
                        "upi_app": upi_app,
                        "msg_id": msg_id,
                        "gmail_account": gmail_email,
                    })
                    emails_processed += 1

                except Exception as e:
                    print(f"Email error: {str(e)[:100]}")
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


def sync_all(user_id: str, gmail_accounts: list) -> dict:
    total, all_banks = 0, set()
    for acc in gmail_accounts:
        try:
            n, banks = sync_gmail_account(
                user_id, acc["email"],
                acc["access_token"], acc["refresh_token"]
            )
            total += n
            all_banks.update(banks)
        except Exception as e:
            print(f"Account error {acc.get('email','?')}: {str(e)[:80]}")
    return {"new_transactions": total, "banks_found": list(all_banks)}
