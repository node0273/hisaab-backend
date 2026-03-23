"""
Merchant Resolver — 3-tier lookup with AI fallback
Tier 1: DB lookup (aliases + VPA patterns)
Tier 2: VPA person/business detection
Tier 3: AI classification → saved as pending for admin review
"""
import os
import re
import requests
from db import get_conn

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Business VPA keyword mapping
VPA_KEYWORDS = {
    'zomato': ('Zomato', 'Food & Dining', 'spend'),
    'swiggy': ('Swiggy', 'Food & Dining', 'spend'),
    'blinkit': ('Blinkit', 'Groceries', 'spend'),
    'bigbasket': ('BigBasket', 'Groceries', 'spend'),
    'zepto': ('Zepto', 'Groceries', 'spend'),
    'amazon': ('Amazon', 'Shopping', 'spend'),
    'flipkart': ('Flipkart', 'Shopping', 'spend'),
    'myntra': ('Myntra', 'Shopping', 'spend'),
    'uber': ('Uber', 'Travel & Transport', 'spend'),
    'olacabs': ('Ola', 'Travel & Transport', 'spend'),
    'rapido': ('Rapido', 'Travel & Transport', 'spend'),
    'irctc': ('IRCTC', 'Travel & Transport', 'spend'),
    'netflix': ('Netflix', 'Entertainment & OTT', 'spend'),
    'hotstar': ('Hotstar', 'Entertainment & OTT', 'spend'),
    'spotify': ('Spotify', 'Entertainment & OTT', 'spend'),
    'airtel': ('Airtel', 'Utilities & Bills', 'spend'),
    'jio': ('Jio', 'Utilities & Bills', 'spend'),
    'apollopharmacy': ('Apollo Pharmacy', 'Health & Medical', 'spend'),
    'pharmeasy': ('PharmEasy', 'Health & Medical', 'spend'),
    'zerodha': ('Zerodha', 'Investments & Finance', 'investment'),
    'groww': ('Groww', 'Investments & Finance', 'investment'),
    'bse': ('BSE/MF', 'Investments & Finance', 'investment'),
    'nse': ('NSE/MF', 'Investments & Finance', 'investment'),
    'mfcentral': ('Mutual Fund', 'Investments & Finance', 'investment'),
    'licpay': ('LIC', 'Insurance', 'spend'),
    'dominos': ('Dominos', 'Food & Dining', 'spend'),
    'kfc': ('KFC', 'Food & Dining', 'spend'),
    'pizzahut': ('Pizza Hut', 'Food & Dining', 'spend'),
    'starbucks': ('Starbucks', 'Food & Dining', 'spend'),
    'makemytrip': ('MakeMyTrip', 'Travel & Transport', 'spend'),
    'indigo': ('IndiGo', 'Travel & Transport', 'spend'),
    'foodsquare': ('Food Square', 'Food & Dining', 'spend'),
    'pranaam': ('Pranaam', 'Travel & Transport', 'spend'),
    'paytm': ('Paytm', 'Other', 'spend'),
}

def check_vpa_keywords(vpa: str):
    """Check VPA handle against known business keywords."""
    if not vpa:
        return None
    handle = vpa.split('@')[0].lower()
    # Remove numbers and special chars for matching
    handle_clean = handle.replace('.', '').replace('-', '').replace('_', '')
    for keyword, result in VPA_KEYWORDS.items():
        if keyword in handle_clean or keyword in handle:
            return result
    return None


ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID", "8130140084")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

CATEGORIES = [
    "Food & Dining", "Groceries", "Shopping", "Travel & Transport",
    "Fuel", "Entertainment & OTT", "Health & Medical", "Utilities & Bills",
    "Subscriptions", "Education", "Rent", "Daily Spend", "P2P Transfer",
    "Insurance", "EMI & Loans", "Credit Card Payment", "Investments & Finance",
    "Refund", "Income & Salary", "Cashback", "Inter-account Transfer", "Other"
]

PERSON_NAME_PATTERN = re.compile(
    r'^[a-zA-Z]+[\.\-][a-zA-Z]+@|^[a-zA-Z]{3,20}[0-9]{0,4}@',
    re.IGNORECASE
)

def is_person_vpa(vpa: str) -> bool:
    """Detect if VPA belongs to a person (not a business)."""
    if not vpa:
        return False
    handle = vpa.split("@")[0].lower()
    # 10-digit phone number
    if re.match(r'^\d{10}$', handle):
        return True
    # firstname.lastname or firstname-lastname pattern
    if re.match(r'^[a-z]+[\.\-][a-z]+\d{0,4}$', handle):
        return True
    # No known business keywords
    business_keywords = [
        'zomato', 'swiggy', 'amazon', 'flipkart', 'uber', 'ola',
        'paytm', 'phonepe', 'googlepay', 'airtel', 'jio', 'netflix',
        'irctc', 'hdfc', 'icici', 'axis', 'sbi', 'kotak', 'ybl',
        'okhdfcbank', 'okicici', 'oksbi', 'okaxis', 'idfcfirst',
        'indus', 'federal', 'rbl', 'yes', 'pnb', 'bob', 'canara'
    ]
    return not any(kw in handle for kw in business_keywords)

def extract_person_name(vpa: str) -> str:
    """Extract readable name from person VPA."""
    handle = vpa.split("@")[0]
    # Phone number → just show truncated
    if re.match(r'^\d{10}$', handle):
        return f"****{handle[-4:]}"
    # firstname.lastname → Firstname Lastname
    name = re.sub(r'[._\-]', ' ', handle)
    name = re.sub(r'\d+$', '', name).strip()
    return name.title()

def resolve_merchant(raw_name: str, vpa: str = "", amount: float = 0) -> tuple:
    """
    Returns (canonical_name, category, treatment, person_name, ai_classified)
    """
    # 1. Person VPA detection
    if vpa and is_person_vpa(vpa):
        person_name = extract_person_name(vpa)
        if amount < 500:
            return "Daily Spend", "Daily Spend", "spend", person_name, False
        else:
            return f"P2P - {person_name}", "P2P Transfer", "spend", person_name, False

    # 2a. Check VPA keywords (fast, no DB needed)
    if vpa:
        result = check_vpa_keywords(vpa)
        if result:
            return result[0], result[1], result[2], "", False

    # 2b. Check VPA against known merchant patterns in DB
    if vpa:
        result = check_vpa_in_db(vpa)
        if result:
            return result[0], result[1], result[2], "", False

    # 3. Check name against DB (aliases + canonical)
    if raw_name:
        # Check keywords in raw name too
        raw_lower = raw_name.lower().replace(' ', '').replace('/', '')
        for keyword, result in VPA_KEYWORDS.items():
            if keyword in raw_lower:
                return result[0], result[1], result[2], "", False
        result = check_name_in_db(raw_name)
        if result:
            return result[0], result[1], result[2], "", False

    # 4. Try extracting merchant from VPA handle
    if vpa:
        handle = vpa.split("@")[0]
        result = check_name_in_db(handle)
        if result:
            return result[0], result[1], result[2], "", False

    # 5. AI classification
    if (raw_name or vpa) and ANTHROPIC_API_KEY:
        result = ai_classify(raw_name, vpa, amount)
        if result:
            save_pending_merchant(raw_name or vpa, result[0], result[1], result[2])
            notify_admin_new_merchant(raw_name or vpa, result[0], result[1])
            return result[0], result[1], result[2], "", True

    return raw_name or vpa or "Unknown", "Other", "spend", "", False

def check_vpa_in_db(vpa: str):
    vpa_lower = vpa.lower()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT canonical_name, category, treatment, vpa_patterns
                FROM merchants WHERE vpa_patterns IS NOT NULL AND status = 'approved'
            """)
            for row in cur.fetchall():
                canonical, category, treatment, patterns = row
                if patterns:
                    for pattern in patterns:
                        if pattern.lower() in vpa_lower:
                            return canonical, category, treatment
    return None

def check_name_in_db(name: str):
    name_upper = name.upper().strip()
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Exact canonical match
            cur.execute("""
                SELECT canonical_name, category, treatment FROM merchants
                WHERE UPPER(canonical_name) = %s AND status = 'approved'
            """, (name_upper,))
            row = cur.fetchone()
            if row:
                return row

            # Alias match
            cur.execute("""
                SELECT canonical_name, category, treatment FROM merchants
                WHERE %s = ANY(SELECT UPPER(unnest(aliases))) AND status = 'approved'
            """, (name_upper,))
            row = cur.fetchone()
            if row:
                return row

            # Partial match
            cur.execute("""
                SELECT canonical_name, category, treatment FROM merchants
                WHERE UPPER(canonical_name) LIKE %s AND status = 'approved'
                LIMIT 1
            """, (f"%{name_upper}%",))
            row = cur.fetchone()
            if row:
                return row
    return None

def ai_classify(raw_name: str, vpa: str, amount: float) -> tuple:
    """Use Claude Haiku to classify unknown merchant."""
    prompt = f"""Classify this Indian transaction merchant.

Raw name: "{raw_name}"
VPA: "{vpa}"
Amount: ₹{amount}

Return ONLY valid JSON:
{{"canonical_name": "short clean name", "category": "one of the categories", "treatment": "spend or investment or settlement or excluded"}}

Categories: {', '.join(CATEGORIES)}

Treatment rules:
- spend: regular purchase
- investment: SIP, mutual fund, stocks, Zerodha, Groww
- settlement: credit card payment, EMI payment
- excluded: salary credit, cashback, inter-account transfer"""

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
            timeout=5
        )
        resp.raise_for_status()
        import json
        text = resp.json()["content"][0]["text"].strip()
        # Clean JSON
        text = re.sub(r'```json|```', '', text).strip()
        data = json.loads(text)
        return data.get("canonical_name", raw_name), data.get("category", "Other"), data.get("treatment", "spend")
    except:
        return None

def save_pending_merchant(raw_name: str, canonical: str, category: str, treatment: str):
    """Save AI-classified merchant as pending for admin review."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO merchants (canonical_name, category, treatment, aliases, ai_classified, status, pending_since)
                VALUES (%s, %s, %s, %s, TRUE, 'pending', NOW())
                ON CONFLICT (canonical_name) DO UPDATE SET
                    aliases = array_append(merchants.aliases, %s),
                    updated_at = NOW()
            """, (canonical, category, treatment, [raw_name], raw_name))
            conn.commit()

def notify_admin_new_merchant(raw_name: str, canonical: str, category: str):
    """Send Telegram notification to admin about new merchant."""
    if not TELEGRAM_TOKEN:
        return
    try:
        msg = f"🏪 *New merchant classified*\n\nRaw: `{raw_name}`\nCanonical: *{canonical}*\nCategory: {category}\n\nReply with:\n`/approve {canonical}` or `/reject {canonical} correct_category`\n\n_Auto-approves in 5 days_"
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_TELEGRAM_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=3
        )
    except:
        pass
