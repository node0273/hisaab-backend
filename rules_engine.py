"""
Rules Engine — DB-backed rules with bank_config.py as fallback
Loads rules from DB on startup, refreshes every 15 mins
Admin can update rules via Telegram without redeploying
"""
import os
import re
import time
from db import get_conn
from bank_config import BANK_SENDERS, VPA_MERCHANT_MAP, NACH_MERCHANT_MAP, BANK_KEYWORDS

# ── In-memory cache ───────────────────────────────────────────
_cache = {
    "bank_senders": {},
    "vpa_rules": {},
    "nach_rules": {},
    "last_loaded": 0,
}
CACHE_TTL = 900  # 15 minutes

def ensure_rules_tables():
    """Create rules tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bank_senders (
                    id SERIAL PRIMARY KEY,
                    sender_email VARCHAR(255) UNIQUE NOT NULL,
                    bank_name VARCHAR(100) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    source VARCHAR(20) DEFAULT 'manual',
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
            """)
            conn.commit()

def seed_rules_from_config():
    """Seed DB from bank_config.py if tables are empty."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Check if already seeded
            cur.execute("SELECT COUNT(*) FROM bank_senders")
            if cur.fetchone()[0] > 0:
                return  # Already seeded

            # Seed bank_senders
            for email, bank in BANK_SENDERS.items():
                cur.execute("""
                    INSERT INTO bank_senders (sender_email, bank_name, source)
                    VALUES (%s, %s, 'seed') ON CONFLICT (sender_email) DO NOTHING
                """, (email, bank))

            # Seed vpa_rules
            for keyword, result in VPA_MERCHANT_MAP.items():
                if result is not None:
                    cur.execute("""
                        INSERT INTO vpa_rules (keyword, merchant_canonical, category, treatment, source)
                        VALUES (%s, %s, %s, %s, 'seed') ON CONFLICT (keyword) DO NOTHING
                    """, (keyword, result[0], result[1], result[2]))

            # Seed nach_rules
            for keyword, result in NACH_MERCHANT_MAP.items():
                cur.execute("""
                    INSERT INTO nach_rules (keyword, merchant_canonical, category, treatment, source)
                    VALUES (%s, %s, %s, %s, 'seed') ON CONFLICT (keyword) DO NOTHING
                """, (keyword, result[0], result[1], result[2]))

            conn.commit()
            print(f"Rules seeded: {len(BANK_SENDERS)} senders, {len(VPA_MERCHANT_MAP)} VPA, {len(NACH_MERCHANT_MAP)} NACH")

def load_rules():
    """Load all rules from DB into memory cache."""
    global _cache

    now = time.time()
    if now - _cache["last_loaded"] < CACHE_TTL:
        return  # Cache still fresh

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Load bank senders
                cur.execute("SELECT sender_email, bank_name FROM bank_senders WHERE is_active = TRUE")
                _cache["bank_senders"] = {r[0]: r[1] for r in cur.fetchall()}

                # Load VPA rules
                cur.execute("SELECT keyword, merchant_canonical, category, treatment FROM vpa_rules WHERE is_active = TRUE")
                _cache["vpa_rules"] = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

                # Load NACH rules
                cur.execute("SELECT keyword, merchant_canonical, category, treatment FROM nach_rules WHERE is_active = TRUE")
                _cache["nach_rules"] = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

                _cache["last_loaded"] = now

        # Merge with bank_config.py as fallback
        # DB takes priority — only add from config if not in DB
        for email, bank in BANK_SENDERS.items():
            if email not in _cache["bank_senders"]:
                _cache["bank_senders"][email] = bank

        for keyword, result in VPA_MERCHANT_MAP.items():
            if keyword not in _cache["vpa_rules"] and result is not None:
                _cache["vpa_rules"][keyword] = result

        for keyword, result in NACH_MERCHANT_MAP.items():
            if keyword not in _cache["nach_rules"]:
                _cache["nach_rules"][keyword] = result

    except Exception as e:
        print(f"Rules load error: {str(e)[:100]}")
        # Fallback to bank_config.py entirely
        _cache["bank_senders"] = dict(BANK_SENDERS)
        _cache["vpa_rules"] = {k: v for k, v in VPA_MERCHANT_MAP.items() if v}
        _cache["nach_rules"] = dict(NACH_MERCHANT_MAP)
        _cache["last_loaded"] = now

def force_refresh():
    """Force reload rules from DB."""
    _cache["last_loaded"] = 0
    load_rules()

# ── Rule lookups ──────────────────────────────────────────────

def get_bank_from_sender(sender_email: str) -> str:
    """Identify bank from sender email."""
    load_rules()
    senders = _cache["bank_senders"]

    # Exact match
    if sender_email in senders:
        return senders[sender_email]

    # Partial match
    for known, bank in senders.items():
        if known in sender_email:
            return bank

    # Keyword match from bank_config
    return None

def get_merchant_from_vpa(vpa: str) -> tuple:
    """
    Resolve VPA to merchant.
    Returns (canonical, category, treatment, person_name) or None
    """
    load_rules()
    if not vpa:
        return None

    handle = vpa.split("@")[0].lower()
    handle_clean = re.sub(r'[^a-z0-9]', '', handle)

    # Check VPA rules - keyword match on handle
    vpa_rules = _cache["vpa_rules"]
    for keyword, result in vpa_rules.items():
        if keyword in handle_clean or keyword in handle:
            return result[0], result[1], result[2], ""

    # Person detection
    is_phone = bool(re.match(r'^\d{10}$', handle))
    is_name = bool(re.match(r'^[a-z]+[.\-][a-z]+\d{0,4}$', handle))

    if is_phone or is_name:
        name = f"****{handle[-4:]}" if is_phone else re.sub(r'[._\-]', ' ', handle).title()
        return None, None, None, name  # Person signal

    return None

def get_merchant_from_nach(body: str) -> tuple:
    """Resolve NACH/mandate to merchant."""
    load_rules()
    body_lower = body.lower()
    nach_rules = _cache["nach_rules"]

    for keyword, result in nach_rules.items():
        if keyword in body_lower:
            return result[0], result[1], result[2]

    return None

def get_bank_from_keywords(subject: str, body: str) -> str:
    """Identify bank from subject/body keywords."""
    text = (subject + " " + body[:500]).lower()
    for bank, keywords in BANK_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return bank
    return None

# ── Admin rule management ──────────────────────────────────────

def add_vpa_rule(keyword: str, merchant: str, category: str, treatment: str = "spend") -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO vpa_rules (keyword, merchant_canonical, category, treatment, source)
                    VALUES (%s, %s, %s, %s, 'admin')
                    ON CONFLICT (keyword) DO UPDATE SET
                        merchant_canonical = EXCLUDED.merchant_canonical,
                        category = EXCLUDED.category,
                        treatment = EXCLUDED.treatment,
                        is_active = TRUE
                """, (keyword.lower(), merchant, category, treatment))
                conn.commit()
        force_refresh()
        return True
    except Exception as e:
        print(f"Add VPA rule error: {e}")
        return False

def add_nach_rule(keyword: str, merchant: str, category: str, treatment: str = "spend") -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO nach_rules (keyword, merchant_canonical, category, treatment, source)
                    VALUES (%s, %s, %s, %s, 'admin')
                    ON CONFLICT (keyword) DO UPDATE SET
                        merchant_canonical = EXCLUDED.merchant_canonical,
                        category = EXCLUDED.category,
                        treatment = EXCLUDED.treatment,
                        is_active = TRUE
                """, (keyword.lower(), merchant, category, treatment))
                conn.commit()
        force_refresh()
        return True
    except Exception as e:
        print(f"Add NACH rule error: {e}")
        return False

def add_bank_sender(sender_email: str, bank_name: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bank_senders (sender_email, bank_name, source)
                    VALUES (%s, %s, 'admin')
                    ON CONFLICT (sender_email) DO UPDATE SET
                        bank_name = EXCLUDED.bank_name,
                        is_active = TRUE
                """, (sender_email.lower(), bank_name))
                conn.commit()
        force_refresh()
        return True
    except Exception as e:
        print(f"Add sender error: {e}")
        return False

def list_rules(rule_type: str) -> list:
    """List rules for admin review."""
    load_rules()
    if rule_type == "vpa":
        return [(k, v[0], v[1], v[2]) for k, v in _cache["vpa_rules"].items()]
    elif rule_type == "nach":
        return [(k, v[0], v[1], v[2]) for k, v in _cache["nach_rules"].items()]
    elif rule_type == "senders":
        return [(k, v) for k, v in _cache["bank_senders"].items()]
    return []

def get_rule_stats() -> dict:
    """Return count of all rules."""
    load_rules()
    return {
        "bank_senders": len(_cache["bank_senders"]),
        "vpa_rules": len(_cache["vpa_rules"]),
        "nach_rules": len(_cache["nach_rules"]),
    }
