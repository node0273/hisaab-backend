"""
Create merchant_rules table and migrate vpa_rules + nach_rules into it.
Run once: python setup_merchant_rules.py
"""
import psycopg2, os

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ── Create merchant_rules table ───────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS merchant_rules (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(255) UNIQUE NOT NULL,
    merchant_canonical VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    treatment VARCHAR(20) DEFAULT 'spend',
    applies_to TEXT[] DEFAULT '{all}',
    source VARCHAR(20) DEFAULT 'seed',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merchant_rules_keyword ON merchant_rules(keyword);
""")
conn.commit()
print("✅ merchant_rules table created")

# ── Migrate vpa_rules ─────────────────────────────────────────
cur.execute("SELECT keyword, merchant_canonical, category, treatment, source FROM vpa_rules")
vpa_rows = cur.fetchall()
migrated_vpa = 0
for r in vpa_rows:
    cur.execute("""
        INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
        VALUES (%s, %s, %s, %s, '{upi,all}', %s)
        ON CONFLICT (keyword) DO NOTHING
    """, (r[0], r[1], r[2], r[3], r[4] or 'seed'))
    migrated_vpa += cur.rowcount
conn.commit()
print(f"✅ Migrated {migrated_vpa} VPA rules")

# ── Migrate nach_rules ────────────────────────────────────────
cur.execute("SELECT keyword, merchant_canonical, category, treatment, source FROM nach_rules")
nach_rows = cur.fetchall()
migrated_nach = 0
for r in nach_rows:
    cur.execute("""
        INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
        VALUES (%s, %s, %s, %s, '{nach}', %s)
        ON CONFLICT (keyword) DO NOTHING
    """, (r[0], r[1], r[2], r[3], r[4] or 'seed'))
    migrated_nach += cur.rowcount
conn.commit()
print(f"✅ Migrated {migrated_nach} NACH rules")

# ── Seed additional credit card + debit card merchant rules ───
cc_rules = [
    # Credit Card — these appear as "payment to MERCHANT" or "PYU*MERCHANT"
    ("pyu*zomato",      "Zomato",           "Food & Dining",        "spend",      ["credit_card","all"]),
    ("pyu*swiggy",      "Swiggy",           "Food & Dining",        "spend",      ["credit_card","all"]),
    ("pyu*amazon",      "Amazon",           "Shopping",             "spend",      ["credit_card","all"]),
    ("pyu*flipkart",    "Flipkart",         "Shopping",             "spend",      ["credit_card","all"]),
    ("pyu*uber",        "Uber",             "Travel & Transport",   "spend",      ["credit_card","all"]),
    ("pyu*ola",         "Ola",              "Travel & Transport",   "spend",      ["credit_card","all"]),
    ("pyu*netflix",     "Netflix",          "Entertainment & OTT",  "spend",      ["credit_card","all"]),
    ("pyu*hotstar",     "Hotstar",          "Entertainment & OTT",  "spend",      ["credit_card","all"]),
    ("pyu*spotify",     "Spotify",          "Entertainment & OTT",  "spend",      ["credit_card","all"]),
    ("pyu*blinkit",     "Blinkit",          "Groceries",            "spend",      ["credit_card","all"]),
    ("pyu*bigbasket",   "BigBasket",        "Groceries",            "spend",      ["credit_card","all"]),
    ("pyu*irctc",       "IRCTC",            "Travel & Transport",   "spend",      ["credit_card","all"]),
    ("pyu*makemytrip",  "MakeMyTrip",       "Travel & Transport",   "spend",      ["credit_card","all"]),
    ("pyu*myntra",      "Myntra",           "Shopping",             "spend",      ["credit_card","all"]),
    ("pyu*indigo",      "IndiGo",           "Travel & Transport",   "spend",      ["credit_card","all"]),
    ("pyu*airindia",    "Air India",        "Travel & Transport",   "spend",      ["credit_card","all"]),
    # CC payment keywords
    ("credit card payment", "Credit Card Payment", "Credit Card Payment", "settlement", ["credit_card","net_banking"]),
    ("cc payment",      "Credit Card Payment", "Credit Card Payment","settlement", ["all"]),
    # Net Banking / Debit Card merchants
    ("amazon retail",   "Amazon",           "Shopping",             "spend",      ["debit_card","net_banking","all"]),
    ("flipkart internet","Flipkart",        "Shopping",             "spend",      ["debit_card","net_banking","all"]),
    ("hdfc life",       "HDFC Life",        "Insurance",            "spend",      ["net_banking","nach","all"]),
    ("sbi life",        "SBI Life",         "Insurance",            "spend",      ["net_banking","nach","all"]),
    ("lic of india",    "LIC",              "Insurance",            "spend",      ["net_banking","nach","all"]),
    ("lic premium",     "LIC",              "Insurance",            "spend",      ["nach","all"]),
    ("hdfc home loan",  "HDFC Home Loan",   "EMI & Loans",          "settlement", ["nach","all"]),
    ("home loan",       "Home Loan EMI",    "EMI & Loans",          "settlement", ["nach","all"]),
    ("car loan",        "Car Loan EMI",     "EMI & Loans",          "settlement", ["nach","all"]),
    ("personal loan",   "Personal Loan EMI","EMI & Loans",          "settlement", ["nach","all"]),
    ("salary",          "Salary Credit",    "Income & Salary",      "excluded",   ["net_banking","all"]),
    ("neft cr",         "NEFT Credit",      "Income & Salary",      "excluded",   ["net_banking"]),
]

seeded = 0
for kw, merchant, category, treatment, applies_to in cc_rules:
    cur.execute("""
        INSERT INTO merchant_rules (keyword, merchant_canonical, category, treatment, applies_to, source)
        VALUES (%s, %s, %s, %s, %s, 'seed')
        ON CONFLICT (keyword) DO NOTHING
    """, (kw, merchant, category, treatment, applies_to))
    seeded += cur.rowcount
conn.commit()
print(f"✅ Seeded {seeded} additional CC/debit/net banking rules")

# ── Summary ───────────────────────────────────────────────────
cur.execute("SELECT COUNT(*), applies_to FROM merchant_rules GROUP BY applies_to ORDER BY COUNT(*) DESC")
print("\n=== merchant_rules breakdown by applies_to ===")
for r in cur.fetchall():
    print(f"  {r[1]} → {r[0]} rules")

cur.execute("SELECT COUNT(*) FROM merchant_rules")
total = cur.fetchone()[0]
print(f"\n✅ Total merchant_rules: {total}")

cur.close()
conn.close()
