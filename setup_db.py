"""
Hisaab Database — Complete schema v2
Run once: python setup_db.py
"""
import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL first! Example:\n$env:DATABASE_URL=\"postgresql://...\"")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ── Users ─────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    platform VARCHAR(20) DEFAULT 'telegram',
    name VARCHAR(255),
    consent_given BOOLEAN DEFAULT FALSE,
    ai_consent_given BOOLEAN DEFAULT FALSE,
    onboarded BOOLEAN DEFAULT FALSE,
    last_active TIMESTAMPTZ DEFAULT NOW(),
    device_id VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── Gmail accounts (multiple per user) ───────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS gmail_accounts (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    access_token_enc TEXT,
    refresh_token_enc TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, email)
);
""")

# ── Categories ────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    emoji VARCHAR(10),
    treatment VARCHAR(20) DEFAULT 'spend',
    display_order INTEGER
);
""")

categories = [
    ("Food & Dining",           "🍽️", "spend",      1),
    ("Groceries",               "🛒", "spend",      2),
    ("Shopping",                "🛍️", "spend",      3),
    ("Travel & Transport",      "🚗", "spend",      4),
    ("Fuel",                    "⛽", "spend",      5),
    ("Entertainment & OTT",     "🎬", "spend",      6),
    ("Health & Medical",        "🏥", "spend",      7),
    ("Utilities & Bills",       "💡", "spend",      8),
    ("Subscriptions",           "📱", "spend",      9),
    ("Education",               "📚", "spend",      10),
    ("Rent",                    "🏠", "spend",      11),
    ("Daily Spend",             "💰", "spend",      12),
    ("P2P Transfer",            "👤", "spend",      13),
    ("Insurance",               "🛡️", "spend",      14),
    ("EMI & Loans",             "💳", "settlement", 15),
    ("Credit Card Payment",     "🏦", "settlement", 16),
    ("Investments & Finance",   "📈", "investment", 17),
    ("Refund",                  "↩️", "refund",     18),
    ("Income & Salary",         "💵", "excluded",   19),
    ("Cashback",                "🎁", "excluded",   20),
    ("Inter-account Transfer",  "🔄", "excluded",   21),
    ("Other",                   "📦", "spend",      22),
]

for name, emoji, treatment, order in categories:
    cur.execute("""
        INSERT INTO categories (name, emoji, treatment, display_order)
        VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO UPDATE
        SET treatment = EXCLUDED.treatment, emoji = EXCLUDED.emoji
    """, (name, emoji, treatment, order))

# ── Merchants ─────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS merchants (
    id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100) NOT NULL,
    treatment VARCHAR(20) DEFAULT 'spend',
    aliases TEXT[] DEFAULT '{}',
    vpa_patterns TEXT[] DEFAULT '{}',
    ai_classified BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) DEFAULT 'approved',
    pending_since TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
""")

merchants = [
    # Food & Dining
    ("Zomato",          "Food & Dining",        "spend",      ["ZOMATO","Pay*Zomato","ZOMATO PVT LTD","Zomato Internet"],          ["zomato@icici","zomato@hdfcbank","zomato.in@axisbank"]),
    ("Swiggy",          "Food & Dining",        "spend",      ["SWIGGY","Bundl Technologies","BUNDL TECHNOLOGIES"],                 ["swiggy@icici","swiggy.in@axisbank"]),
    ("Dominos",         "Food & Dining",        "spend",      ["DOMINOS","DOMINO S PIZZA","Jubilant FoodWorks"],                    ["dominos@icici"]),
    ("McDonald's",      "Food & Dining",        "spend",      ["MCDONALDS","MCD","Hardcastle Restaurants"],                        ["mcdonalds@icici"]),
    ("Starbucks",       "Food & Dining",        "spend",      ["STARBUCKS","Tata Starbucks"],                                      ["starbucks@icici"]),
    ("KFC",             "Food & Dining",        "spend",      ["KFC","Devyani International"],                                     ["kfc@icici"]),
    ("Pizza Hut",       "Food & Dining",        "spend",      ["PIZZA HUT","Sapphire Foods"],                                      ["pizzahut@icici"]),
    ("Barbeque Nation", "Food & Dining",        "spend",      ["BARBEQUE NATION","BBQ NATION"],                                    []),
    # Groceries
    ("BigBasket",       "Groceries",            "spend",      ["BIGBASKET","BIG BASKET","Supermarket Grocery"],                    ["bigbasket@icici","bigbasket@hdfcbank"]),
    ("Blinkit",         "Groceries",            "spend",      ["BLINKIT","GROFERS","Grofers India"],                               ["blinkit@icici"]),
    ("Zepto",           "Groceries",            "spend",      ["ZEPTO","KiranaKart"],                                              ["zepto@icici"]),
    ("JioMart",         "Groceries",            "spend",      ["JIOMART","Reliance Retail"],                                       ["jiomart@icici"]),
    ("DMart",           "Groceries",            "spend",      ["DMART","Avenue Supermarts","D MART"],                              []),
    ("Dunzo",           "Groceries",            "spend",      ["DUNZO"],                                                           ["dunzo@icici"]),
    # Shopping
    ("Amazon",          "Shopping",             "spend",      ["AMAZON","Amazon Seller","AMZN"],                                   ["amazon@icici","amazonpay@icici"]),
    ("Flipkart",        "Shopping",             "spend",      ["FLIPKART","Flipkart Internet"],                                    ["flipkart@icici","fkaxis@axisbank"]),
    ("Myntra",          "Shopping",             "spend",      ["MYNTRA","Myntra Designs"],                                         ["myntra@icici"]),
    ("Ajio",            "Shopping",             "spend",      ["AJIO","Reliance Industries"],                                      ["ajio@icici"]),
    ("Meesho",          "Shopping",             "spend",      ["MEESHO","Fashnear Technologies"],                                  ["meesho@icici"]),
    ("Nykaa",           "Shopping",             "spend",      ["NYKAA","FSN E-Commerce"],                                          ["nykaa@icici"]),
    # Travel & Transport
    ("Uber",            "Travel & Transport",   "spend",      ["UBER","Uber India","UBER INDIA SYSTEMS"],                          ["uber@icici","uber@hdfcbank"]),
    ("Ola",             "Travel & Transport",   "spend",      ["OLA","ANI Technologies","Ola Cabs"],                               ["ola@icici","olamoney@icici"]),
    ("Rapido",          "Travel & Transport",   "spend",      ["RAPIDO","Roppen Transportation"],                                  ["rapido@icici"]),
    ("IRCTC",           "Travel & Transport",   "spend",      ["IRCTC","Indian Railway Catering"],                                 ["irctc@irctc","irctc@sbi"]),
    ("MakeMyTrip",      "Travel & Transport",   "spend",      ["MAKEMYTRIP","MakeMyTrip India","MMT"],                             ["makemytrip@icici"]),
    ("Ixigo",           "Travel & Transport",   "spend",      ["IXIGO","Le Travenues"],                                            ["ixigo@icici"]),
    ("IndiGo",          "Travel & Transport",   "spend",      ["INDIGO","InterGlobe Aviation"],                                    ["indigo@icici"]),
    ("Air India",       "Travel & Transport",   "spend",      ["AIR INDIA","AIRINDIA"],                                            ["airindia@icici"]),
    # Fuel
    ("Indian Oil",      "Fuel",                 "spend",      ["INDIAN OIL","IOCL","IndianOil"],                                   []),
    ("HPCL",            "Fuel",                 "spend",      ["HPCL","Hindustan Petroleum","HP PETROL"],                          []),
    ("BPCL",            "Fuel",                 "spend",      ["BPCL","Bharat Petroleum","BP PETROL"],                             []),
    # Entertainment & OTT
    ("Netflix",         "Entertainment & OTT",  "spend",      ["NETFLIX","Netflix Inc"],                                           ["netflix@icici"]),
    ("Hotstar",         "Entertainment & OTT",  "spend",      ["HOTSTAR","Disney Hotstar","DISNEY PLUS","Star India"],             ["hotstar@icici"]),
    ("Amazon Prime",    "Entertainment & OTT",  "spend",      ["AMAZON PRIME","Prime Video"],                                      ["primevideo@icici"]),
    ("Spotify",         "Entertainment & OTT",  "spend",      ["SPOTIFY","Spotify AB"],                                            ["spotify@icici"]),
    ("YouTube Premium", "Entertainment & OTT",  "spend",      ["YOUTUBE PREMIUM","Google YouTube"],                                ["youtube@icici"]),
    ("Sony LIV",        "Entertainment & OTT",  "spend",      ["SONYLIV","Sony Pictures"],                                         ["sonyliv@icici"]),
    ("Zee5",            "Entertainment & OTT",  "spend",      ["ZEE5","Zee Entertainment"],                                        ["zee5@icici"]),
    # Health & Medical
    ("Apollo Pharmacy", "Health & Medical",     "spend",      ["APOLLO PHARMACY","Apollo Health","APOLLO MEDICALS"],               ["apollopharmacy@icici"]),
    ("PharmEasy",       "Health & Medical",     "spend",      ["PHARMEASY","Docon Technologies"],                                  ["pharmeasy@icici"]),
    ("1mg",             "Health & Medical",     "spend",      ["1MG","Tata 1mg","HEALTHKART"],                                     ["1mg@icici"]),
    ("Practo",          "Health & Medical",     "spend",      ["PRACTO","Practo Technologies"],                                    ["practo@icici"]),
    # Utilities & Bills
    ("Airtel",          "Utilities & Bills",    "spend",      ["AIRTEL","Bharti Airtel","BHARTI AIRTEL"],                          ["airtel@airtel","airtel@icici"]),
    ("Jio",             "Utilities & Bills",    "spend",      ["JIO","Reliance Jio","RJIL"],                                       ["jio@icici","jio@hdfcbank"]),
    ("BSES",            "Utilities & Bills",    "spend",      ["BSES","BSES Rajdhani","BSES YAMUNA"],                              []),
    ("Tata Power",      "Utilities & Bills",    "spend",      ["TATA POWER","TATA ELECTRIC"],                                      []),
    ("Vi",              "Utilities & Bills",    "spend",      ["VODAFONE","IDEA","Vi","Vodafone Idea"],                             ["vi@icici"]),
    ("BSNL",            "Utilities & Bills",    "spend",      ["BSNL","Bharat Sanchar"],                                           []),
    # Subscriptions
    ("Google One",      "Subscriptions",        "spend",      ["GOOGLE ONE","Google Storage"],                                     ["google@icici"]),
    ("iCloud",          "Subscriptions",        "spend",      ["ICLOUD","Apple iCloud","APPLE.COM"],                               []),
    ("LinkedIn Premium","Subscriptions",        "spend",      ["LINKEDIN","LinkedIn Premium"],                                     ["linkedin@icici"]),
    ("Canva",           "Subscriptions",        "spend",      ["CANVA"],                                                           ["canva@icici"]),
    # Investments
    ("Zerodha",         "Investments & Finance","investment",  ["ZERODHA","Zerodha Broking"],                                       ["zerodha@icici"]),
    ("Groww",           "Investments & Finance","investment",  ["GROWW","Nextbillion Technology"],                                  ["groww@icici"]),
    ("Upstox",          "Investments & Finance","investment",  ["UPSTOX","RKSV Securities"],                                       ["upstox@icici"]),
    ("ETMoney",         "Investments & Finance","investment",  ["ETMONEY","Times Internet"],                                        ["etmoney@icici"]),
    ("Kuvera",          "Investments & Finance","investment",  ["KUVERA"],                                                          ["kuvera@icici"]),
    # Insurance
    ("LIC",             "Insurance",            "spend",      ["LIC","Life Insurance Corporation","LIC OF INDIA"],                 ["lic@icici"]),
    ("HDFC Life",       "Insurance",            "spend",      ["HDFC LIFE","HDFC Standard Life"],                                  []),
    ("Star Health",     "Insurance",            "spend",      ["STAR HEALTH","Star Health Insurance"],                             []),
    ("PolicyBazaar",    "Insurance",            "spend",      ["POLICYBAZAAR","PB Fintech"],                                       ["policybazaar@icici"]),
    # Education
    ("BYJU'S",          "Education",            "spend",      ["BYJUS","Think & Learn","BYJU S"],                                  ["byjus@icici"]),
    ("Unacademy",       "Education",            "spend",      ["UNACADEMY","Sorting Hat Technologies"],                            ["unacademy@icici"]),
    ("Coursera",        "Education",            "spend",      ["COURSERA"],                                                        ["coursera@icici"]),
    ("Udemy",           "Education",            "spend",      ["UDEMY"],                                                           ["udemy@icici"]),
    # Settlements - CC payments
    ("HDFC Credit Card","Credit Card Payment",  "settlement", ["HDFC CREDIT CARD","HDFC CC PAYMENT","HDFC CREDITCARD"],            []),
    ("ICICI Credit Card","Credit Card Payment", "settlement", ["ICICI CREDIT CARD","ICICI CC PAYMENT"],                            []),
    ("Axis Credit Card","Credit Card Payment",  "settlement", ["AXIS CREDIT CARD","AXIS CC PAYMENT"],                              []),
    ("SBI Credit Card", "Credit Card Payment",  "settlement", ["SBI CREDIT CARD","SBI CC PAYMENT","SBI CARD"],                     []),
    ("HSBC Credit Card","Credit Card Payment",  "settlement", ["HSBC CREDIT CARD","HSBC CC PAYMENT"],                              []),
]

for canonical, category, treatment, aliases, vpas in merchants:
    cur.execute("""
        INSERT INTO merchants (canonical_name, category, treatment, aliases, vpa_patterns, status)
        VALUES (%s, %s, %s, %s, %s, 'approved')
        ON CONFLICT (canonical_name) DO UPDATE SET
            category = EXCLUDED.category,
            treatment = EXCLUDED.treatment,
            aliases = EXCLUDED.aliases,
            vpa_patterns = EXCLUDED.vpa_patterns,
            updated_at = NOW()
    """, (canonical, category, treatment, aliases, vpas))

# ── Transactions ──────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    gmail_account VARCHAR(255),
    bank VARCHAR(50),
    mode VARCHAR(50),
    amount FLOAT,
    merchant_raw_enc TEXT,
    merchant_canonical VARCHAR(255),
    category VARCHAR(100),
    treatment VARCHAR(20) DEFAULT 'spend',
    vpa_enc TEXT,
    person_name VARCHAR(255),
    transaction_date DATE,
    email_message_id VARCHAR(255),
    ai_classified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, email_message_id)
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_date
ON transactions(user_id, transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_category
ON transactions(user_id, category, transaction_date DESC);
""")

# ── Gmail sync log ────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS gmail_sync_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    gmail_account VARCHAR(255) NOT NULL,
    last_synced_at TIMESTAMPTZ DEFAULT NOW(),
    emails_processed INTEGER DEFAULT 0,
    transactions_found INTEGER DEFAULT 0,
    UNIQUE(user_id, gmail_account)
);
""")

# ── Conversations (30-day auto-delete) ────────────────────────
cur.execute("""
DROP TABLE IF EXISTS conversations CASCADE;
CREATE TABLE conversations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_conversations_user ON conversations(user_id, created_at DESC);
""")

# ── Rate limiting ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS rate_limits (
    user_id VARCHAR(50) PRIMARY KEY,
    message_count INTEGER DEFAULT 0,
    window_start TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── Admin audit log ───────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    action VARCHAR(100),
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── Auto-delete conversations older than 30 days ─────────────
cur.execute("""
CREATE OR REPLACE FUNCTION delete_old_conversations()
RETURNS void AS $$
BEGIN
    DELETE FROM conversations WHERE created_at < NOW() - INTERVAL '30 days';
    DELETE FROM transactions WHERE transaction_date < NOW() - INTERVAL '12 months';
END;
$$ LANGUAGE plpgsql;
""")

conn.commit()
cur.close()
conn.close()
print("✅ Hisaab DB v2 setup complete!")
print("   Tables: users, gmail_accounts, categories, merchants, transactions")
print("   Tables: gmail_sync_log, conversations, rate_limits, audit_log")
print(f"   Categories: {len(categories)} seeded")
print(f"   Merchants: {len(merchants)} seeded")

# ── Rules tables (added in v3) ────────────────────────────────
conn2 = psycopg2.connect(DATABASE_URL)
cur2 = conn2.cursor()
cur2.execute("""
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

CREATE TABLE IF NOT EXISTS learned_senders (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) UNIQUE NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    discovered_at TIMESTAMPTZ DEFAULT NOW()
);
""")
conn2.commit()
cur2.close()
conn2.close()
print("   Rules tables: bank_senders, vpa_rules, nach_rules, learned_senders")
