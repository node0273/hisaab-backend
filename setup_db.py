"""
Hisaab — Complete DB setup
Run once: python setup_db.py
"""
import psycopg2, os, sys

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("Set DATABASE_URL first!")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ── 1. users ──────────────────────────────────────────────────
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── 2. gmail_accounts ─────────────────────────────────────────
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

# ── 3. transactions ───────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    bank VARCHAR(50),
    mode VARCHAR(50),
    amount FLOAT NOT NULL,
    merchant_canonical VARCHAR(255),
    category VARCHAR(100),
    treatment VARCHAR(20) DEFAULT 'spend',
    transaction_date DATE,
    vpa VARCHAR(255),
    person_name VARCHAR(255),
    upi_app VARCHAR(100),
    email_message_id VARCHAR(255),
    gmail_account VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, email_message_id)
);
CREATE INDEX IF NOT EXISTS idx_txn_user_date 
    ON transactions(user_id, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_txn_category 
    ON transactions(user_id, category, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_txn_merchant 
    ON transactions(user_id, merchant_canonical, transaction_date DESC);
""")

# ── 4. bank_senders ───────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS bank_senders (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) UNIQUE NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    source VARCHAR(20) DEFAULT 'seed',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── 5. negative_rules ─────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS negative_rules (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) UNIQUE NOT NULL,
    reason VARCHAR(255),
    source VARCHAR(20) DEFAULT 'ai',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── 6. parsing_rules ──────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS parsing_rules (
    id SERIAL PRIMARY KEY,
    bank VARCHAR(50) NOT NULL,
    mode VARCHAR(50) NOT NULL,
    subject_pattern VARCHAR(500),
    amount_pattern VARCHAR(500),
    merchant_pattern VARCHAR(500),
    vpa_pattern VARCHAR(500),
    sample_subject TEXT,
    status VARCHAR(20) DEFAULT 'pending',
    pending_since TIMESTAMPTZ DEFAULT NOW(),
    source VARCHAR(20) DEFAULT 'ai',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── 7. merchant_rules ─────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS merchant_rules (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(255) UNIQUE NOT NULL,
    merchant_canonical VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    treatment VARCHAR(20) DEFAULT 'spend',
    status VARCHAR(20) DEFAULT 'approved',
    pending_since TIMESTAMPTZ,
    source VARCHAR(20) DEFAULT 'seed',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merchant_rules_keyword 
    ON merchant_rules(keyword);
""")

# ── 8. conversations ──────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS conversations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conv_user 
    ON conversations(user_id, created_at DESC);
""")

# ── Supporting tables ─────────────────────────────────────────
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

CREATE TABLE IF NOT EXISTS rate_limits (
    user_id VARCHAR(50) PRIMARY KEY,
    message_count INTEGER DEFAULT 0,
    window_start TIMESTAMPTZ DEFAULT NOW()
);
""")

conn.commit()

# ── Seed bank_senders ─────────────────────────────────────────
bank_senders = [
    # HDFC
    ("alerts@hdfcbank.net",             "HDFC"),
    ("hdfcbank@hdfcbank.net",           "HDFC"),
    ("nachautoemailer@hdfcbank.net",    "HDFC"),
    # HSBC
    ("hsbc@hsbc.co.in",                 "HSBC"),
    ("hsbc@mail.hsbc.co.in",            "HSBC"),
    ("hsbc@mandatehq.com",              "HSBC"),
    # ICICI
    ("alerts@icicibank.com",            "ICICI"),
    ("icicibank@icicibank.com",         "ICICI"),
    # Axis
    ("axisindiaalerts@axisbank.com",    "Axis"),
    ("alerts@axisbank.com",             "Axis"),
    # SBI
    ("sbialerts@sbi.co.in",             "SBI"),
    ("alerts@sbi.co.in",                "SBI"),
    ("alerts@sbicard.com",              "SBI"),
    # Kotak
    ("alerts@kotak.com",                "Kotak"),
    ("kotak@kotak.com",                 "Kotak"),
    # Yes Bank
    ("alerts@yesbank.in",               "Yes Bank"),
    # IDFC First
    ("alerts@idfcfirstbank.com",        "IDFC First"),
    # IndusInd
    ("alerts@indusind.com",             "IndusInd"),
    # Federal Bank
    ("alerts@federalbank.co.in",        "Federal Bank"),
    # RBL
    ("alerts@rblbank.com",              "RBL"),
    # Bandhan
    ("alerts@bandhanbank.com",          "Bandhan"),
    # Public Banks
    ("alerts@bankofbaroda.com",         "Bank of Baroda"),
    ("alerts@pnb.co.in",               "PNB"),
    ("alerts@canarabank.in",            "Canara Bank"),
    ("alerts@unionbankofindia.com",     "Union Bank"),
    ("alerts@idbi.co.in",               "IDBI"),
    ("alerts@bankofindia.co.in",        "Bank of India"),
    ("alerts@centralbankofindia.co.in", "Central Bank"),
    ("alerts@indianbank.in",            "Indian Bank"),
    ("alerts@ucobank.com",              "UCO Bank"),
    ("alerts@psbindia.com",             "Punjab & Sind Bank"),
    # Foreign Banks
    ("alerts@sc.com",                   "Standard Chartered"),
    ("scalerts@sc.com",                 "Standard Chartered"),
    ("citibank@citi.com",               "Citibank"),
    ("alerts@dbs.com",                  "DBS"),
    ("alerts@db.com",                   "Deutsche Bank"),
    # Regional
    ("alerts@southindianbank.com",      "South Indian Bank"),
    ("alerts@kvb.co.in",               "Karur Vysya"),
    ("alerts@cityunionbank.com",        "City Union Bank"),
]

for email, bank in bank_senders:
    cur.execute("""
        INSERT INTO bank_senders (sender_email, bank_name, source)
        VALUES (%s, %s, 'seed') ON CONFLICT (sender_email) DO NOTHING
    """, (email, bank))

# ── Seed merchant_rules ───────────────────────────────────────
merchant_rules = [
    # Food & Dining
    ("zomato",          "Zomato",           "Food & Dining",        "spend"),
    ("swiggy",          "Swiggy",           "Food & Dining",        "spend"),
    ("dominos",         "Dominos",          "Food & Dining",        "spend"),
    ("dominopizza",     "Dominos",          "Food & Dining",        "spend"),
    ("mcdonalds",       "McDonald's",       "Food & Dining",        "spend"),
    ("kfc",             "KFC",              "Food & Dining",        "spend"),
    ("pizzahut",        "Pizza Hut",        "Food & Dining",        "spend"),
    ("starbucks",       "Starbucks",        "Food & Dining",        "spend"),
    ("burgerking",      "Burger King",      "Food & Dining",        "spend"),
    ("subway",          "Subway",           "Food & Dining",        "spend"),
    ("eatsure",         "EatSure",          "Food & Dining",        "spend"),
    ("foodsquare",      "Food Square",      "Food & Dining",        "spend"),
    ("barbeque",        "Barbeque Nation",  "Food & Dining",        "spend"),
    ("behrouz",         "Behrouz Biryani",  "Food & Dining",        "spend"),
    ("fasoos",          "Faasos",           "Food & Dining",        "spend"),
    # Groceries
    ("blinkit",         "Blinkit",          "Groceries",            "spend"),
    ("bigbasket",       "BigBasket",        "Groceries",            "spend"),
    ("zepto",           "Zepto",            "Groceries",            "spend"),
    ("jiomart",         "JioMart",          "Groceries",            "spend"),
    ("dmart",           "DMart",            "Groceries",            "spend"),
    ("grofers",         "Blinkit",          "Groceries",            "spend"),
    ("dunzo",           "Dunzo",            "Groceries",            "spend"),
    ("swiggyinstamart", "Swiggy Instamart", "Groceries",            "spend"),
    # Shopping
    ("amazon",          "Amazon",           "Shopping",             "spend"),
    ("flipkart",        "Flipkart",         "Shopping",             "spend"),
    ("myntra",          "Myntra",           "Shopping",             "spend"),
    ("ajio",            "Ajio",             "Shopping",             "spend"),
    ("meesho",          "Meesho",           "Shopping",             "spend"),
    ("nykaa",           "Nykaa",            "Shopping",             "spend"),
    ("snapdeal",        "Snapdeal",         "Shopping",             "spend"),
    ("tatacliq",        "Tata CLiQ",        "Shopping",             "spend"),
    # Travel & Transport
    ("uber",            "Uber",             "Travel & Transport",   "spend"),
    ("olacabs",         "Ola",              "Travel & Transport",   "spend"),
    ("rapido",          "Rapido",           "Travel & Transport",   "spend"),
    ("irctc",           "IRCTC",            "Travel & Transport",   "spend"),
    ("makemytrip",      "MakeMyTrip",       "Travel & Transport",   "spend"),
    ("goibibo",         "Goibibo",          "Travel & Transport",   "spend"),
    ("indigo",          "IndiGo",           "Travel & Transport",   "spend"),
    ("airindia",        "Air India",        "Travel & Transport",   "spend"),
    ("spicejet",        "SpiceJet",         "Travel & Transport",   "spend"),
    ("vistara",         "Vistara",          "Travel & Transport",   "spend"),
    ("ixigo",           "Ixigo",            "Travel & Transport",   "spend"),
    ("redbus",          "RedBus",           "Travel & Transport",   "spend"),
    ("pranaam",         "Pranaam",          "Travel & Transport",   "spend"),
    # Fuel
    ("indianoil",       "Indian Oil",       "Fuel",                 "spend"),
    ("iocl",            "Indian Oil",       "Fuel",                 "spend"),
    ("hpcl",            "HPCL",             "Fuel",                 "spend"),
    ("bpcl",            "BPCL",             "Fuel",                 "spend"),
    # Entertainment & OTT
    ("netflix",         "Netflix",          "Entertainment & OTT",  "spend"),
    ("hotstar",         "Hotstar",          "Entertainment & OTT",  "spend"),
    ("primevideo",      "Amazon Prime",     "Entertainment & OTT",  "spend"),
    ("spotify",         "Spotify",          "Entertainment & OTT",  "spend"),
    ("sonyliv",         "Sony LIV",         "Entertainment & OTT",  "spend"),
    ("zee5",            "Zee5",             "Entertainment & OTT",  "spend"),
    ("jiocinema",       "JioCinema",        "Entertainment & OTT",  "spend"),
    ("bookmyshow",      "BookMyShow",       "Entertainment & OTT",  "spend"),
    ("youtube",         "YouTube Premium",  "Entertainment & OTT",  "spend"),
    # Health & Medical
    ("apollopharmacy",  "Apollo Pharmacy",  "Health & Medical",     "spend"),
    ("apollo",          "Apollo",           "Health & Medical",     "spend"),
    ("pharmeasy",       "PharmEasy",        "Health & Medical",     "spend"),
    ("1mg",             "1mg",              "Health & Medical",     "spend"),
    ("practo",          "Practo",           "Health & Medical",     "spend"),
    ("medplus",         "MedPlus",          "Health & Medical",     "spend"),
    # Utilities & Bills
    ("airtel",          "Airtel",           "Utilities & Bills",    "spend"),
    ("jio",             "Jio",              "Utilities & Bills",    "spend"),
    ("bsnl",            "BSNL",             "Utilities & Bills",    "spend"),
    ("vodafone",        "Vi",               "Utilities & Bills",    "spend"),
    ("tatapower",       "Tata Power",       "Utilities & Bills",    "spend"),
    ("bses",            "BSES",             "Utilities & Bills",    "spend"),
    ("adani",           "Adani Electricity","Utilities & Bills",    "spend"),
    # Subscriptions
    ("google",          "Google",           "Subscriptions",        "spend"),
    ("apple",           "Apple",            "Subscriptions",        "spend"),
    ("linkedin",        "LinkedIn",         "Subscriptions",        "spend"),
    ("microsoft",       "Microsoft",        "Subscriptions",        "spend"),
    ("canva",           "Canva",            "Subscriptions",        "spend"),
    ("dropbox",         "Dropbox",          "Subscriptions",        "spend"),
    # Investments
    ("zerodha",         "Zerodha",          "Investments & Finance","investment"),
    ("groww",           "Groww",            "Investments & Finance","investment"),
    ("upstox",          "Upstox",           "Investments & Finance","investment"),
    ("kuvera",          "Kuvera",           "Investments & Finance","investment"),
    ("etmoney",         "ETMoney",          "Investments & Finance","investment"),
    ("bse",             "BSE/MF SIP",       "Investments & Finance","investment"),
    ("bse limited",     "BSE/MF SIP",       "Investments & Finance","investment"),
    ("nse",             "NSE/MF SIP",       "Investments & Finance","investment"),
    ("cams",            "CAMS/MF",          "Investments & Finance","investment"),
    ("kfintech",        "KFintech/MF",      "Investments & Finance","investment"),
    ("mfcentral",       "MF Central",       "Investments & Finance","investment"),
    # Insurance
    ("lic",             "LIC",              "Insurance",            "spend"),
    ("licpremium",      "LIC",              "Insurance",            "spend"),
    ("hdfclife",        "HDFC Life",        "Insurance",            "spend"),
    ("sbilife",         "SBI Life",         "Insurance",            "spend"),
    ("icicipruli",      "ICICI Pru Life",   "Insurance",            "spend"),
    ("starhealth",      "Star Health",      "Insurance",            "spend"),
    ("niacl",           "New India Assurance","Insurance",          "spend"),
    # Education
    ("byjus",           "BYJU'S",           "Education",            "spend"),
    ("unacademy",       "Unacademy",        "Education",            "spend"),
    ("vedantu",         "Vedantu",          "Education",            "spend"),
    ("coursera",        "Coursera",         "Education",            "spend"),
    ("udemy",           "Udemy",            "Education",            "spend"),
    # Settlements
    ("credit card payment","CC Payment",    "Credit Card Payment",  "settlement"),
    ("cc payment",      "CC Payment",       "Credit Card Payment",  "settlement"),
    ("home loan",       "Home Loan EMI",    "EMI & Loans",          "settlement"),
    ("car loan",        "Car Loan EMI",     "EMI & Loans",          "settlement"),
    ("personal loan",   "Personal Loan",    "EMI & Loans",          "settlement"),
    ("emi",             "EMI Payment",      "EMI & Loans",          "settlement"),
    # Excluded
    ("salary",          "Salary",           "Income & Salary",      "excluded"),
    ("cashback",        "Cashback",         "Cashback",             "excluded"),
]

for keyword, canonical, category, treatment in merchant_rules:
    cur.execute("""
        INSERT INTO merchant_rules 
            (keyword, merchant_canonical, category, treatment, status, source)
        VALUES (%s, %s, %s, %s, 'approved', 'seed')
        ON CONFLICT (keyword) DO NOTHING
    """, (keyword, canonical, category, treatment))

conn.commit()
cur.close()
conn.close()

print("✅ Database setup complete!")
print(f"   Tables: users, gmail_accounts, transactions, bank_senders")
print(f"   Tables: negative_rules, parsing_rules, merchant_rules, conversations")
print(f"   Tables: gmail_sync_log, rate_limits")
print(f"   Seeded: {len(bank_senders)} bank senders, {len(merchant_rules)} merchant rules")
