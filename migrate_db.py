"""
Hisaab DB Migration — fixes schema from v1 to v2
Run: python migrate_db.py
"""
import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL first!")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Starting migration...")

# ── Step 1: Check what columns exist in users table ───────────
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'users'
""")
existing_cols = [r[0] for r in cur.fetchall()]
print(f"Existing users columns: {existing_cols}")

# ── Step 2: Migrate users table ───────────────────────────────
if 'whatsapp_number' in existing_cols and 'user_id' not in existing_cols:
    print("Migrating users table: whatsapp_number → user_id")
    cur.execute("ALTER TABLE users RENAME COLUMN whatsapp_number TO user_id")
    conn.commit()
    print("✅ Renamed whatsapp_number to user_id")

if 'ai_consent_given' not in existing_cols:
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ai_consent_given BOOLEAN DEFAULT FALSE")
    conn.commit()
    print("✅ Added ai_consent_given column")

if 'last_active' not in existing_cols:
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active TIMESTAMPTZ DEFAULT NOW()")
    conn.commit()
    print("✅ Added last_active column")

if 'platform' not in existing_cols:
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS platform VARCHAR(20) DEFAULT 'telegram'")
    conn.commit()
    print("✅ Added platform column")

if 'device_id' not in existing_cols:
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS device_id VARCHAR(255)")
    conn.commit()
    print("✅ Added device_id column")

# ── Step 3: Create gmail_accounts from old users data ─────────
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'gmail_accounts'
""")
gmail_cols = [r[0] for r in cur.fetchall()]

if not gmail_cols:
    print("Creating gmail_accounts table...")
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
    )""")
    conn.commit()
    print("✅ Created gmail_accounts table")

# ── Step 4: Migrate existing token data to gmail_accounts ─────
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'users'
""")
current_cols = [r[0] for r in cur.fetchall()]
print(f"Current users columns after migration: {current_cols}")

if 'access_token_enc' in current_cols and 'email' in current_cols:
    print("Migrating token data to gmail_accounts...")
    cur.execute("""
        INSERT INTO gmail_accounts (user_id, email, name, access_token_enc, refresh_token_enc, is_active)
        SELECT user_id, email, name, access_token_enc, refresh_token_enc, TRUE
        FROM users
        WHERE email IS NOT NULL AND email != ''
        ON CONFLICT (user_id, email) DO NOTHING
    """)
    migrated = cur.rowcount
    conn.commit()
    print(f"✅ Migrated {migrated} user tokens to gmail_accounts")

    # Update onboarded flag
    cur.execute("""
        UPDATE users SET onboarded = TRUE
        WHERE user_id IN (
            SELECT DISTINCT user_id FROM gmail_accounts WHERE is_active = TRUE
        )
    """)
    conn.commit()
    print("✅ Updated onboarded flags")

# ── Step 5: Create missing tables ─────────────────────────────
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
CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(user_id, category, transaction_date DESC);
""")

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

cur.execute("""
CREATE TABLE IF NOT EXISTS rate_limits (
    user_id VARCHAR(50) PRIMARY KEY,
    message_count INTEGER DEFAULT 0,
    window_start TIMESTAMPTZ DEFAULT NOW()
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    action VARCHAR(100),
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    emoji VARCHAR(10),
    treatment VARCHAR(20) DEFAULT 'spend',
    display_order INTEGER
);
""")

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

conn.commit()
print("✅ All missing tables created")

# ── Step 6: Add conversations created_at if missing ──────────
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'conversations'
""")
conv_cols = [r[0] for r in cur.fetchall()]
if 'created_at' not in conv_cols and conv_cols:
    cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
    conn.commit()
    print("✅ Added created_at to conversations")

cur.close()
conn.close()
print("\n✅ Migration complete!")
