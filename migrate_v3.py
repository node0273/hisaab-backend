"""
Migrate existing DB to v3 schema.
Run: python migrate_v3.py
"""
import psycopg2, os

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("=== Migrating to v3 schema ===")

# Add missing columns to merchant_rules
cols_to_add = [
    ("merchant_rules", "status", "VARCHAR(20) DEFAULT 'approved'"),
    ("merchant_rules", "pending_since", "TIMESTAMPTZ"),
    ("merchant_rules", "source", "VARCHAR(20) DEFAULT 'seed'"),
    ("parsing_rules", "status", "VARCHAR(20) DEFAULT 'pending'"),
    ("parsing_rules", "pending_since", "TIMESTAMPTZ DEFAULT NOW()"),
    ("parsing_rules", "source", "VARCHAR(20) DEFAULT 'ai'"),
    ("parsing_rules", "amount_pattern", "VARCHAR(500)"),
    ("parsing_rules", "merchant_pattern", "VARCHAR(500)"),
    ("parsing_rules", "vpa_pattern", "VARCHAR(500)"),
    ("parsing_rules", "sample_subject", "TEXT"),
    ("transactions", "vpa", "VARCHAR(255)"),
    ("transactions", "person_name", "VARCHAR(255)"),
    ("transactions", "upi_app", "VARCHAR(100)"),
    ("transactions", "gmail_account", "VARCHAR(255)"),
    ("users", "ai_consent_given", "BOOLEAN DEFAULT FALSE"),
    ("users", "last_active", "TIMESTAMPTZ DEFAULT NOW()"),
]

for table, col, definition in cols_to_add:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {definition}")
        conn.commit()
        print(f"  ✅ {table}.{col}")
    except Exception as e:
        conn.rollback()
        print(f"  ⚠️  {table}.{col}: {str(e)[:60]}")

# Create missing tables
cur.execute("""
CREATE TABLE IF NOT EXISTS bank_senders (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) UNIQUE NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    source VARCHAR(20) DEFAULT 'seed',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS negative_rules (
    id SERIAL PRIMARY KEY,
    sender_email VARCHAR(255) UNIQUE NOT NULL,
    reason VARCHAR(255),
    source VARCHAR(20) DEFAULT 'ai',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS rate_limits (
    user_id VARCHAR(50) PRIMARY KEY,
    message_count INTEGER DEFAULT 0,
    window_start TIMESTAMPTZ DEFAULT NOW()
);
""")
conn.commit()
print("  ✅ Ensured bank_senders, negative_rules, rate_limits tables exist")

# Update existing merchant_rules to have status=approved
cur.execute("UPDATE merchant_rules SET status = 'approved' WHERE status IS NULL")
conn.commit()
print(f"  ✅ Set all existing merchant_rules to approved")

conn.commit()
cur.close()
conn.close()
print("\n✅ Migration complete! Now run: python setup_db.py")
