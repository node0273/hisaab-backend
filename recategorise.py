"""
Re-run merchant resolver on all existing "Other" category transactions.
Run once after deploying new merchant_resolver.py:
python recategorise.py
"""
import os, psycopg2, psycopg2.extras
from security import decrypt, encrypt

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL first!")

# Import merchant resolver
import sys
sys.path.insert(0, os.path.dirname(__file__))
from merchant_resolver import resolve_merchant

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Get all transactions in "Other" category
cur.execute("""
    SELECT id, merchant_raw_enc, merchant_canonical, vpa_enc, amount
    FROM transactions
    WHERE category = 'Other' OR merchant_canonical LIKE 'VPA %'
""")
rows = cur.fetchall()
print(f"Found {len(rows)} transactions to re-categorise...")

update_cur = conn.cursor()
updated = 0

for row in rows:
    try:
        merchant_raw = decrypt(row['merchant_raw_enc']) if row['merchant_raw_enc'] else ''
        vpa = decrypt(row['vpa_enc']) if row['vpa_enc'] else ''
        amount = row['amount'] or 0

        # Clean up "VPA xxx merchant_name" format
        if merchant_raw.startswith('VPA '):
            parts = merchant_raw.split(' ', 2)
            if len(parts) >= 2:
                vpa = vpa or parts[1]
            if len(parts) >= 3:
                merchant_raw = parts[2]

        canonical, category, treatment, person_name, ai_cls = resolve_merchant(
            merchant_raw, vpa, amount
        )

        if category != 'Other' or canonical != row['merchant_canonical']:
            update_cur.execute("""
                UPDATE transactions 
                SET merchant_canonical = %s, category = %s, treatment = %s, person_name = %s
                WHERE id = %s
            """, (canonical, category, treatment, person_name, row['id']))
            updated += 1
    except Exception as e:
        print(f"Error on row {row['id']}: {e}")
        continue

conn.commit()
print(f"✅ Updated {updated} out of {len(rows)} transactions!")
cur.close()
update_cur.close()
conn.close()
