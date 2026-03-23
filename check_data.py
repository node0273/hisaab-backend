"""
Run this to see all recorded transactions and diagnose issues.
python check_data.py
"""
import psycopg2
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set $env:DATABASE_URL first!")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("=" * 80)
print("HISAAB — DATABASE DIAGNOSTIC REPORT")
print("=" * 80)

# Users
print("\n=== USERS ===")
cur.execute("SELECT user_id, name, consent_given, ai_consent_given, onboarded, last_active FROM users")
for r in cur.fetchall():
    print(f"ID: {r[0]} | Name: {r[1]} | Consent: {r[2]} | AI: {r[3]} | Onboarded: {r[4]} | Last active: {str(r[5])[:16]}")

# Gmail accounts
print("\n=== GMAIL ACCOUNTS ===")
cur.execute("SELECT user_id, email, is_active FROM gmail_accounts")
for r in cur.fetchall():
    print(f"User: {r[0]} | Gmail: {r[1]} | Active: {r[2]}")

# Transactions by bank
print("\n=== TRANSACTIONS BY BANK ===")
cur.execute("""
    SELECT bank, COUNT(*), SUM(amount), MIN(transaction_date), MAX(transaction_date)
    FROM transactions
    GROUP BY bank ORDER BY COUNT(*) DESC
""")
rows = cur.fetchall()
if not rows:
    print("NO TRANSACTIONS RECORDED!")
else:
    for r in rows:
        print(f"{str(r[0]):15} | {r[1]:3} txns | ₹{float(r[2]):10,.2f} | {r[3]} to {r[4]}")

# Transactions by category
print("\n=== TRANSACTIONS BY CATEGORY ===")
cur.execute("""
    SELECT category, treatment, COUNT(*), SUM(amount)
    FROM transactions
    GROUP BY category, treatment ORDER BY SUM(amount) DESC
""")
for r in cur.fetchall():
    print(f"{str(r[0]):25} | {str(r[1]):10} | {r[2]:3} txns | ₹{float(r[3]):10,.2f}")

# All transactions (last 30)
print("\n=== RECENT TRANSACTIONS (last 30) ===")
cur.execute("""
    SELECT transaction_date, bank, mode, amount, merchant_canonical, category, treatment
    FROM transactions
    ORDER BY transaction_date DESC
    LIMIT 30
""")
for r in cur.fetchall():
    print(f"{r[0]} | {str(r[1]):8} | {str(r[2]):12} | ₹{float(r[3]):8,.2f} | {str(r[4]):20} | {str(r[5]):20} | {r[6]}")

# Learned senders
print("\n=== LEARNED SENDERS ===")
try:
    cur.execute("SELECT sender_email, bank_name, discovered_at FROM learned_senders")
    rows = cur.fetchall()
    if not rows:
        print("None yet")
    for r in rows:
        print(f"{r[0]} → {r[1]} (discovered: {str(r[2])[:10]})")
except:
    print("learned_senders table not created yet")

# Gmail sync log
print("\n=== GMAIL SYNC LOG ===")
cur.execute("SELECT user_id, gmail_account, last_synced_at, emails_processed, transactions_found FROM gmail_sync_log")
for r in cur.fetchall():
    print(f"User: {r[0]} | Gmail: {r[1]} | Last sync: {str(r[2])[:16]} | Emails: {r[3]} | Txns: {r[4]}")

cur.close()
conn.close()
print("\n" + "=" * 80)
