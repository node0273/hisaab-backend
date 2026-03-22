"""
Hisaab Database layer
"""
import os
import psycopg2
import psycopg2.extras
from security import encrypt, decrypt

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# ── Users ─────────────────────────────────────────────────────

def get_user(user_id: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None

def create_user(user_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id) VALUES (%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (user_id,))
            conn.commit()

def save_consent(user_id: str, ai_consent: bool = False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET consent_given = TRUE, ai_consent_given = %s
                WHERE user_id = %s
            """, (ai_consent, user_id))
            conn.commit()

def update_last_active(user_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_active = NOW() WHERE user_id = %s", (user_id,))
            conn.commit()

def delete_user(user_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM gmail_accounts WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM transactions WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM conversations WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM gmail_sync_log WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM rate_limits WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            conn.commit()

# ── Gmail accounts (multiple per user) ───────────────────────

def get_gmail_accounts(user_id: str) -> list:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM gmail_accounts WHERE user_id = %s AND is_active = TRUE
            """, (user_id,))
            rows = cur.fetchall()
            result = []
            for row in rows:
                r = dict(row)
                r["access_token"] = decrypt(r.get("access_token_enc", ""))
                r["refresh_token"] = decrypt(r.get("refresh_token_enc", ""))
                result.append(r)
            return result

def save_gmail_account(user_id: str, email: str, name: str, access_token: str, refresh_token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Check limit of 3 accounts
            cur.execute("SELECT COUNT(*) FROM gmail_accounts WHERE user_id = %s AND is_active = TRUE", (user_id,))
            count = cur.fetchone()[0]
            if count >= 3:
                return False, "You can connect maximum 3 Gmail accounts."

            cur.execute("""
                INSERT INTO gmail_accounts (user_id, email, name, access_token_enc, refresh_token_enc)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, email) DO UPDATE SET
                    access_token_enc = EXCLUDED.access_token_enc,
                    refresh_token_enc = EXCLUDED.refresh_token_enc,
                    is_active = TRUE
            """, (user_id, email, name, encrypt(access_token), encrypt(refresh_token)))

            # Mark user as onboarded
            cur.execute("UPDATE users SET onboarded = TRUE, name = %s WHERE user_id = %s", (name, user_id))
            conn.commit()
            return True, "ok"

def update_access_token(user_id: str, email: str, new_token: str):
    """Update access token after rotation."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE gmail_accounts SET access_token_enc = %s
                WHERE user_id = %s AND email = %s
            """, (encrypt(new_token), user_id, email))
            conn.commit()

def remove_gmail_account(user_id: str, email: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE gmail_accounts SET is_active = FALSE
                WHERE user_id = %s AND email = %s
            """, (user_id, email))
            # If no more accounts, mark as not onboarded
            cur.execute("SELECT COUNT(*) FROM gmail_accounts WHERE user_id = %s AND is_active = TRUE", (user_id,))
            if cur.fetchone()[0] == 0:
                cur.execute("UPDATE users SET onboarded = FALSE WHERE user_id = %s", (user_id,))
            conn.commit()

# ── Transactions ──────────────────────────────────────────────

def save_transactions(user_id: str, transactions: list):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for t in transactions:
                cur.execute("""
                    INSERT INTO transactions
                        (user_id, gmail_account, bank, mode, amount,
                         merchant_raw_enc, merchant_canonical, category, treatment,
                         vpa_enc, person_name, transaction_date, email_message_id, ai_classified)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, email_message_id) DO NOTHING
                """, (
                    user_id,
                    t.get("gmail_account", ""),
                    t.get("bank", ""),
                    t.get("mode", ""),
                    t.get("amount", 0),
                    encrypt(t.get("merchant_raw", "")),
                    t.get("merchant_canonical", ""),
                    t.get("category", "Other"),
                    t.get("treatment", "spend"),
                    encrypt(t.get("vpa", "")),
                    t.get("person_name", ""),
                    t.get("date"),
                    t.get("msg_id", ""),
                    t.get("ai_classified", False),
                ))
            conn.commit()

def get_transactions_from_db(user_id: str, days: int = 30, 
                              start_date=None, end_date=None,
                              treatment_filter: list = None) -> list:
    """Load transactions from DB. Excludes settlements/excluded by default."""
    from datetime import datetime, timedelta
    
    if start_date and end_date:
        since = start_date
        until = end_date
    else:
        since = (datetime.now() - timedelta(days=days)).date()
        until = datetime.now().date()

    # Default: only show spend, investment, refund — not settlement/excluded
    if treatment_filter is None:
        treatment_filter = ['spend', 'investment', 'refund']

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT bank, mode, amount, merchant_canonical,
                       category, treatment, person_name, transaction_date
                FROM transactions
                WHERE user_id = %s
                  AND transaction_date BETWEEN %s AND %s
                  AND treatment = ANY(%s)
                ORDER BY transaction_date DESC
            """, (user_id, since, until, treatment_filter))
            rows = cur.fetchall()
            return [{
                "bank": r[0], "mode": r[1], "amount": r[2],
                "merchant": r[3], "category": r[4],
                "treatment": r[5], "person_name": r[6],
                "date": str(r[7]),
            } for r in rows]

# ── Gmail sync log ────────────────────────────────────────────

def get_last_sync(user_id: str, gmail_account: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT last_synced_at FROM gmail_sync_log
                WHERE user_id = %s AND gmail_account = %s
            """, (user_id, gmail_account))
            row = cur.fetchone()
            return row[0] if row else None

def update_sync_log(user_id: str, gmail_account: str, emails: int, transactions: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gmail_sync_log (user_id, gmail_account, last_synced_at, emails_processed, transactions_found)
                VALUES (%s, %s, NOW(), %s, %s)
                ON CONFLICT (user_id, gmail_account) DO UPDATE SET
                    last_synced_at = NOW(),
                    emails_processed = gmail_sync_log.emails_processed + %s,
                    transactions_found = gmail_sync_log.transactions_found + %s
            """, (user_id, gmail_account, emails, transactions, emails, transactions))
            conn.commit()

def get_sync_stats(user_id: str) -> dict:
    """For 'my data' command."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT gmail_account, last_synced_at, emails_processed, transactions_found
                FROM gmail_sync_log WHERE user_id = %s
            """, (user_id,))
            syncs = cur.fetchall()

            cur.execute("SELECT COUNT(*) FROM transactions WHERE user_id = %s", (user_id,))
            total_txns = cur.fetchone()[0]

            cur.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions WHERE user_id = %s", (user_id,))
            date_range = cur.fetchone()

            return {
                "syncs": [{"email": r[0], "last_synced": str(r[1]), "emails": r[2], "transactions": r[3]} for r in syncs],
                "total_transactions": total_txns,
                "date_from": str(date_range[0]) if date_range[0] else None,
                "date_to": str(date_range[1]) if date_range[1] else None,
            }

# ── Conversations ─────────────────────────────────────────────

def save_message(user_id: str, role: str, content: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conversations (user_id, role, content)
                VALUES (%s, %s, %s)
            """, (user_id, role, content))
            conn.commit()

def get_recent_messages(user_id: str, limit: int = 8) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role, content FROM conversations
                WHERE user_id = %s
                ORDER BY created_at DESC LIMIT %s
            """, (user_id, limit))
            rows = cur.fetchall()
            return [{"role": r[0], "content": r[1]} for r in reversed(rows)]

# ── Rate limiting ─────────────────────────────────────────────

def check_rate_limit(user_id: str, max_per_minute: int = 10) -> bool:
    """Returns True if allowed, False if rate limited."""
    from datetime import datetime, timezone, timedelta
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT message_count, window_start FROM rate_limits WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            now = datetime.now(timezone.utc)

            if not row:
                cur.execute("INSERT INTO rate_limits (user_id, message_count, window_start) VALUES (%s, 1, %s)", (user_id, now))
                conn.commit()
                return True

            count, window_start = row
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=timezone.utc)

            if (now - window_start) > timedelta(minutes=1):
                cur.execute("UPDATE rate_limits SET message_count = 1, window_start = %s WHERE user_id = %s", (now, user_id))
                conn.commit()
                return True

            if count >= max_per_minute:
                return False

            cur.execute("UPDATE rate_limits SET message_count = message_count + 1 WHERE user_id = %s", (user_id,))
            conn.commit()
            return True

# ── Merchants ─────────────────────────────────────────────────

def get_pending_merchants(limit: int = 20) -> list:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM merchants WHERE status = 'pending'
                ORDER BY pending_since ASC LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

def approve_merchant(canonical_name: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchants SET status = 'approved', updated_at = NOW()
                WHERE canonical_name = %s
            """, (canonical_name,))
            conn.commit()

def reject_and_correct_merchant(canonical_name: str, correct_category: str, correct_treatment: str = 'spend'):
    """Correct merchant and update all past transactions retroactively."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchants SET
                    category = %s, treatment = %s,
                    status = 'approved', updated_at = NOW()
                WHERE canonical_name = %s
            """, (correct_category, correct_treatment, canonical_name))
            # Retroactive update of all transactions
            cur.execute("""
                UPDATE transactions SET category = %s, treatment = %s
                WHERE merchant_canonical = %s
            """, (correct_category, correct_treatment, canonical_name))
            conn.commit()

def auto_approve_old_pending():
    """Auto-approve merchants pending for more than 5 days."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchants SET status = 'approved', updated_at = NOW()
                WHERE status = 'pending'
                AND pending_since < NOW() - INTERVAL '5 days'
            """)
            conn.commit()

# ── Admin stats ───────────────────────────────────────────────

def get_admin_stats() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users WHERE onboarded = TRUE")
            active_users = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM transactions WHERE created_at > NOW() - INTERVAL '24 hours'")
            txns_today = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM merchants WHERE status = 'pending'")
            pending_merchants = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM gmail_sync_log WHERE last_synced_at > NOW() - INTERVAL '24 hours'")
            syncs_today = cur.fetchone()[0]

            return {
                "active_users": active_users,
                "transactions_today": txns_today,
                "pending_merchants": pending_merchants,
                "syncs_today": syncs_today,
            }
