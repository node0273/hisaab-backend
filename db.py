"""
Hisaab — All database operations
"""
import os, psycopg2, psycopg2.extras
from security import encrypt, decrypt

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# ── Users ─────────────────────────────────────────────────────

def get_user(user_id: str) -> dict:
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
            for table in ["gmail_accounts", "transactions", "conversations",
                          "gmail_sync_log", "rate_limits", "users"]:
                cur.execute(f"DELETE FROM {table} WHERE user_id = %s", (user_id,))
            conn.commit()

# ── Gmail accounts ────────────────────────────────────────────

def get_gmail_accounts(user_id: str) -> list:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM gmail_accounts 
                WHERE user_id = %s AND is_active = TRUE
            """, (user_id,))
            rows = cur.fetchall()
            result = []
            for row in rows:
                r = dict(row)
                r["access_token"] = decrypt(r.get("access_token_enc", ""))
                r["refresh_token"] = decrypt(r.get("refresh_token_enc", ""))
                result.append(r)
            return result

def save_gmail_account(user_id: str, email: str, name: str,
                       access_token: str, refresh_token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gmail_accounts WHERE user_id = %s AND is_active = TRUE", (user_id,))
            if cur.fetchone()[0] >= 3:
                return False, "Maximum 3 Gmail accounts allowed"
            cur.execute("""
                INSERT INTO gmail_accounts (user_id, email, name, access_token_enc, refresh_token_enc)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, email) DO UPDATE SET
                    access_token_enc = EXCLUDED.access_token_enc,
                    refresh_token_enc = EXCLUDED.refresh_token_enc,
                    is_active = TRUE
            """, (user_id, email, name, encrypt(access_token), encrypt(refresh_token)))
            cur.execute("UPDATE users SET onboarded = TRUE, name = %s WHERE user_id = %s", (name, user_id))
            conn.commit()
            return True, "ok"

def update_access_token(user_id: str, email: str, new_token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE gmail_accounts SET access_token_enc = %s
                WHERE user_id = %s AND email = %s
            """, (encrypt(new_token), user_id, email))
            conn.commit()

# ── Transactions ──────────────────────────────────────────────

def save_transactions(user_id: str, transactions: list):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for t in transactions:
                cur.execute("""
                    INSERT INTO transactions
                        (user_id, bank, mode, amount, merchant_canonical,
                         category, treatment, transaction_date,
                         vpa, person_name, upi_app,
                         email_message_id, gmail_account)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, email_message_id) DO NOTHING
                """, (
                    user_id,
                    t.get("bank"),
                    t.get("mode"),
                    t.get("amount"),
                    t.get("merchant_canonical"),
                    t.get("category"),
                    t.get("treatment", "spend"),
                    t.get("date"),
                    t.get("vpa"),
                    t.get("person_name"),
                    t.get("upi_app"),
                    t.get("msg_id"),
                    t.get("gmail_account"),
                ))
            conn.commit()

def get_transactions(user_id: str, days: int = 30,
                     start_date=None, end_date=None,
                     treatments: list = None) -> list:
    from datetime import datetime, timedelta
    if start_date and end_date:
        since, until = start_date, end_date
    else:
        since = (datetime.now() - timedelta(days=days)).date()
        until = datetime.now().date()
    if treatments is None:
        treatments = ["spend", "investment", "refund"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT bank, mode, amount, merchant_canonical,
                       category, treatment, transaction_date,
                       vpa, person_name, upi_app
                FROM transactions
                WHERE user_id = %s
                  AND transaction_date BETWEEN %s AND %s
                  AND treatment = ANY(%s)
                ORDER BY transaction_date DESC
            """, (user_id, since, until, treatments))
            cols = ["bank","mode","amount","merchant","category",
                    "treatment","date","vpa","person_name","upi_app"]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

# ── Sync log ──────────────────────────────────────────────────

def get_last_sync(user_id: str, gmail_account: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT last_synced_at FROM gmail_sync_log
                WHERE user_id = %s AND gmail_account = %s
            """, (user_id, gmail_account))
            row = cur.fetchone()
            return row[0] if row else None

def update_sync_log(user_id: str, gmail_account: str, emails: int, found: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gmail_sync_log 
                    (user_id, gmail_account, last_synced_at, emails_processed, transactions_found)
                VALUES (%s, %s, NOW(), %s, %s)
                ON CONFLICT (user_id, gmail_account) DO UPDATE SET
                    last_synced_at = NOW(),
                    emails_processed = gmail_sync_log.emails_processed + %s,
                    transactions_found = gmail_sync_log.transactions_found + %s
            """, (user_id, gmail_account, emails, found, emails, found))
            conn.commit()

def get_sync_stats(user_id: str) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT gmail_account, last_synced_at, emails_processed, transactions_found
                FROM gmail_sync_log WHERE user_id = %s
            """, (user_id,))
            syncs = [{"email": r[0], "last_synced": str(r[1])[:16],
                      "emails": r[2], "transactions": r[3]}
                     for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) FROM transactions WHERE user_id = %s", (user_id,))
            total = cur.fetchone()[0]
            cur.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions WHERE user_id = %s", (user_id,))
            dr = cur.fetchone()
            return {"syncs": syncs, "total_transactions": total,
                    "date_from": str(dr[0]) if dr[0] else None,
                    "date_to": str(dr[1]) if dr[1] else None}

# ── Rules ─────────────────────────────────────────────────────

def get_bank_senders() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sender_email, bank_name FROM bank_senders WHERE is_active = TRUE")
            return {r[0]: r[1] for r in cur.fetchall()}

def get_negative_senders() -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sender_email FROM negative_rules")
            return {r[0] for r in cur.fetchall()}

def get_merchant_rules() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT keyword, merchant_canonical, category, treatment
                FROM merchant_rules WHERE status = 'approved'
            """)
            return {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

def get_parsing_rules(bank: str, mode: str) -> list:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM parsing_rules
                WHERE bank = %s AND mode = %s AND status = 'approved'
            """, (bank, mode))
            return [dict(r) for r in cur.fetchall()]

def save_bank_sender(sender: str, bank: str, source: str = 'ai'):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bank_senders (sender_email, bank_name, source)
                VALUES (%s, %s, %s) ON CONFLICT (sender_email) DO NOTHING
            """, (sender, bank, source))
            conn.commit()

def save_negative_rule(sender: str, reason: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO negative_rules (sender_email, reason)
                VALUES (%s, %s) ON CONFLICT (sender_email) DO NOTHING
            """, (sender, reason))
            conn.commit()

def save_merchant_rule(keyword: str, canonical: str, category: str,
                       treatment: str, source: str = 'ai', auto_approve: bool = False):
    status = 'approved' if auto_approve else 'pending'
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO merchant_rules
                    (keyword, merchant_canonical, category, treatment, status, pending_since, source)
                VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (keyword) DO NOTHING
                RETURNING id
            """, (keyword.lower()[:255], canonical, category, treatment, status, source))
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None

def save_parsing_rule(bank: str, mode: str, amount_pattern: str,
                      merchant_pattern: str, vpa_pattern: str, sample_subject: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO parsing_rules
                    (bank, mode, amount_pattern, merchant_pattern, 
                     vpa_pattern, sample_subject, status, pending_since, source)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', NOW(), 'ai')
                RETURNING id
            """, (bank, mode, amount_pattern, merchant_pattern, vpa_pattern, sample_subject))
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None

def approve_merchant_rule(rule_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchant_rules SET status = 'approved' WHERE id = %s
                RETURNING keyword, merchant_canonical, category, treatment
            """, (rule_id,))
            row = cur.fetchone()
            conn.commit()
            return row

def reject_merchant_rule(rule_id: int, correct_canonical: str,
                         correct_category: str, correct_treatment: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchant_rules SET
                    merchant_canonical = %s, category = %s,
                    treatment = %s, status = 'approved'
                WHERE id = %s
                RETURNING keyword
            """, (correct_canonical, correct_category, correct_treatment, rule_id))
            row = cur.fetchone()
            if row:
                # Retroactively update all transactions
                cur.execute("""
                    UPDATE transactions SET
                        merchant_canonical = %s, category = %s, treatment = %s
                    WHERE merchant_canonical IN (
                        SELECT merchant_canonical FROM merchant_rules WHERE id = %s
                    )
                """, (correct_canonical, correct_category, correct_treatment, rule_id))
            conn.commit()

def approve_parsing_rule(rule_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE parsing_rules SET status = 'approved' WHERE id = %s", (rule_id,))
            conn.commit()

def auto_approve_pending():
    """Auto-approve rules pending more than 5 days."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE merchant_rules SET status = 'approved'
                WHERE status = 'pending'
                AND pending_since < NOW() - INTERVAL '5 days'
            """)
            cur.execute("""
                UPDATE parsing_rules SET status = 'approved'
                WHERE status = 'pending'
                AND pending_since < NOW() - INTERVAL '5 days'
            """)
            conn.commit()

def get_pending_rules() -> dict:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, keyword, merchant_canonical, category, treatment, source
                FROM merchant_rules WHERE status = 'pending'
                ORDER BY pending_since ASC LIMIT 20
            """)
            merchants = [dict(r) for r in cur.fetchall()]
            cur.execute("""
                SELECT id, bank, mode, sample_subject, source
                FROM parsing_rules WHERE status = 'pending'
                ORDER BY pending_since ASC LIMIT 20
            """)
            parsings = [dict(r) for r in cur.fetchall()]
            return {"merchants": merchants, "parsing_rules": parsings}

# ── Conversations ─────────────────────────────────────────────

def save_message(user_id: str, role: str, content: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conversations (user_id, role, content)
                VALUES (%s, %s, %s)
            """, (user_id, role, content))
            conn.commit()

def get_recent_messages(user_id: str, limit: int = 6) -> list:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role, content FROM conversations
                WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
            """, (user_id, limit))
            return [{"role": r[0], "content": r[1]}
                    for r in reversed(cur.fetchall())]

# ── Rate limiting ─────────────────────────────────────────────

def check_rate_limit(user_id: str, max_per_minute: int = 10) -> bool:
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

# ── Admin stats ───────────────────────────────────────────────

def get_admin_stats() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users WHERE onboarded = TRUE")
            users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM transactions WHERE created_at > NOW() - INTERVAL '24 hours'")
            txns_today = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM merchant_rules WHERE status = 'pending'")
            pending_m = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM parsing_rules WHERE status = 'pending'")
            pending_p = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM bank_senders")
            senders = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM merchant_rules WHERE status = 'approved'")
            merchants = cur.fetchone()[0]
            return {
                "active_users": users,
                "transactions_today": txns_today,
                "pending_merchants": pending_m,
                "pending_parsing": pending_p,
                "bank_senders": senders,
                "merchant_rules": merchants,
            }
