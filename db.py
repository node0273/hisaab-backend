"""
Database layer — PostgreSQL on Railway
All tokens encrypted with AES-256 before storage
"""
import os
import base64
import psycopg2
import psycopg2.extras
from cryptography.fernet import Fernet

DATABASE_URL = os.environ.get("DATABASE_URL")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "hisaab_secure_key_32chars_india!!")

def get_cipher():
    key = ENCRYPTION_KEY.encode() if isinstance(ENCRYPTION_KEY, str) else ENCRYPTION_KEY
    key_bytes = base64.urlsafe_b64encode(key[:32].ljust(32, b'0'))
    return Fernet(key_bytes)

def encrypt(text: str) -> str:
    if not text:
        return ""
    try:
        return get_cipher().encrypt(text.encode()).decode()
    except Exception:
        return text

def decrypt(text: str) -> str:
    if not text:
        return ""
    try:
        return get_cipher().decrypt(text.encode()).decode()
    except Exception:
        return text

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_user(whatsapp_number: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE whatsapp_number = %s", (whatsapp_number,))
            row = cur.fetchone()
            if row:
                row = dict(row)
                row["access_token"] = decrypt(row.get("access_token_enc", ""))
                row["refresh_token"] = decrypt(row.get("refresh_token_enc", ""))
            return row

def create_user(whatsapp_number: str) -> dict:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO users (whatsapp_number)
                VALUES (%s)
                ON CONFLICT (whatsapp_number) DO UPDATE SET updated_at = NOW()
                RETURNING *
            """, (whatsapp_number,))
            conn.commit()
            return dict(cur.fetchone())

def save_user_tokens(whatsapp_number: str, email: str, name: str,
                     access_token: str, refresh_token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Upsert — create user if not exists, then update tokens
            cur.execute("""
                INSERT INTO users (whatsapp_number, email, name, access_token_enc, refresh_token_enc, onboarded, consent_given, updated_at)
                VALUES (%s, %s, %s, %s, %s, TRUE, TRUE, NOW())
                ON CONFLICT (whatsapp_number) DO UPDATE SET
                    email = EXCLUDED.email,
                    name = EXCLUDED.name,
                    access_token_enc = EXCLUDED.access_token_enc,
                    refresh_token_enc = EXCLUDED.refresh_token_enc,
                    onboarded = TRUE,
                    updated_at = NOW()
            """, (whatsapp_number, email, name,
                  encrypt(access_token), encrypt(refresh_token)))
            conn.commit()

def save_consent(whatsapp_number: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET consent_given = TRUE, consent_timestamp = NOW()
                WHERE whatsapp_number = %s
            """, (whatsapp_number,))
            conn.commit()

def delete_user(whatsapp_number: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE whatsapp_number = %s", (whatsapp_number,))
            cur.execute("DELETE FROM conversations WHERE whatsapp_number = %s", (whatsapp_number,))
            conn.commit()

def save_message(whatsapp_number: str, role: str, message: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conversations (whatsapp_number, role, message)
                VALUES (%s, %s, %s)
            """, (whatsapp_number, role, message))
            conn.commit()

def get_recent_messages(whatsapp_number: str, limit: int = 10) -> list:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT role, message FROM conversations
                WHERE whatsapp_number = %s
                ORDER BY created_at DESC LIMIT %s
            """, (whatsapp_number, limit))
            rows = cur.fetchall()
            return [{"role": r["role"], "content": r["message"]} for r in reversed(rows)]

def queue_unknown_email(whatsapp_number: str, subject: str, sender: str, body: str, date: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO unknown_emails (whatsapp_number, email_subject, email_sender, email_body, email_date)
                VALUES (%s, %s, %s, %s, %s)
            """, (whatsapp_number, subject, sender, body[:5000], date))
            conn.commit()
