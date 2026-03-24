"""Admin commands — only for ADMIN_TELEGRAM_ID"""
import os, requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID", "8130140084")

def is_admin(user_id: str) -> bool:
    return str(user_id) == str(ADMIN_TELEGRAM_ID)

def send_admin_alert(message: str):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_TELEGRAM_ID, "text": message, "parse_mode": "Markdown"},
            timeout=3
        )
    except: pass

async def handle_admin_command(user_id: str, command: str) -> str:
    from db import (get_admin_stats, get_pending_rules, auto_approve_pending,
                    approve_merchant_rule, reject_merchant_rule,
                    approve_parsing_rule, save_bank_sender, save_negative_rule,
                    save_merchant_rule, get_merchant_rules)

    parts = command.strip().split(None, 3)
    cmd = parts[0].lower()

    auto_approve_pending()

    if cmd == "/stats":
        s = get_admin_stats()
        return (f"📊 *Hisaab Stats*\n\n"
                f"👥 Active users: {s['active_users']}\n"
                f"💸 Transactions today: {s['transactions_today']}\n"
                f"🏦 Bank senders: {s['bank_senders']}\n"
                f"🏪 Merchant rules: {s['merchant_rules']}\n"
                f"⏳ Pending merchants: {s['pending_merchants']}\n"
                f"⏳ Pending parsing rules: {s['pending_parsing']}")

    if cmd == "/pending":
        pending = get_pending_rules()
        reply = ""
        if pending["merchants"]:
            reply += "🏪 *Pending merchants:*\n"
            for m in pending["merchants"][:10]:
                reply += f"#{m['id']} `{m['keyword']}` → {m['merchant_canonical']} ({m['category']})\n"
            reply += "\n/approve_merchant {id} or /reject_merchant {id} {category} {treatment}\n\n"
        if pending["parsing_rules"]:
            reply += "📧 *Pending parsing rules:*\n"
            for p in pending["parsing_rules"][:5]:
                reply += f"#{p['id']} {p['bank']} {p['mode']}: `{p['sample_subject'][:60]}`\n"
            reply += "\n/approve_rule {id} or /reject_rule {id}"
        return reply or "✅ Nothing pending!"

    if cmd == "/approve_merchant" and len(parts) >= 2:
        try:
            row = approve_merchant_rule(int(parts[1]))
            return f"✅ Approved: *{row[1]}* → {row[2]}" if row else "Not found"
        except Exception as e:
            return f"Error: {e}"

    if cmd == "/reject_merchant" and len(parts) >= 3:
        try:
            rule_id = int(parts[1])
            rest = parts[2].rsplit(None, 1) if len(parts) == 3 else [parts[2], parts[3] if len(parts) > 3 else "spend"]
            category = rest[0]
            treatment = rest[1] if len(rest) > 1 else "spend"
            # Need canonical name too
            from db import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT merchant_canonical FROM merchant_rules WHERE id = %s", (rule_id,))
                    row = cur.fetchone()
                    canonical = row[0] if row else "Unknown"
            reject_merchant_rule(rule_id, canonical, category, treatment)
            return f"✅ Corrected #{rule_id} → {category} ({treatment})\nAll past transactions updated."
        except Exception as e:
            return f"Error: {e}"

    if cmd == "/approve_rule" and len(parts) >= 2:
        try:
            approve_parsing_rule(int(parts[1]))
            return f"✅ Parsing rule #{parts[1]} approved"
        except Exception as e:
            return f"Error: {e}"

    if cmd == "/reject_rule" and len(parts) >= 2:
        try:
            from db import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM parsing_rules WHERE id = %s", (int(parts[1]),))
                    conn.commit()
            return f"✅ Parsing rule #{parts[1]} rejected and deleted"
        except Exception as e:
            return f"Error: {e}"

    if cmd == "/add_sender" and len(parts) >= 3:
        save_bank_sender(parts[1].lower(), parts[2], "admin")
        return f"✅ Sender added: `{parts[1]}` → {parts[2]}"

    if cmd == "/add_merchant" and len(parts) >= 4:
        rest = parts[3].split()
        category = " ".join(rest[:-1]) if len(rest) > 1 else rest[0]
        treatment = rest[-1] if len(rest) > 1 else "spend"
        save_merchant_rule(parts[1].lower(), parts[2], category, treatment, "admin", auto_approve=True)
        return f"✅ Merchant rule added: `{parts[1]}` → {parts[2]} ({category})"

    if cmd == "/negative" and len(parts) >= 2:
        save_negative_rule(parts[1].lower(), "manual")
        return f"✅ Added to negative rules: `{parts[1]}`"

    if cmd == "/rules":
        rules = get_merchant_rules()
        lines = "\n".join([f"`{k}` → {v[0]} ({v[1]})" for k, v in list(rules.items())[:20]])
        return f"📋 *Merchant rules (first 20 of {len(rules)}):*\n{lines}"

    return """*Admin commands:*
/stats — system overview
/pending — pending approvals
/approve_merchant {id}
/reject_merchant {id} {category} {treatment}
/approve_rule {id}
/reject_rule {id}
/add_sender {email} {bank}
/add_merchant {keyword} {canonical} {category} {treatment}
/negative {email} — mark as spam
/rules — show merchant rules"""
