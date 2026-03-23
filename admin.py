"""
Admin commands — only for ADMIN_TELEGRAM_ID
Handles merchant approvals, stats, monitoring alerts
"""
import os
import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID", "8130140084")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

def is_admin(user_id: str) -> bool:
    return str(user_id) == str(ADMIN_TELEGRAM_ID)

def send_admin_alert(message: str):
    """Send alert to admin Telegram."""
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": ADMIN_TELEGRAM_ID,
            "text": message,
            "parse_mode": "Markdown"
        }, timeout=3)
    except:
        pass

async def handle_admin_command(user_id: str, command: str) -> str:
    """Handle admin-only commands."""
    from db import get_admin_stats, get_pending_merchants, approve_merchant, reject_and_correct_merchant, auto_approve_old_pending
    from rules_engine import add_vpa_rule, add_nach_rule, add_bank_sender, list_rules, get_rule_stats, force_refresh

    parts = command.strip().split(None, 3)
    cmd = parts[0].lower()

    if cmd == "/stats":
        stats = get_admin_stats()
        rule_stats = get_rule_stats()
        return f"""📊 *Hisaab Stats*

👥 Active users: {stats['active_users']}
💸 Transactions today: {stats['transactions_today']}
🔄 Syncs today: {stats['syncs_today']}
⏳ Pending merchants: {stats['pending_merchants']}

📋 *Rules in DB:*
• Bank senders: {rule_stats['bank_senders']}
• VPA rules: {rule_stats['vpa_rules']}
• NACH rules: {rule_stats['nach_rules']}"""

    if cmd == "/pending":
        auto_approve_old_pending()
        merchants = get_pending_merchants(10)
        if not merchants:
            return "✅ No pending merchants."
        lines = "\n".join([f"• `{m['canonical_name']}` → {m['category']}" for m in merchants])
        return f"⏳ *Pending merchants:*\n{lines}\n\nUse `/approve name` or `/reject name category`"

    if cmd == "/approve" and len(parts) >= 2:
        name = parts[1]
        approve_merchant(name)
        return f"✅ Approved: *{name}*"

    if cmd == "/reject" and len(parts) >= 3:
        name = parts[1]
        category = parts[2]
        reject_and_correct_merchant(name, category)
        return f"✅ Corrected *{name}* → {category}\nAll past transactions updated."

    # Rule management
    if cmd == "/add_vpa" and len(parts) >= 4:
        # /add_vpa keyword merchant category [treatment]
        keyword = parts[1].lower()
        merchant = parts[2]
        rest = parts[3].split()
        category = rest[0] if rest else "Other"
        treatment = rest[1] if len(rest) > 1 else "spend"
        if add_vpa_rule(keyword, merchant, category, treatment):
            return f"✅ VPA rule added: `{keyword}` → {merchant} ({category})"
        return "❌ Failed to add VPA rule"

    if cmd == "/add_nach" and len(parts) >= 4:
        keyword = parts[1].lower()
        merchant = parts[2]
        rest = parts[3].split()
        category = rest[0] if rest else "Other"
        treatment = rest[1] if len(rest) > 1 else "spend"
        if add_nach_rule(keyword, merchant, category, treatment):
            return f"✅ NACH rule added: `{keyword}` → {merchant} ({category})"
        return "❌ Failed to add NACH rule"

    if cmd == "/add_sender" and len(parts) >= 3:
        email = parts[1].lower()
        bank = parts[2]
        if add_bank_sender(email, bank):
            return f"✅ Sender added: `{email}` → {bank}"
        return "❌ Failed to add sender"

    if cmd == "/rules" and len(parts) >= 2:
        rule_type = parts[1].lower()
        rules = list_rules(rule_type)
        if not rules:
            return f"No {rule_type} rules found."
        lines = "\n".join([f"• `{r[0]}` → {r[1]} ({r[2]})" for r in rules[:20]])
        return f"📋 *{rule_type.upper()} rules (first 20):*\n{lines}"

    if cmd == "/refresh_rules":
        force_refresh()
        stats = get_rule_stats()
        return f"✅ Rules refreshed from DB:\n• Senders: {stats['bank_senders']}\n• VPA: {stats['vpa_rules']}\n• NACH: {stats['nach_rules']}"

    if cmd == "/errors":
        return "No recent errors logged. ✅"

    return """Unknown admin command.

Available commands:
/stats — system stats
/pending — pending merchant approvals
/approve name — approve merchant
/reject name category — correct merchant
/add_vpa keyword merchant category [treatment]
/add_nach keyword merchant category [treatment]
/add_sender email@bank.com BankName
/rules vpa|nach|senders
/refresh_rules — reload from DB"""

def monitor_health():
    """Send health check — call this from a scheduled endpoint."""
    try:
        from db import get_admin_stats
        stats = get_admin_stats()
        send_admin_alert(f"✅ *Hisaab Health Check*\n\nUsers: {stats['active_users']}\nTxns today: {stats['transactions_today']}\nPending merchants: {stats['pending_merchants']}")
    except Exception as e:
        send_admin_alert(f"🚨 *Hisaab Health Check FAILED*\n\nError: {str(e)[:200]}")
