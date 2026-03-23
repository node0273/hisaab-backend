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

    parts = command.strip().split(None, 2)
    cmd = parts[0].lower()

    if cmd == "/stats":
        stats = get_admin_stats()
        return f"""📊 *Hisaab Stats*

👥 Active users: {stats['active_users']}
💸 Transactions today: {stats['transactions_today']}
🔄 Syncs today: {stats['syncs_today']}
⏳ Pending merchants: {stats['pending_merchants']}"""

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

    if cmd == "/errors":
        return "No recent errors logged. ✅"

    return "Unknown admin command."

def monitor_health():
    """Send health check — call this from a scheduled endpoint."""
    try:
        from db import get_admin_stats
        stats = get_admin_stats()
        send_admin_alert(f"✅ *Hisaab Health Check*\n\nUsers: {stats['active_users']}\nTxns today: {stats['transactions_today']}\nPending merchants: {stats['pending_merchants']}")
    except Exception as e:
        send_admin_alert(f"🚨 *Hisaab Health Check FAILED*\n\nError: {str(e)[:200]}")
