"""Hisaab Bot — Telegram conversation handler"""
import os
from db import (get_user, create_user, save_consent, update_last_active,
                delete_user, get_gmail_accounts, get_sync_stats,
                get_recent_messages, save_message, check_rate_limit,
                get_transactions, auto_approve_pending)
from gmail_reader import sync_all
from ai_brain import generate_reply
from time_periods import parse_time_period, get_time_period_buttons, CALLBACK_TO_PERIOD
from security import check_session_active
from admin import is_admin, handle_admin_command

BACKEND_URL = os.environ.get("BACKEND_URL", "")

CONSENT_TEXT = """👋 Welcome to *Hisaab* — your personal finance assistant!

Here's what I do:
• Read your bank alert emails (HDFC, HSBC, ICICI, SBI & more)
• Extract every transaction automatically
• Answer your spending questions in plain language

*Your privacy:*
🔒 Read-only Gmail access — I only read bank alert emails
🇮🇳 Your data stays encrypted and secure
🗑️ Type *DELETE MY DATA* anytime
📋 Type *my data* to see what I've stored

Type *I AGREE* to continue."""

AI_CONSENT_TEXT = """One more thing before we connect your Gmail.

*AI Notice:* I use Claude AI to answer your questions. Your transaction data (amounts, merchant names, dates) is processed by AI.

*What is NOT stored:* Raw email content is never saved.

Type *I AGREE TO AI* to continue, or *NO AI* to skip."""

async def handle_message(user_id: str, message: str) -> tuple:
    """Returns (reply_text, keyboard or None)"""
    msg = message.strip()
    msg_lower = msg.lower()

    if not check_rate_limit(user_id):
        return "⏳ Too many messages! Wait a moment.", None

    if is_admin(user_id) and msg.startswith("/"):
        return await handle_admin_command(user_id, msg), None

    if msg_lower in ["delete my data", "delete data"]:
        delete_user(user_id)
        return "✅ All your data has been permanently deleted. Send anything to start fresh.", None

    user = get_user(user_id)
    if not user:
        create_user(user_id)
        return CONSENT_TEXT, None

    update_last_active(user_id)
    auto_approve_pending()

    # Session timeout
    if user.get("onboarded") and not check_session_active(user.get("last_active")):
        return "👋 Welcome back! For security, please confirm: type *yes its me*", None
    if msg_lower == "yes its me":
        update_last_active(user_id)
        return "✅ Verified! What would you like to know?", None

    # Consent flow
    if not user.get("consent_given"):
        if msg_lower == "i agree":
            save_consent(user_id)
            return AI_CONSENT_TEXT, None
        return CONSENT_TEXT, None

    if not user.get("ai_consent_given"):
        if msg_lower == "i agree to ai":
            save_consent(user_id, ai_consent=True)
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            return (f"✅ Perfect! Now connect your Gmail.\n\n"
                    f"👉 [Tap here to connect Gmail]({auth_url})\n\n"
                    f"I'll only read emails from your bank. Nothing else.\n\n"
                    f"Come back here once done!"), None
        elif msg_lower == "no ai":
            save_consent(user_id, ai_consent=False)
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            return f"✅ Got it. [Connect Gmail here]({auth_url})", None
        return AI_CONSENT_TEXT, None

    if not user.get("onboarded"):
        auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
        return (f"You haven't connected your Gmail yet.\n\n"
                f"👉 [Tap here to connect Gmail]({auth_url})"), None

    gmail_accounts = get_gmail_accounts(user_id)

    # Commands
    if msg_lower in ["hi", "hello", "hey", "/start", "start"]:
        name = (user.get("name") or "there").split()[0]
        emails = "\n".join([f"  • {a['email']}" for a in gmail_accounts])
        return (f"Hi {name}! 👋 I'm Hisaab, your expense assistant.\n\n"
                f"*Connected Gmail:*\n{emails}\n\n"
                f"*What you can ask:*\n"
                f"• *summary* — spending overview\n"
                f"• *sync* — fetch latest transactions\n"
                f"• *my data* — what I've stored\n"
                f"• *add gmail* — connect another account\n"
                f"• How much on food this week?\n"
                f"• How much through HDFC?\n"
                f"• Any big transactions this month?\n"
                f"• Show my P2P transfers"), None

    if msg_lower in ["summary", "/summary"]:
        return "📅 *Select time period:*", get_time_period_buttons()

    if msg_lower in ["sync", "/sync", "refresh"]:
        return await do_sync(user_id, gmail_accounts), None

    if msg_lower == "my data":
        return await show_my_data(user_id, gmail_accounts), None

    if msg_lower == "add gmail":
        if len(gmail_accounts) >= 3:
            return "You've already connected 3 Gmail accounts (maximum).", None
        auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
        return (f"👉 [Connect another Gmail]({auth_url})\n\n"
                f"Connected: {len(gmail_accounts)}/3"), None

    # Natural language time period
    time_keywords = ["this week", "last week", "this month", "last month",
                     "3 months", "6 months", "this year"]
    if any(kw in msg_lower for kw in time_keywords):
        start, end, label = parse_time_period(msg_lower)
        return await generate_summary(user_id, start, end, label), None

    # AI chat — load from DB only
    save_message(user_id, "user", msg)
    history = get_recent_messages(user_id)
    transactions = get_transactions(user_id, days=90)
    reply = await generate_reply(transactions, history, msg)
    save_message(user_id, "assistant", reply)
    return reply, None

async def handle_callback(user_id: str, callback_data: str) -> tuple:
    if callback_data in CALLBACK_TO_PERIOD:
        period_text = CALLBACK_TO_PERIOD[callback_data]
        start, end, label = parse_time_period(period_text)
        return await generate_summary(user_id, start, end, label), None
    if callback_data == "period_custom":
        return "Type your date range like:\n*from 1 Jan to 31 Jan*", None
    return "Unknown option.", None

async def do_sync(user_id: str, gmail_accounts: list) -> str:
    try:
        result = sync_all(user_id, gmail_accounts)
        banks = ", ".join(result["banks_found"]) if result["banks_found"] else "none detected"
        new = result["new_transactions"]
        if result["banks_found"]:
            return (f"✅ *Sync complete!*\n\n"
                    f"New transactions: *{new}*\n"
                    f"Banks found: {banks}\n\n"
                    f"Type *summary* to see your spending!")
        else:
            return ("✅ Sync complete — 0 new transactions.\n\n"
                    "Possible reasons:\n"
                    "• Bank alerts might be in spam — check and move to inbox\n"
                    "• Bank alerts not enabled — enable in net banking\n"
                    "• Try *add gmail* to connect the Gmail that has bank alerts\n"
                    "• Which bank do you use?")
    except Exception as e:
        return f"Sync failed. Please try again. ({str(e)[:80]})"

async def show_my_data(user_id: str, gmail_accounts: list) -> str:
    stats = get_sync_stats(user_id)
    accounts = "\n".join([
        f"  • {s['email']}: {s['emails']} emails scanned, last synced {s['last_synced'][:10]}"
        for s in stats["syncs"]
    ]) or "  None"
    return (f"📋 *Your Data*\n\n"
            f"*Connected Gmail:*\n{accounts}\n\n"
            f"*Transactions stored:* {stats['total_transactions']}\n"
            f"*Date range:* {stats['date_from'] or 'N/A'} to {stats['date_to'] or 'N/A'}\n\n"
            f"*Retention:* 12 months. Chat history: 30 days.\n\n"
            f"Type *DELETE MY DATA* to remove everything.")

async def generate_summary(user_id: str, start_date, end_date, label: str) -> str:
    try:
        transactions = get_transactions(user_id, start_date=start_date, end_date=end_date)
        if not transactions:
            return (f"No transactions found for *{label}*.\n\n"
                    f"Type *sync* to fetch latest emails.")

        spend = [t for t in transactions if t.get("treatment") == "spend"]
        invest = [t for t in transactions if t.get("treatment") == "investment"]
        total_spend = sum(t["amount"] for t in spend)
        total_invest = sum(t["amount"] for t in invest)

        # Category breakdown
        cat = {}
        for t in spend:
            c = t.get("category", "Other")
            cat[c] = cat.get(c, 0) + t["amount"]
        cat_lines = "\n".join([f"  • {c}: ₹{round(a):,}"
                                for c, a in sorted(cat.items(), key=lambda x: x[1], reverse=True)[:6]])

        # Top merchants
        merch = {}
        for t in spend:
            m = t.get("merchant")
            if m and m not in ["P2P Transfer", "Daily Spend", "Unknown"]:
                merch[m] = merch.get(m, 0) + t["amount"]
        top = sorted(merch.items(), key=lambda x: x[1], reverse=True)[:5]
        merch_lines = "\n".join([f"  • {m}: ₹{round(a):,}" for m, a in top])

        # P2P
        p2p = [t for t in spend if t.get("category") in ["P2P Transfer", "Daily Spend"]]
        p2p_total = sum(t["amount"] for t in p2p)

        invest_line = f"\n📈 *Invested:* ₹{round(total_invest):,}" if total_invest > 0 else ""
        p2p_line = f"\n👤 *P2P Transfers:* ₹{round(p2p_total):,} ({len(p2p)} transactions)" if p2p else ""

        return (f"📊 *{label} Summary*\n\n"
                f"💰 *Total spent:* ₹{round(total_spend):,}\n"
                f"📝 Transactions: {len(spend)}"
                f"{invest_line}{p2p_line}\n\n"
                f"📂 *By category:*\n{cat_lines}\n\n"
                f"🏪 *Top merchants:*\n{merch_lines}\n\n"
                f"Ask me anything — 'food spend?', 'HDFC transactions?', 'compare with last month'")

    except Exception as e:
        return f"Error generating summary. Please try again. ({str(e)[:80]})"
