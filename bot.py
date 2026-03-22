"""
Hisaab Bot v2 — Full conversation handler
All 20 fixes implemented
"""
import os
from db import (get_user, create_user, save_consent, update_last_active,
                delete_user, get_gmail_accounts, get_sync_stats,
                get_recent_messages, save_message, check_rate_limit)
from gmail_reader import get_transactions, sync_all_gmail
from ai_brain import generate_reply
from time_periods import parse_time_period, get_time_period_buttons, CALLBACK_TO_PERIOD
from security import check_session_active
from admin import is_admin, handle_admin_command

BACKEND_URL = os.environ.get("BACKEND_URL", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

CONSENT_TEXT = """👋 Welcome to *Hisaab* — your personal finance assistant\.

Here's what I do:
• Read your bank alert emails \(HDFC, HSBC, ICICI, SBI & 11 more banks\)
• Categorise every transaction automatically
• Answer your spending questions in plain language
• Show weekly & monthly summaries

*Your privacy:*
🔒 Read\-only Gmail access — I only search bank alert emails
🇮🇳 Your data stays secure and encrypted
🗑️ Type *DELETE MY DATA* anytime to remove everything
📋 Type *my data* to see exactly what I've stored

Type *I AGREE* to continue\."""

AI_CONSENT_TEXT = """One more thing before we connect your Gmail\.

*AI Notice:* I use Claude AI to answer your questions\. Your transaction data \(amounts, merchant names, dates\) is sent to Anthropic's AI for analysis\.

*What is NOT sent:* Your email content, subjects, or personal emails\.

Type *I AGREE TO AI* to continue, or *NO AI* to use basic analysis only\."""

async def handle_message(user_id: str, message: str, platform: str = "telegram") -> tuple:
    """
    Returns (reply_text, inline_keyboard or None)
    """
    message_lower = message.lower().strip()

    # Rate limiting
    if not check_rate_limit(user_id):
        return "⏳ Slow down a little\! Max 10 messages per minute\.", None

    # Admin commands
    if is_admin(user_id) and message.startswith("/"):
        reply = await handle_admin_command(user_id, message)
        return reply, None

    # Delete data
    if message_lower in ["delete my data", "delete data"]:
        delete_user(user_id)
        return "✅ All your data has been permanently deleted\. Send anything to start fresh\.", None

    # Get or create user
    user = get_user(user_id)
    if not user:
        create_user(user_id)
        return CONSENT_TEXT, None

    # Update activity
    update_last_active(user_id)

    # Session timeout check
    if user.get("onboarded") and not check_session_active(user.get("last_active")):
        return "👋 Welcome back\! For your security, please confirm it's you\.\n\nType *yes it's me* to continue\.", None

    if message_lower == "yes it's me":
        update_last_active(user_id)
        return "✅ Verified\! Welcome back\. What would you like to know?", None

    # Step 1 — Main consent
    if not user.get("consent_given"):
        if message_lower == "i agree":
            save_consent(user_id)
            return AI_CONSENT_TEXT, None
        return CONSENT_TEXT, None

    # Step 2 — AI consent
    if not user.get("ai_consent_given"):
        if message_lower == "i agree to ai":
            save_consent(user_id, ai_consent=True)
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            return f"""✅ Perfect\! Now connect your Gmail so I can read your bank alerts\.

👉 [Tap here to connect Gmail]({auth_url})

*What I'll search for:* Emails from your bank \(HDFC, ICICI, SBI etc\.\) only\. Nothing else\.

Come back here once done\!""", None
        elif message_lower == "no ai":
            save_consent(user_id, ai_consent=False)
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            return f"""✅ Understood\. I'll use basic analysis only\.

👉 [Tap here to connect Gmail]({auth_url})""", None
        return AI_CONSENT_TEXT, None

    # Not yet connected Gmail
    if not user.get("onboarded"):
        auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
        accounts = get_gmail_accounts(user_id)
        if not accounts:
            return f"""You haven't connected your Gmail yet\.

👉 [Tap here to connect Gmail]({auth_url})""", None

    # Get Gmail accounts
    gmail_accounts = get_gmail_accounts(user_id)

    # ── Commands ──────────────────────────────────────────────

    if message_lower in ["hi", "hello", "hey", "/start", "start"]:
        name = user.get("name", "").split()[0] if user.get("name") else "there"
        accounts_info = "\n".join([f"  • {a['email']}" for a in gmail_accounts])
        return f"""Hi {name}\! 👋 I'm Hisaab, your expense assistant\.

*Connected Gmail:*
{accounts_info}

*What you can ask:*
• *summary* — spending overview with time period options
• *sync* — fetch latest transactions
• *my data* — see what I've stored
• *add gmail* — connect another Gmail account
• How much did I spend on food?
• Compare this week vs last week
• Any big transactions this month?
• Show my subscriptions

What would you like to know?""", None

    if message_lower in ["summary", "/summary"]:
        return "📅 *Select time period:*", get_time_period_buttons()

    if message_lower in ["sync", "/sync", "refresh"]:
        return await do_sync(user_id, gmail_accounts), None

    if message_lower == "my data":
        return await show_my_data(user_id, gmail_accounts), None

    if message_lower == "add gmail":
        accounts = get_gmail_accounts(user_id)
        if len(accounts) >= 3:
            return "You've already connected 3 Gmail accounts \(maximum\)\.", None
        auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
        return f"""👉 [Tap here to connect another Gmail]({auth_url})

You can connect up to 3 Gmail accounts\. Currently connected: {len(accounts)}/3""", None

    # Time period callbacks handled in main.py
    # Natural language time period detection
    from time_periods import parse_time_period
    time_keywords = ["this week", "last week", "this month", "last month", "3 months", "6 months", "this year"]
    if any(kw in message_lower for kw in time_keywords) and any(w in message_lower for w in ["summary", "spend", "spent", "show"]):
        start, end, label = parse_time_period(message_lower)
        return await generate_period_summary(user_id, gmail_accounts, start, end, label), None

    # AI chat
    save_message(user_id, "user", message)
    history = get_recent_messages(user_id, limit=6)

    try:
        transactions = get_transactions(user_id, gmail_accounts, days=90)
        reply = await generate_reply(transactions, history, message)
    except Exception as e:
        reply = f"Something went wrong\. Please try again\."

    save_message(user_id, "assistant", reply)
    return reply, None

async def handle_callback(user_id: str, callback_data: str) -> tuple:
    """Handle Telegram button presses."""
    from time_periods import CALLBACK_TO_PERIOD, parse_time_period

    if callback_data in CALLBACK_TO_PERIOD:
        period_text = CALLBACK_TO_PERIOD[callback_data]
        start, end, label = parse_time_period(period_text)
        gmail_accounts = get_gmail_accounts(user_id)
        reply = await generate_period_summary(user_id, gmail_accounts, start, end, label)
        return reply, None

    if callback_data == "period_custom":
        return "Please type your date range like:\n*from 1 Jan to 31 Jan*", None

    return "Unknown option\.", None

async def do_sync(user_id: str, gmail_accounts: list) -> str:
    try:
        result = sync_all_gmail(user_id, gmail_accounts)
        banks = ", ".join(result["banks_found"]) if result["banks_found"] else "none detected"
        new = result["new_transactions"]

        reply = f"✅ *Sync complete\!*\n\nNew transactions: *{new}*\nBanks found: {banks}\n\n"

        if result["banks_found"]:
            reply += "Type *summary* to see your spending\!"
        else:
            reply += """⚠️ No bank emails found\. Possible reasons:

1\. Bank alerts might be in *spam* — check and move to inbox
2\. Bank alerts not enabled — enable in your net banking app
3\. Connected wrong Gmail — type *add gmail* to add another account
4\. Bank not yet supported — which bank do you use?"""

        return reply
    except Exception as e:
        return f"Sync failed\. Please try again\."

async def show_my_data(user_id: str, gmail_accounts: list) -> str:
    stats = get_sync_stats(user_id)
    accounts_info = "\n".join([
        f"  • {s['email']}: {s['emails']} emails scanned, last synced {str(s['last_synced'])[:10]}"
        for s in stats["syncs"]
    ]) or "  None connected"

    return f"""📋 *Your Data*

*Connected Gmail accounts:*
{accounts_info}

*Transactions stored:* {stats['total_transactions']}
*Date range:* {stats['date_from'] or 'N/A'} to {stats['date_to'] or 'N/A'}

*Data retention:* Transactions kept for 12 months, chat history for 30 days\.

*Your rights:*
• Type *DELETE MY DATA* to permanently delete everything
• All data is encrypted and stored securely

*We ONLY read:* Emails from your bank's known sender addresses\. Nothing else\."""

async def generate_period_summary(user_id: str, gmail_accounts: list, start_date, end_date, label: str) -> str:
    try:
        from db import get_transactions_from_db
        # Sync first
        sync_all_gmail(user_id, gmail_accounts)
        transactions = get_transactions_from_db(user_id, start_date=start_date, end_date=end_date)

        if not transactions:
            return f"No transactions found for *{label}*\.\n\nTry *sync* to fetch latest emails\."

        spend_txns = [t for t in transactions if t.get("treatment") == "spend"]
        invest_txns = [t for t in transactions if t.get("treatment") == "investment"]

        total_spend = sum(t["amount"] for t in spend_txns)
        total_invest = sum(t["amount"] for t in invest_txns)

        cat_totals = {}
        for t in spend_txns:
            cat = t.get("category", "Other")
            cat_totals[cat] = cat_totals.get(cat, 0) + t["amount"]

        top_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)[:6]
        cat_lines = "\n".join([f"  • {c}: ₹{round(a):,}" for c, a in top_cats])

        merchant_totals = {}
        for t in spend_txns:
            m = t.get("merchant", "Unknown")
            if m and m not in ["Unknown", "Daily Spend"]:
                merchant_totals[m] = merchant_totals.get(m, 0) + t["amount"]

        top = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        merchant_lines = "\n".join([f"  • {m}: ₹{round(a):,}" for m, a in top])

        invest_line = f"\n📈 *Invested:* ₹{round(total_invest):,}" if total_invest > 0 else ""

        return f"""📊 *{label} Summary*

💰 *Total spent:* ₹{round(total_spend):,}
📝 Transactions: {len(spend_txns)}{invest_line}

📂 *By category:*
{cat_lines}

🏪 *Top merchants:*
{merchant_lines}

Ask me anything — "food spend?", "any subscriptions?", "compare with last month" """

    except Exception as e:
        return f"Error generating summary\. Please try again\."
