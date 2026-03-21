"""
Hisaab bot — core conversation handler (Telegram)
"""
import os
from db import get_user, create_user, save_consent, save_message, get_recent_messages, delete_user
from gmail_reader import get_transactions
from ai_brain import generate_reply

BACKEND_URL = os.environ.get("BACKEND_URL", "")

CONSENT_MESSAGE = """👋 Welcome to *Hisaab* — your personal finance assistant\.

Before we begin, here's what I do:
• Read your bank alert emails \(HDFC, HSBC & more\)
• Analyse where your money goes
• Answer your spending questions in plain language

*Your privacy:*
• Data stored securely in India 🇮🇳
• Read\-only Gmail access — no personal emails read
• Type *DELETE MY DATA* anytime to remove everything

Type *I AGREE* to continue\."""

async def handle_message(user_id: str, message: str) -> str:
    message_lower = message.lower().strip()

    if message_lower in ["delete my data", "delete data"]:
        delete_user(user_id)
        return "✅ Your data has been deleted\. Send anything to start fresh\."

    user = get_user(user_id)
    if not user:
        create_user(user_id)
        return CONSENT_MESSAGE

    if not user.get("consent_given"):
        if message_lower == "i agree":
            save_consent(user_id)
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            return f"✅ Thank you\!\n\nNow connect your Gmail so I can read your bank alerts\.\n\n👉 [Tap here to connect Gmail]({auth_url})\n\nCome back here once done\!"
        else:
            return CONSENT_MESSAGE

    if not user.get("onboarded"):
        auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
        return f"You haven't connected your Gmail yet\.\n\n👉 [Tap here to connect Gmail]({auth_url})"

    if message_lower in ["hi", "hello", "hey", "/start", "start"]:
        name = user.get("name", "").split()[0] if user.get("name") else "there"
        return f"Hi {name}\! 👋 I'm Hisaab, your expense assistant\.\n\nHere's what you can ask:\n• *summary* — 30\-day spending overview\n• How much did I spend on food?\n• What are my subscriptions?\n• Any unusual transactions?\n• Which merchant did I spend most on?\n\nWhat would you like to know?"

    if message_lower in ["summary", "/summary"]:
        return await generate_summary(user)

    save_message(user_id, "user", message)
    history = get_recent_messages(user_id, limit=8)

    try:
        transactions = get_transactions(
            access_token=user["access_token"],
            refresh_token=user["refresh_token"],
            days=30
        )
        reply = await generate_reply(transactions, history, message)
    except Exception as e:
        if "401" in str(e) or "403" in str(e):
            auth_url = f"{BACKEND_URL}/auth/google?number={user_id}"
            reply = f"Your Gmail access expired\. Please reconnect:\n{auth_url}"
        else:
            reply = f"Error: {str(e)}"

    save_message(user_id, "assistant", reply)
    return reply

async def generate_summary(user: dict) -> str:
    try:
        transactions = get_transactions(
            access_token=user["access_token"],
            refresh_token=user["refresh_token"],
            days=30
        )
        if not transactions:
            return "I couldn't find any bank transactions in the last 30 days\.\n\nPossible reasons:\n• Your bank alert emails are in a different Gmail account\n• Your bank doesn't send email alerts \(enable them in net banking\)\n• Emails might be in spam\n\nWhich bank do you use? I'll help check\."

        total = sum(t["amount"] for t in transactions)
        count = len(transactions)

        merchant_totals = {}
        for t in transactions:
            m = t.get("merchant", "Unknown")
            merchant_totals[m] = merchant_totals.get(m, 0) + t.get("amount", 0)

        top = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        top_lines = "\n".join([f"  • {m}: ₹{round(amt):,}" for m, amt in top])

        mode_totals = {}
        for t in transactions:
            mode = t.get("mode", "Other")
            mode_totals[mode] = mode_totals.get(mode, 0) + t.get("amount", 0)
        mode_lines = "\n".join([f"  • {m}: ₹{round(amt):,}" for m, amt in sorted(mode_totals.items(), key=lambda x: x[1], reverse=True)])

        return f"📊 *Your 30\-day Summary*\n\n💰 Total spent: *₹{round(total):,}*\n📝 Transactions: {count}\n\n🏪 *Top merchants:*\n{top_lines}\n\n💳 *By payment mode:*\n{mode_lines}\n\nAsk me anything — food spend?, big transactions?, compare weeks"

    except Exception as e:
        return f"Error fetching summary: {str(e)}"
