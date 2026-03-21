"""
Hisaab bot — core conversation handler
Manages user state and routes messages
"""
import os
from db import (get_user, create_user, save_consent, save_message,
                get_recent_messages, delete_user)
from auth_link import get_google_auth_url
from gmail_reader import get_transactions
from ai_brain import generate_reply

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

CONSENT_MESSAGE = """Welcome to *Hisaab* — your personal finance assistant.

Before we begin, here's what I do:
• Read your bank alert emails (HDFC, HSBC, and more)
• Analyse where your money goes
• Answer your questions about your spending

*Your privacy matters:*
• Your data is stored securely in India
• I never read personal emails — only bank alerts
• You can delete your data anytime by typing *DELETE MY DATA*
• Read-only access to Gmail

Type *I AGREE* to continue."""

async def handle_message(whatsapp_number: str, message: str) -> str:
    message_lower = message.lower().strip()

    # Delete data request
    if message_lower in ["delete my data", "delete data", "deletedata"]:
        delete_user(whatsapp_number)
        return "Your data has been deleted. Type anything to start fresh."

    # Get or create user
    user = get_user(whatsapp_number)
    if not user:
        create_user(whatsapp_number)
        return CONSENT_MESSAGE

    # Consent flow
    if not user.get("consent_given"):
        if message_lower == "i agree":
            save_consent(whatsapp_number)
            backend_url = BACKEND_URL
            auth_url = f"{backend_url}/auth/google?number={whatsapp_number}"
            return f"""Thank you! Now connect your Gmail so I can read your bank alerts.

Tap the link below to sign in with Google:
{auth_url}

This takes 30 seconds. Come back here once done."""
        else:
            return CONSENT_MESSAGE

    # Not yet onboarded (Gmail not connected)
    if not user.get("onboarded"):
        auth_url = f"{BACKEND_URL}/auth/google?number={whatsapp_number}"
        return f"""You haven't connected your Gmail yet.

Tap this link to connect:
{auth_url}"""

    # Fully onboarded — handle commands
    if message_lower in ["hi", "hello", "hey", "hii", "helo"]:
        name = user.get("name", "").split()[0] if user.get("name") else "there"
        return f"""Hi {name}! I'm Hisaab, your expense assistant.

Here's what you can ask me:
• *summary* — spending overview
• *this month* — current month spend
• *last month* — previous month
• How much did I spend on food?
• What are my subscriptions?
• Any unusual transactions?

What would you like to know?"""

    if message_lower in ["summary", "show summary", "spending summary"]:
        return await generate_summary(user)

    # AI-powered chat for everything else
    save_message(whatsapp_number, "user", message)
    history = get_recent_messages(whatsapp_number, limit=8)

    try:
        transactions = get_transactions(
            access_token=user["access_token"],
            refresh_token=user["refresh_token"],
            days=30
        )
        reply = await generate_reply(transactions, history, message)
    except Exception as e:
        if "401" in str(e) or "403" in str(e):
            auth_url = f"{BACKEND_URL}/auth/google?number={whatsapp_number}"
            reply = f"Your Gmail access expired. Please reconnect:\n{auth_url}"
        else:
            reply = "Sorry, I had trouble reading your emails. Please try again in a moment."

    save_message(whatsapp_number, "assistant", reply)
    return reply

async def generate_summary(user: dict) -> str:
    try:
        transactions = get_transactions(
            access_token=user["access_token"],
            refresh_token=user["refresh_token"],
            days=30
        )
        if not transactions:
            return "I couldn't find any bank transactions in the last 30 days. Make sure your bank sends email alerts."

        total = sum(t["amount"] for t in transactions if t.get("amount"))
        count = len(transactions)

        # Group by merchant
        merchant_totals = {}
        for t in transactions:
            m = t.get("merchant", "Unknown")
            merchant_totals[m] = merchant_totals.get(m, 0) + t.get("amount", 0)

        top = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        top_lines = "\n".join([f"  • {m}: ₹{round(amt):,}" for m, amt in top])

        # Group by mode
        mode_totals = {}
        for t in transactions:
            mode = t.get("mode", "Other")
            mode_totals[mode] = mode_totals.get(mode, 0) + t.get("amount", 0)

        mode_lines = "\n".join([f"  • {m}: ₹{round(amt):,}" for m, amt in sorted(mode_totals.items(), key=lambda x: x[1], reverse=True)])

        return f"""*Your 30-day Spending Summary*

Total spent: *₹{round(total):,}*
Transactions: {count}

*Top merchants:*
{top_lines}

*By payment mode:*
{mode_lines}

Ask me anything — "how much on food?", "any big transactions?", "compare this week vs last week" """

    except Exception as e:
        return f"Couldn't fetch your summary right now. Try again in a moment."
