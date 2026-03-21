"""
conversation.py — Handles the WhatsApp conversation flow.

States:
  new         → First time user, show consent
  consent     → Waiting for consent
  linking     → Sent OAuth link, waiting for Gmail connection
  active      → Fully onboarded, AI chat mode
"""
import os
from db import (get_user_by_whatsapp, create_user, update_consent,
                save_conversation, get_recent_conversations, get_tokens)
from twilio_handler import send_whatsapp
from ai_chat import get_ai_response
from analysis import get_spending_summary

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

CONSENT_MESSAGE = """🙏 *Welcome to Hisaab!*

I'm your personal finance assistant. I help you understand exactly where your money goes — across all your bank accounts.

Here's what I do:
• Read your bank alert emails (HDFC, HSBC, and more)
• Show you a smart spending summary
• Answer questions like "How much on food this month?"
• Alert you about unusual transactions every week

*Before we begin, I need your consent:*

📋 I will read your Gmail bank alert emails only
🔐 Your data is stored securely in India
🚫 I never read personal emails
🗑️ Reply *DELETE* anytime to remove all your data

Reply *YES* to continue or *NO* to cancel."""

ALREADY_LINKED_MESSAGE = """✅ Your Gmail is already connected!

Try asking me:
• *summary* — this month's spending
• *last week* — last 7 days
• How much did I spend on food?
• What are my subscriptions?
• Any unusual transactions?"""

async def handle_message(whatsapp_number: str, message: str):
    """Main entry point for all incoming WhatsApp messages."""
    msg = message.strip().lower()
    user = get_user_by_whatsapp(whatsapp_number)

    # Save incoming message
    if user:
        save_conversation(whatsapp_number, "user", message)

    # --- NEW USER ---
    if not user:
        create_user(whatsapp_number)
        send_whatsapp(whatsapp_number, CONSENT_MESSAGE)
        return

    # --- DELETE DATA ---
    if msg == "delete":
        handle_delete(whatsapp_number)
        return

    # --- CONSENT PENDING ---
    if not user["consent_given"]:
        if msg in ["yes", "y", "agree", "ok", "okay", "haan", "ha"]:
            update_consent(whatsapp_number)
            send_oauth_link(whatsapp_number)
        elif msg in ["no", "n", "nahi", "nope"]:
            send_whatsapp(whatsapp_number,
                "No problem! If you change your mind, just message me again. 😊")
        else:
            send_whatsapp(whatsapp_number,
                "Please reply *YES* to connect your Gmail or *NO* to cancel.")
        return

    # --- NOT YET LINKED ---
    if not user["onboarded"]:
        if msg in ["link", "connect", "login", "signin"]:
            send_oauth_link(whatsapp_number)
        else:
            send_whatsapp(whatsapp_number,
                "I'm waiting for you to connect your Gmail. "
                "Click the link I sent earlier or reply *link* to get a new one.")
        return

    # --- FULLY ONBOARDED — AI CHAT MODE ---
    access_token, refresh_token = get_tokens(whatsapp_number)
    if not access_token:
        send_whatsapp(whatsapp_number,
            "⚠️ Your Gmail connection expired. Reply *link* to reconnect.")
        return

    # Quick commands
    if msg in ["summary", "show summary", "spending", "report"]:
        send_whatsapp(whatsapp_number, "⏳ Getting your spending summary...")
        summary = await get_spending_summary(whatsapp_number, access_token, days=30)
        send_whatsapp(whatsapp_number, summary)
        save_conversation(whatsapp_number, "assistant", summary)
        return

    if msg in ["last week", "this week", "week"]:
        send_whatsapp(whatsapp_number, "⏳ Getting last 7 days...")
        summary = await get_spending_summary(whatsapp_number, access_token, days=7)
        send_whatsapp(whatsapp_number, summary)
        save_conversation(whatsapp_number, "assistant", summary)
        return

    if msg in ["link", "relink", "reconnect"]:
        send_oauth_link(whatsapp_number)
        return

    # AI response for everything else
    send_whatsapp(whatsapp_number, "🤔 Let me check...")
    history = get_recent_conversations(whatsapp_number, limit=8)
    reply = await get_ai_response(
        whatsapp_number=whatsapp_number,
        user_message=message,
        history=history,
        access_token=access_token,
        user_name=user.get("name", "").split()[0] if user.get("name") else "there"
    )
    send_whatsapp(whatsapp_number, reply)
    save_conversation(whatsapp_number, "assistant", reply)

def send_oauth_link(whatsapp_number: str):
    """Send Gmail OAuth link to user."""
    from auth import get_oauth_url
    url = get_oauth_url(state=whatsapp_number)
    send_whatsapp(
        whatsapp_number,
        f"🔗 *Connect your Gmail*\n\n"
        f"Tap the link below to securely connect your Gmail account:\n\n"
        f"{url}\n\n"
        f"_This link is valid for 10 minutes. Your data stays in India and is fully encrypted._"
    )

def handle_delete(whatsapp_number: str):
    """Delete all user data."""
    import psycopg2
    from db import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conversations WHERE whatsapp_number = %s", (whatsapp_number,))
            cur.execute("DELETE FROM unknown_emails WHERE whatsapp_number = %s", (whatsapp_number,))
            cur.execute("DELETE FROM users WHERE whatsapp_number = %s", (whatsapp_number,))
            conn.commit()
    send_whatsapp(whatsapp_number,
        "🗑️ All your data has been permanently deleted. "
        "Message me anytime if you want to start again.")
