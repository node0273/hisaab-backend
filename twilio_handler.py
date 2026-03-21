"""
twilio_handler.py — Send WhatsApp messages via Twilio.
"""
import os
from twilio.rest import Client

ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

def send_whatsapp(to_number: str, message: str):
    """Send a WhatsApp message to a user."""
    client = Client(ACCOUNT_SID, AUTH_TOKEN)
    to = f"whatsapp:{to_number}" if not to_number.startswith("whatsapp:") else to_number
    client.messages.create(
        from_=WHATSAPP_FROM,
        to=to,
        body=message
    )
