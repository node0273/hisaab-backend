"""
whatsapp.py — Send WhatsApp messages via Twilio
"""
import os
from twilio.rest import Client

ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
FROM_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")

def send_message(to_number: str, body: str):
    """Send a WhatsApp message to a phone number."""
    client = Client(ACCOUNT_SID, AUTH_TOKEN)
    if not to_number.startswith("whatsapp:"):
        to_number = f"whatsapp:{to_number}"
    client.messages.create(
        from_=FROM_NUMBER,
        to=to_number,
        body=body,
    )
