import os
from twilio.rest import Client

ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
FROM_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")

def send_whatsapp(to_number: str, message: str):
    client = Client(ACCOUNT_SID, AUTH_TOKEN)
    # Clean the number — remove whatsapp: prefix if present
    to_number = to_number.replace("whatsapp:", "").strip()
    # Add + only if not already there
    if not to_number.startswith("+"):
        to_number = "+" + to_number
    # Add whatsapp: prefix
    to_number = f"whatsapp:{to_number}"
    client.messages.create(
        body=message,
        from_=FROM_NUMBER,
        to=to_number
    )
