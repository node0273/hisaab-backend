"""
ai_chat.py — Claude-powered responses for Hisaab.
"""
import os
import requests
from analysis import get_spending_summary, fetch_transactions

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

async def get_ai_response(whatsapp_number: str, user_message: str, 
                           history: list, access_token: str, user_name: str) -> str:
    """Get AI response based on user's actual transaction data."""
    try:
        # Fetch transactions for context
        transactions = await fetch_transactions(access_token, days=30)
        txn_context = build_transaction_context(transactions)

        system_prompt = f"""You are Hisaab, a friendly personal finance assistant on WhatsApp for Indian users.

You have access to {user_name}'s bank transaction data from the last 30 days.

{txn_context}

Guidelines:
- Be conversational and friendly, like a helpful friend
- Use ₹ for amounts, format numbers clearly (e.g., ₹1,234)
- Keep responses concise — this is WhatsApp, not an email
- Use emojis sparingly but naturally
- If asked about something not in the data, say so honestly
- For spending questions, give specific numbers from the data
- Highlight anything unusual or worth noting
- Respond in the same language the user writes in (Hindi/English/Hinglish)
- Never share raw token or technical data"""

        messages = []
        for h in history[-6:]:
            messages.append({"role": h["role"], "content": h["message"]})
        messages.append({"role": "user", "content": user_message})

        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": os.environ.get("ANTHROPIC_API_KEY"),
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 500,
                "system": system_prompt,
                "messages": messages,
            },
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"]
    except Exception as e:
        return f"Sorry, I couldn't process that right now. Please try again in a moment. 🙏"

def build_transaction_context(transactions: list) -> str:
    if not transactions:
        return "No transactions found for the last 30 days."

    total = sum(t["amount"] for t in transactions)
    merchant_totals = {}
    for t in transactions:
        m = t.get("merchant", "Unknown")
        merchant_totals[m] = merchant_totals.get(m, 0) + t["amount"]

    top = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    top_lines = "\n".join([f"  • {m}: ₹{round(v):,}" for m, v in top])

    txn_lines = "\n".join([
        f"  {t['date']} | {t['bank']} | {t['merchant']} | ₹{t['amount']:,.0f} | {t['mode']}"
        for t in transactions[:50]
    ])

    return f"""TRANSACTION DATA (last 30 days):
Total spend: ₹{round(total):,}
Transaction count: {len(transactions)}

TOP MERCHANTS:
{top_lines}

RECENT TRANSACTIONS:
{txn_lines}"""
