"""
AI brain — Claude-powered conversation about user's spending
"""
import os
import requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

def build_system_prompt(transactions: list) -> str:
    if not transactions:
        return """You are Hisaab, a friendly WhatsApp expense assistant for Indian users.
No transactions found. Tell the user politely and suggest they check their bank email alerts are enabled.
Keep responses under 200 words. Use ₹ for amounts. Be warm and conversational."""

    total = sum(t.get("amount", 0) for t in transactions)
    merchant_totals = {}
    mode_totals = {}

    for t in transactions:
        m = t.get("merchant", "Unknown")
        merchant_totals[m] = merchant_totals.get(m, 0) + t.get("amount", 0)
        mode = t.get("mode", "Other")
        mode_totals[mode] = mode_totals.get(mode, 0) + t.get("amount", 0)

    top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    txn_lines = "\n".join([
        f"- {t['date']} | {t['bank']} | {t['mode']} | {t['merchant']} | ₹{t['amount']}"
        for t in transactions[:50]
    ])
    merchant_lines = "\n".join([f"- {m}: ₹{round(a)}" for m, a in top_merchants])
    mode_lines = "\n".join([f"- {m}: ₹{round(a)}" for m, a in mode_totals.items()])

    return f"""You are Hisaab, a friendly WhatsApp expense assistant for Indian users.
Answer questions based ONLY on the transaction data below.

SUMMARY:
- Total transactions: {len(transactions)}
- Total spend: ₹{round(total):,}

TOP MERCHANTS:
{merchant_lines}

BY PAYMENT MODE:
{mode_lines}

ALL TRANSACTIONS (last 50):
{txn_lines}

RULES:
- Keep replies under 200 words (WhatsApp messages should be short)
- Use ₹ for amounts, format numbers with commas
- Be warm and conversational, like a knowledgeable friend
- If asked about something not in the data, say so clearly
- For UPI payments under ₹500 with unknown merchants, group as "small daily spends"
- Always end with a follow-up question or suggestion"""

async def generate_reply(transactions: list, history: list, user_message: str) -> str:
    system = build_system_prompt(transactions)
    messages = history + [{"role": "user", "content": user_message}]

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "system": system,
            "messages": messages,
        }
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]
