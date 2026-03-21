"""
ai_parser.py — AI-powered email parser + rule generator
Invoked only when no existing rule matches
"""
import os
import json
import requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

def call_claude(prompt: str, max_tokens: int = 500) -> str:
    resp = requests.post(
        ANTHROPIC_URL,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        },
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]

def ai_parse_email(subject: str, sender: str, body: str) -> dict:
    """Use AI to extract transaction data from an unknown email format."""
    prompt = f"""Extract transaction details from this bank alert email.

Subject: {subject}
From: {sender}
Body: {body[:1000]}

Return ONLY a JSON object with these fields (no other text):
{{
  "amount": <number or null>,
  "merchant": "<string or null>",
  "mode": "<UPI/Credit Card/Debit Card/NEFT/IMPS/NACH/Other>",
  "bank": "<bank name>",
  "is_debit": <true/false>,
  "confidence": <0.0-1.0>
}}

If this is NOT a transaction alert, return: {{"amount": null}}"""

    try:
        response = call_claude(prompt)
        response = response.strip()
        if "```" in response:
            response = response.split("```")[1]
            if response.startswith("json"):
                response = response[4:]
        return json.loads(response.strip())
    except Exception as e:
        print(f"AI parse failed: {e}")
        return {"amount": None}

def ai_generate_rule(subject: str, sender: str, body: str, parsed: dict) -> dict:
    """Generate a regex-based rule from an AI-parsed email."""
    prompt = f"""I parsed this bank email and extracted:
- Amount: {parsed.get('amount')}
- Merchant: {parsed.get('merchant')}
- Mode: {parsed.get('mode')}

Email details:
Subject: {subject}
From: {sender}
Body: {body[:800]}

Generate regex patterns to extract amount and merchant from similar emails.
Return ONLY a JSON object:
{{
  "amount_pattern": "<regex with one capture group for amount>",
  "merchant_pattern": "<regex with one capture group for merchant, or null>",
  "confidence": <0.0-1.0>
}}"""

    try:
        response = call_claude(prompt)
        response = response.strip()
        if "```" in response:
            response = response.split("```")[1]
            if response.startswith("json"):
                response = response[4:]
        return json.loads(response.strip())
    except Exception as e:
        print(f"Rule generation failed: {e}")
        return None
