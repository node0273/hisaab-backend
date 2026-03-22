"""
AI Brain — Claude-powered conversation with data minimisation
Only sends relevant transactions based on the user's question
"""
import os, requests, re

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

CATEGORY_KEYWORDS = {
    "food": ["Food & Dining", "Groceries"],
    "restaurant": ["Food & Dining"],
    "zomato": ["Food & Dining"], "swiggy": ["Food & Dining"],
    "grocery": ["Groceries"], "bigbasket": ["Groceries"],
    "travel": ["Travel & Transport"], "transport": ["Travel & Transport"],
    "uber": ["Travel & Transport"], "ola": ["Travel & Transport"],
    "fuel": ["Fuel"], "petrol": ["Fuel"],
    "entertainment": ["Entertainment & OTT"], "ott": ["Entertainment & OTT"],
    "netflix": ["Entertainment & OTT"], "hotstar": ["Entertainment & OTT"],
    "health": ["Health & Medical"], "medical": ["Health & Medical"], "doctor": ["Health & Medical"],
    "bill": ["Utilities & Bills"], "recharge": ["Utilities & Bills"], "electricity": ["Utilities & Bills"],
    "subscription": ["Subscriptions"],
    "shopping": ["Shopping"], "amazon": ["Shopping"], "flipkart": ["Shopping"],
    "invest": ["Investments & Finance"], "sip": ["Investments & Finance"], "mutual fund": ["Investments & Finance"],
    "insurance": ["Insurance"],
    "emi": ["EMI & Loans"], "loan": ["EMI & Loans"],
    "education": ["Education"],
    "rent": ["Rent"],
    "upi": None,  # All UPI transactions
    "transfer": ["P2P Transfer", "Daily Spend"],
}

def get_relevant_categories(question: str) -> list:
    """Extract relevant categories from user question."""
    question_lower = question.lower()
    categories = set()
    for keyword, cats in CATEGORY_KEYWORDS.items():
        if keyword in question_lower and cats:
            categories.update(cats)
    return list(categories) if categories else None  # None = all categories

def filter_transactions(transactions: list, question: str) -> list:
    """Filter transactions by relevance to question — data minimisation."""
    relevant_cats = get_relevant_categories(question)

    # If asking about specific time period already filtered by DB query
    # If asking about specific category, filter by it
    if relevant_cats:
        filtered = [t for t in transactions if t.get("category") in relevant_cats]
        # If filter returns too few, return all (fallback)
        return filtered if len(filtered) >= 3 else transactions

    return transactions

def build_system_prompt(transactions: list, label: str = "Last 30 days") -> str:
    if not transactions:
        return f"""You are Hisaab, a friendly Indian expense assistant on Telegram.
No transactions found for {label}. Tell user to try 'sync' or check Gmail connection.
Keep responses under 150 words. Use ₹ for amounts."""

    total = sum(t.get("amount", 0) for t in transactions if t.get("treatment") == "spend")
    invested = sum(t.get("amount", 0) for t in transactions if t.get("treatment") == "investment")

    cat_totals = {}
    for t in transactions:
        if t.get("treatment") == "spend":
            cat = t.get("category", "Other")
            cat_totals[cat] = cat_totals.get(cat, 0) + t.get("amount", 0)

    top_cats = sorted(cat_totals.items(), key=lambda x: x[1], reverse=True)
    cat_lines = "\n".join([f"- {c}: ₹{round(a):,}" for c, a in top_cats])

    merchant_totals = {}
    for t in transactions:
        if t.get("treatment") == "spend":
            m = t.get("merchant", "Unknown")
            merchant_totals[m] = merchant_totals.get(m, 0) + t.get("amount", 0)

    top_merchants = sorted(merchant_totals.items(), key=lambda x: x[1], reverse=True)[:8]
    merchant_lines = "\n".join([f"- {m}: ₹{round(a):,}" for m, a in top_merchants])

    recent = transactions[:20]
    txn_lines = "\n".join([
        f"- {t.get('date','')} | {t.get('category','')} | {t.get('merchant','?')} | ₹{t.get('amount',0)}"
        for t in recent
    ])

    return f"""You are Hisaab, a friendly Indian personal finance assistant on Telegram.
Period: {label}

SPEND TOTAL: ₹{round(total):,} across {len([t for t in transactions if t.get('treatment')=='spend'])} transactions
INVESTED: ₹{round(invested):,}

BY CATEGORY:
{cat_lines}

TOP MERCHANTS:
{merchant_lines}

RECENT TRANSACTIONS:
{txn_lines}

RULES:
- Keep replies under 150 words
- Use ₹ with commas (₹1,234)
- Be warm like a knowledgeable friend
- Investments shown separately, not in spend total
- Credit card payments and EMIs are excluded from totals (they are settlements)
- For date comparisons use transaction dates
- Use Telegram markdown: *bold*"""

async def generate_reply(transactions: list, history: list, user_message: str, label: str = "Last 30 days") -> str:
    # Data minimisation — only send relevant transactions
    filtered = filter_transactions(transactions, user_message)
    system = build_system_prompt(filtered, label)
    messages = history[-6:] + [{"role": "user", "content": user_message}]

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 400,
            "system": system,
            "messages": messages,
        }
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]
