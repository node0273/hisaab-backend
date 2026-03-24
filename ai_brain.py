"""AI Brain — answers spending questions from DB data"""
import os, requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

def build_context(transactions: list, label: str) -> str:
    if not transactions:
        return f"No transactions found for {label}. Tell user to type 'sync'."

    spend = [t for t in transactions if t.get("treatment") == "spend"]
    invest = [t for t in transactions if t.get("treatment") == "investment"]
    total_spend = sum(t["amount"] for t in spend)
    total_invest = sum(t["amount"] for t in invest)

    # Category breakdown
    cat = {}
    for t in spend:
        c = t.get("category", "Other")
        cat[c] = cat.get(c, 0) + t["amount"]
    cat_lines = "\n".join([f"- {c}: ₹{round(a):,}" for c, a in sorted(cat.items(), key=lambda x: x[1], reverse=True)])

    # Merchant breakdown
    merch = {}
    for t in spend:
        m = t.get("merchant", "Unknown")
        if m and m not in ["Unknown", "P2P Transfer", "Daily Spend"]:
            merch[m] = merch.get(m, 0) + t["amount"]
    merch_lines = "\n".join([f"- {m}: ₹{round(a):,}" for m, a in sorted(merch.items(), key=lambda x: x[1], reverse=True)[:10]])

    # Bank breakdown
    bank = {}
    for t in spend:
        b = t.get("bank", "Unknown")
        bank[b] = bank.get(b, 0) + t["amount"]
    bank_lines = "\n".join([f"- {b}: ₹{round(a):,}" for b, a in sorted(bank.items(), key=lambda x: x[1], reverse=True)])

    # P2P breakdown
    p2p = [t for t in transactions if t.get("category") in ["P2P Transfer", "Daily Spend"]]
    p2p_total = sum(t["amount"] for t in p2p)
    p2p_lines = ""
    if p2p:
        p2p_detail = {}
        for t in p2p:
            key = t.get("person_name") or t.get("upi_app") or "Unknown"
            p2p_detail[key] = p2p_detail.get(key, 0) + t["amount"]
        p2p_lines = f"\nP2P TRANSFERS: ₹{round(p2p_total):,}\n" + \
                    "\n".join([f"- {k}: ₹{round(v):,}" for k, v in sorted(p2p_detail.items(), key=lambda x: x[1], reverse=True)[:8]])

    # Recent transactions
    recent = "\n".join([
        f"- {t.get('date','')} | {t.get('bank','')} | {t.get('mode','')} | {t.get('merchant','?')} | ₹{t.get('amount',0):,.0f}"
        for t in transactions[:25]
    ])

    return f"""Period: {label}
TOTAL SPEND: ₹{round(total_spend):,} ({len(spend)} transactions)
INVESTED: ₹{round(total_invest):,}

BY CATEGORY:
{cat_lines}

TOP MERCHANTS:
{merch_lines}

BY BANK:
{bank_lines}
{p2p_lines}

RECENT TRANSACTIONS:
{recent}"""

async def generate_reply(transactions: list, history: list,
                         question: str, label: str = "Last 30 days") -> str:
    context = build_context(transactions, label)
    system = f"""You are Hisaab, a friendly Indian personal finance assistant on Telegram.
Answer questions based ONLY on the transaction data below.

{context}

RULES:
- Keep replies under 150 words
- Use ₹ with commas (₹1,234)
- Be warm and conversational
- Investments shown separately from spend
- Credit card payments and EMIs excluded from spend totals
- For P2P: show person name if available, else UPI app name
- Use Markdown: *bold*"""

    messages = history[-6:] + [{"role": "user", "content": question}]
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001",
                  "max_tokens": 400, "system": system,
                  "messages": messages}
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"]
    except Exception as e:
        return f"Error: {str(e)[:100]}"
