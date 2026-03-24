"""Hisaab FastAPI — main server"""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import os, httpx

from bot import handle_message, handle_callback
from auth_link import get_auth_url, handle_callback as handle_oauth
from admin import send_admin_alert
from security import sanitise_log

app = FastAPI(title="Hisaab")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
BACKEND_URL = os.environ.get("BACKEND_URL", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "hisaab_2024")

async def send_telegram(chat_id: int, text: str, keyboard=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = {"inline_keyboard": keyboard}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json=payload)
    except Exception as e:
        print(f"send_telegram error: {sanitise_log(str(e)[:80])}")

@app.get("/")
def root():
    return {"status": "Hisaab running", "version": "3.0"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    # Validate Telegram secret
    if WEBHOOK_SECRET:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if secret and secret != WEBHOOK_SECRET:
            return {"ok": False}

    data = await request.json()

    # Button press
    if "callback_query" in data:
        cq = data["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        user_id = str(chat_id)
        reply, keyboard = await handle_callback(user_id, cq.get("data", ""))
        await send_telegram(chat_id, reply, keyboard)
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(f"{TELEGRAM_API}/answerCallbackQuery",
                              json={"callback_query_id": cq["id"]})
        return {"ok": True}

    # Regular message
    msg = data.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    text = msg.get("text", "").strip()
    if not chat_id or not text:
        return {"ok": True}

    user_id = str(chat_id)
    try:
        reply, keyboard = await handle_message(user_id, text)
        await send_telegram(chat_id, reply, keyboard)
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print(f"ERROR user={user_id}: {err}")
        send_admin_alert(f"🚨 Error for user {user_id[:4]}...\n`{sanitise_log(str(e)[:200])}`")
        await send_telegram(chat_id, f"Something went wrong. Please try again.\n\nError: {str(e)[:100]}")

    return {"ok": True}

@app.get("/auth/google")
def google_auth(number: str):
    return RedirectResponse(url=get_auth_url(number))

@app.get("/auth/callback")
async def google_callback(code: str, state: str):
    result = await handle_oauth(code, state)
    return HTMLResponse(content=result)

@app.get("/admin/health")
def admin_health():
    from admin import send_admin_alert
    from db import get_admin_stats
    try:
        s = get_admin_stats()
        send_admin_alert(f"✅ *Health Check*\nUsers: {s['active_users']}\nTxns today: {s['transactions_today']}\nPending: {s['pending_merchants']} merchants, {s['pending_parsing']} rules")
    except Exception as e:
        send_admin_alert(f"🚨 Health check failed: {str(e)[:100]}")
    return {"status": "ok"}

@app.on_event("startup")
async def startup():
    if BACKEND_URL and TELEGRAM_TOKEN:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{TELEGRAM_API}/setWebhook", json={
                "url": f"{BACKEND_URL}/webhook/telegram",
                "secret_token": WEBHOOK_SECRET,
                "allowed_updates": ["message", "callback_query"]
            })
            result = r.json()
            if result.get("ok"):
                send_admin_alert("✅ *Hisaab v3 started!* Webhook registered.")
            else:
                send_admin_alert(f"⚠️ Webhook failed: {str(result)[:100]}")
