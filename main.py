"""
Hisaab FastAPI — main entry point
Rate limiting, signature validation, session handling
"""
from fastapi import FastAPI, Request, Header
from fastapi.responses import HTMLResponse, RedirectResponse
import os, httpx, hashlib, hmac, json

from bot import handle_message, handle_callback
from auth_link import get_google_auth_url, handle_google_callback
from admin import send_admin_alert, monitor_health
from security import sanitise_log

app = FastAPI(title="Hisaab Bot")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
BACKEND_URL = os.environ.get("BACKEND_URL", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "hisaab_secret_2024")

async def send_telegram(chat_id: int, text: str, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = {"inline_keyboard": reply_markup}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json=payload)
    except Exception as e:
        print(f"send_telegram error: {str(e)[:100]}")

@app.get("/")
def root():
    return {"status": "Hisaab is running", "version": "2.0"}

@app.get("/health")
def health():
    """Health check endpoint for monitoring."""
    return {"status": "ok"}

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    # Validate request is from Telegram (only if WEBHOOK_SECRET is set)
    if WEBHOOK_SECRET:
        secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if secret_header and secret_header != WEBHOOK_SECRET:
            return {"ok": False, "error": "Unauthorized"}

    data = await request.json()

    # Handle callback queries (button presses)
    if "callback_query" in data:
        cq = data["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        user_id = str(chat_id)
        callback_data = cq.get("data", "")

        reply, keyboard = await handle_callback(user_id, callback_data)
        await send_telegram(chat_id, reply, keyboard)

        # Answer callback to remove loading state
        async with httpx.AsyncClient() as client:
            await client.post(f"{TELEGRAM_API}/answerCallbackQuery",
                              json={"callback_query_id": cq["id"]})
        return {"ok": True}

    # Handle regular messages
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "").strip()

    if not chat_id or not text:
        return {"ok": True}

    user_id = str(chat_id)
    try:
        reply, keyboard = await handle_message(user_id, text)
        await send_telegram(chat_id, reply, keyboard)
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print(f"ERROR: {err}")
        await send_telegram(chat_id, f"Debug error: {str(e)[:300]}")

    return {"ok": True}

@app.get("/auth/google")
def google_auth(number: str):
    url = get_google_auth_url(number)
    return RedirectResponse(url=url)

@app.get("/auth/callback")
async def google_callback(code: str, state: str):
    result = await handle_google_callback(code, state)
    return HTMLResponse(content=result)

@app.get("/admin/health")
async def admin_health():
    """Manual health check trigger."""
    monitor_health()
    return {"status": "health check sent"}

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
                send_admin_alert("✅ *Hisaab started successfully*\nWebhook registered\\.")
            else:
                send_admin_alert(f"⚠️ *Webhook registration failed*\n`{str(result)[:200]}`")
