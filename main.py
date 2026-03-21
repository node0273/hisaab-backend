from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import os
import httpx
from bot import handle_message
from auth_link import get_google_auth_url, handle_google_callback

app = FastAPI(title="Hisaab Bot")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
BACKEND_URL = os.environ.get("BACKEND_URL", "")

async def send_telegram(chat_id: int, text: str):
    async with httpx.AsyncClient() as client:
        await client.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        })

@app.get("/")
def root():
    return {"status": "Hisaab is running"}

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    data = await request.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "").strip()
    if not chat_id or not text:
        return {"ok": True}
    user_id = str(chat_id)
    reply = await handle_message(user_id, text)
    await send_telegram(chat_id, reply)
    return {"ok": True}

@app.get("/auth/google")
def google_auth(number: str):
    url = get_google_auth_url(number)
    return RedirectResponse(url=url)

@app.get("/auth/callback")
async def google_callback(code: str, state: str):
    result = await handle_google_callback(code, state)
    return HTMLResponse(content=result)

@app.on_event("startup")
async def set_webhook():
    if BACKEND_URL and TELEGRAM_TOKEN:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{TELEGRAM_API}/setWebhook", json={
                "url": f"{BACKEND_URL}/webhook/telegram"
            })
            print(f"Webhook set: {r.json()}")
