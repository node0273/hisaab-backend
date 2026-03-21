from fastapi import FastAPI, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel
from typing import List, Dict, Any
import os
from bot import handle_message
from auth_link import get_google_auth_url, handle_google_callback

app = FastAPI(title="Hisaab Bot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "Hisaab is running"}

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(
    request: Request,
    From: str = Form(...),
    Body: str = Form(...),
):
    whatsapp_number = From.replace("whatsapp:", "")
    message = Body.strip()
    reply = await handle_message(whatsapp_number, message)
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Message>{reply}</Message>
</Response>"""
    return PlainTextResponse(content=twiml, media_type="application/xml")

@app.get("/auth/google")
def google_auth(number: str):
    """Redirects user directly to Google OAuth — no JSON, no file download."""
    url = get_google_auth_url(number)
    return RedirectResponse(url=url)

@app.get("/auth/callback")
async def google_callback(code: str, state: str):
    result = await handle_google_callback(code, state)
    return HTMLResponse(content=result)
