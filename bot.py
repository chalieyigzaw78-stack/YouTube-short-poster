"""
YouTube Shorts Scheduler Bot
-----------------------------
Telegram bot that lets you upload videos, add a caption and schedule date/time,
then automatically posts them to your YouTube channel as Shorts.

FLOW:
  1. Send a video to the bot
  2. Bot asks for title/caption
  3. Bot asks for schedule date/time (or "now")
  4. At scheduled time, bot uploads to YouTube Shorts automatically

ENV VARS:
  BOT_TOKEN=              Telegram bot token
  ADMIN_ID=               Your Telegram user ID
  YOUTUBE_CLIENT_SECRET=  Path to client_secret JSON file (or inline JSON string)
  YOUTUBE_TOKEN_FILE=     Path to store OAuth token (default: token.json)
  DATABASE_URL=           PostgreSQL connection string (Neon)
  PORT=                   HTTP port for Render (default: 10000)
"""

import asyncio
import json
import os
import logging
import tempfile
import aiohttp
from aiohttp import web
from datetime import datetime
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import psycopg

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN  = os.environ["BOT_TOKEN"]
ADMIN_ID   = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL = os.environ["DATABASE_URL"]
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "./client_secret.json")
YOUTUBE_TOKEN_FILE    = os.environ.get("YOUTUBE_TOKEN_FILE", "./token.json")

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Conversation states
WAIT_VIDEO, WAIT_TITLE, WAIT_SCHEDULE = range(3)


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg.connect(DATABASE_URL)


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_videos (
                id SERIAL PRIMARY KEY,
                file_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                scheduled_at TIMESTAMP NOT NULL,
                posted BOOLEAN DEFAULT FALSE,
                posted_at TIMESTAMP,
                youtube_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()


# ── YouTube auth ──────────────────────────────────────────────────────────────

def get_youtube_service():
    creds = None

    # Support token as env var (JSON string) or file path
    token_env = os.environ.get("YOUTUBE_TOKEN", "")
    if token_env.strip().startswith("{"):
        creds = Credentials.from_authorized_user_info(json.loads(token_env), SCOPES)
    elif os.path.exists(YOUTUBE_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(YOUTUBE_TOKEN_FILE, SCOPES)

    # Refresh if expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Save refreshed token back
        if token_env.strip().startswith("{"):
            os.environ["YOUTUBE_TOKEN"] = creds.to_json()
        else:
            with open(YOUTUBE_TOKEN_FILE, "w") as f:
                f.write(creds.to_json())

    if not creds or not creds.valid:
        raise RuntimeError("YouTube not authorized.")

    return build("youtube", "v3", credentials=creds)


def run_auth_flow():
    """Run OAuth flow to get token. Call this once locally to generate token.json"""
    # Support inline JSON string or file path
    if YOUTUBE_CLIENT_SECRET.strip().startswith("{"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(YOUTUBE_CLIENT_SECRET)
            secret_path = f.name
    else:
        secret_path = YOUTUBE_CLIENT_SECRET

    flow = InstalledAppFlow.from_client_secrets_file(secret_path, SCOPES)
    creds = flow.run_local_server(port=0)
    with open(YOUTUBE_TOKEN_FILE, "w") as f:
        f.write(creds.to_json())
    print(f"[auth] Token saved to {YOUTUBE_TOKEN_FILE}")
    return creds


# ── Upload to YouTube ─────────────────────────────────────────────────────────

async def upload_to_youtube(video_path: str, title: str, description: str = "") -> str:
    """Upload video to YouTube as a Short. Returns YouTube video ID."""
    loop = asyncio.get_event_loop()

    def _upload():
        youtube = get_youtube_service()
        body = {
            "snippet": {
                "title": title,
                "description": description + "\n\n#Shorts",
                "tags": ["Shorts"],
                "categoryId": "22",  # People & Blogs
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False,
            },
        }
        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            _, response = request.next_chunk()
        return response["id"]

    video_id = await loop.run_in_executor(None, _upload)
    return video_id


# ── Scheduler loop ────────────────────────────────────────────────────────────

async def scheduler_loop(bot):
    """Check every minute for videos due to be posted."""
    while True:
        try:
            with get_conn() as conn:
                rows = conn.execute("""
                    SELECT id, file_id, title, description
                    FROM scheduled_videos
                    WHERE posted = FALSE AND scheduled_at <= NOW()
                    ORDER BY scheduled_at ASC
                """).fetchall()

            for row_id, file_id, title, description in rows:
                logger.info(f"[scheduler] Processing video id={row_id} title={title}")
                try:
                    # Download video from Telegram
                    file = await bot.get_file(file_id)
                    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                        tmp_path = tmp.name
                    await file.download_to_drive(tmp_path)

                    # Upload to YouTube
                    yt_id = await upload_to_youtube(tmp_path, title, description)
                    os.unlink(tmp_path)

                    # Mark as posted
                    with get_conn() as conn:
                        conn.execute("""
                            UPDATE scheduled_videos
                            SET posted=TRUE, posted_at=NOW(), youtube_id=%s
                            WHERE id=%s
                        """, (yt_id, row_id))
                        conn.commit()

                    # Notify admin
                    await bot.send_message(
                        ADMIN_ID,
                        f"✅ ቪዲዮ ተለቀቀ!\n\n"
                        f"📹 {title}\n"
                        f"🔗 https://youtube.com/shorts/{yt_id}"
                    )
                    logger.info(f"[scheduler] ✅ Posted: https://youtube.com/shorts/{yt_id}")

                except Exception as e:
                    logger.error(f"[scheduler] ❌ Failed to post video id={row_id}: {e}")
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE scheduled_videos SET posted=TRUE, youtube_id=%s WHERE id=%s",
                            (f"ERROR: {e}", row_id)
                        )
                        conn.commit()
                    await bot.send_message(ADMIN_ID, f"❌ ቪዲዮ መለቀቅ አልተቻለም:\n{title}\n\nስህተት: {e}")

        except Exception as e:
            logger.error(f"[scheduler] DB error: {e}")

        await asyncio.sleep(60)  # check every minute


# ── Telegram handlers ─────────────────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ ይህ ቦት ለግል አጠቃቀም ብቻ ነው።")
        return
    await update.message.reply_text(
        "📹 YouTube Shorts Scheduler Bot\n\n"
        "ቪዲዮ ይላኩ → ርዕስ ያስፍሩ → ጊዜ ያቀናብሩ → ቦቱ ይለቅቃል!\n\n"
        "ለመጀመር ቪዲዮ ይላኩ 👇",
        reply_markup=ReplyKeyboardRemove()
    )


async def receive_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return ConversationHandler.END

    video = update.message.video or update.message.document
    if not video:
        await update.message.reply_text("ቪዲዮ ብቻ ይላኩ።")
        return WAIT_VIDEO

    ctx.user_data["file_id"] = video.file_id
    await update.message.reply_text(
        "✅ ቪዲዮ ተቀብሏል!\n\n📝 የቪዲዮውን ርዕስ ያስፍሩ:"
    )
    return WAIT_TITLE


async def receive_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["title"] = update.message.text.strip()

    keyboard = ReplyKeyboardMarkup(
        [["አሁን ይለቀቅ"]],
        resize_keyboard=True, one_time_keyboard=True
    )
    await update.message.reply_text(
        "🕐 መቼ ይለቀቅ?\n\n"
        "• **አሁን ይለቀቅ** ለመጫን ወዲያውኑ\n"
        "• ወይም ቀን እና ሰዓት ያስፍሩ:\n"
        "  ቅርጸት: `2026-11-15 20:00`",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    return WAIT_SCHEDULE


async def receive_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "አሁን ይለቀቅ":
        scheduled_at = datetime.now()
    else:
        try:
            scheduled_at = datetime.strptime(text, "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text(
                "❌ ቅርጸት ትክክል አይደለም። እንደዚህ ያስፍሩ: `2026-11-15 20:00`\nወይም **አሁን ይለቀቅ** ይምረጡ።",
                parse_mode="Markdown"
            )
            return WAIT_SCHEDULE

    file_id = ctx.user_data["file_id"]
    title   = ctx.user_data["title"]

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO scheduled_videos (file_id, title, scheduled_at) VALUES (%s, %s, %s)",
            (file_id, title, scheduled_at)
        )
        conn.commit()

    if text == "አሁን ይለቀቅ":
        time_str = "አሁን ወዲያውኑ"
    else:
        time_str = scheduled_at.strftime("%Y-%m-%d %H:%M")

    await update.message.reply_text(
        f"✅ ቀጠሮ ተይዟል!\n\n"
        f"📹 {title}\n"
        f"🕐 {time_str}\n\n"
        f"ቦቱ በተቀናበረው ሰዓት ወደ YouTube Shorts ይለቅቃል።",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ ተሰርዟል።", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def list_scheduled(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT title, scheduled_at, posted, youtube_id
            FROM scheduled_videos
            ORDER BY scheduled_at DESC LIMIT 20
        """).fetchall()
    if not rows:
        await update.message.reply_text("ምንም ቀጠሮ የለም።")
        return
    lines = ["📋 የቀጠሮ ዝርዝር:\n"]
    for title, sched, posted, yt_id in rows:
        status = "✅" if posted else "⏳"
        time_str = sched.strftime("%Y-%m-%d %H:%M")
        link = f" → youtu.be/shorts/{yt_id}" if posted and yt_id and not yt_id.startswith("ERROR") else ""
        lines.append(f"{status} {title} — {time_str}{link}")
    await update.message.reply_text("\n".join(lines))


# ── Web server ────────────────────────────────────────────────────────────────

async def health(request):
    return web.Response(text="OK - YouTube Shorts Scheduler Bot")


async def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    web_app = web.Application()
    web_app.router.add_get("/", health)
    web_app.router.add_get("/ping", health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"[web] Listening on port {port}")


# ── Self-ping ─────────────────────────────────────────────────────────────────

async def self_ping_loop():
    port = int(os.environ.get("PORT", 10000))
    await asyncio.sleep(30)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://0.0.0.0:{port}/ping") as resp:
                    logger.info(f"[self-ping] {resp.status}")
        except Exception as e:
            logger.warning(f"[self-ping] failed: {e}")
        await asyncio.sleep(240)


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    init_db()
    await start_web_server()

    tg_app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.VIDEO | filters.Document.VIDEO, receive_video)],
        states={
            WAIT_VIDEO:    [MessageHandler(filters.VIDEO | filters.Document.VIDEO, receive_video)],
            WAIT_TITLE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_title)],
            WAIT_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_schedule)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("list", list_scheduled))
    tg_app.add_handler(conv)

    async with tg_app:
        await tg_app.start()
        await tg_app.updater.start_polling(allowed_updates=["message"])
        asyncio.create_task(scheduler_loop(tg_app.bot))
        asyncio.create_task(self_ping_loop())
        await asyncio.Event().wait()
        await tg_app.updater.stop()
        await tg_app.stop()


if __name__ == "__main__":
    asyncio.run(run())
