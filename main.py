"""
Faah Fuck Ads Bot — Single-file deployment build.
All modules merged: config, states, database, session_mgr,
ad_engine, scheduler_engine, all handlers, and main entry point.
"""
import asyncio
import json
import io
import logging
import os
import sys
import warnings
from datetime import datetime, timedelta
from bson import ObjectId

warnings.filterwarnings("ignore", message=".*per_message=False.*", category=UserWarning)

# ════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    stream=sys.stdout,
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("FaahAdsBot")

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════
BOT_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
BOT_NAME    = "Faah Fuck Ads Bot"
BOT_VERSION = "2.0"
OWNER_ID    = 6864535438
MAX_ACCOUNTS = 500
API_ID      = int(os.environ.get("TELETHON_API_ID", "0"))
API_HASH    = os.environ.get("TELETHON_API_HASH", "")
MONGODB_URL = os.environ.get("MONGODB_URL", "")
DB_NAME     = "faah_fuck_ads_bot"

# ════════════════════════════════════════════════════════════════
# STATES
# ════════════════════════════════════════════════════════════════
(
    ACCT_MENU, ACCT_WAIT_PHONE, ACCT_WAIT_OTP,
    ACCT_WAIT_2FA, ACCT_SELECT_DELETE, ACCT_WAIT_COOLDOWN,
) = range(6)

ADS_WAIT_MESSAGE, ADS_WAIT_INTERVAL, ADS_WAIT_WAIT_TIME = range(10, 13)
SCHED_MENU, SCHED_WAIT_MESSAGE, SCHED_WAIT_INTERVAL, SCHED_SELECT_GROUPS = range(20, 24)
ADMIN_MENU, ADMIN_WAIT_ADD, ADMIN_WAIT_REMOVE = range(30, 33)
REPLY_WAIT_TRIGGER, REPLY_WAIT_MESSAGE = range(40, 42)
JOIN_WAIT_LINK   = 50
REPORT_WAIT_TARGET = 60

# ════════════════════════════════════════════════════════════════
# IMPORTS
# ════════════════════════════════════════════════════════════════
import motor.motor_asyncio
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters,
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeExpiredError,
    PhoneCodeInvalidError, FloodWaitError, AuthKeyUnregisteredError,
)

# ════════════════════════════════════════════════════════════════
# UTILS
# ════════════════════════════════════════════════════════════════
async def is_authorized(update: Update, db) -> bool:
    uid = update.effective_user.id
    if uid == OWNER_ID:
        return True
    user = await db.get_user(uid)
    return bool(user and user.get("is_admin"))

def is_owner(update: Update) -> bool:
    return update.effective_user.id == OWNER_ID

def header(title: str) -> str:
    line = "━" * 28
    return f"<code>{line}</code>\n⚡ <b>{title}</b>\n<code>{line}</code>"

def dot(value) -> str:
    return "🟢" if value else "🔴"

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Accounts",      callback_data="add_accounts"),
         InlineKeyboardButton("👥 My Accounts",       callback_data="my_accounts")],
        [InlineKeyboardButton("📝 Set Ad Message",    callback_data="set_ad_message"),
         InlineKeyboardButton("⏱ Set Time Interval", callback_data="set_interval")],
        [InlineKeyboardButton("▶️ Start Ads",         callback_data="start_ads"),
         InlineKeyboardButton("⏸ Stop Ads",          callback_data="stop_ads")],
        [InlineKeyboardButton("📅 Schedule",          callback_data="schedule_menu"),
         InlineKeyboardButton("🔁 Auto Reply",        callback_data="auto_reply")],
        [InlineKeyboardButton("🗑 Delete Accounts",   callback_data="delete_accounts"),
         InlineKeyboardButton("🔗 Join Link",         callback_data="join_link")],
        [InlineKeyboardButton("⚠️ Mass Report",       callback_data="mass_report"),
         InlineKeyboardButton("⏳ Acc Cooldown",      callback_data="acc_cooldown")],
        [InlineKeyboardButton("💾 Download DB",       callback_data="download_db"),
         InlineKeyboardButton("📋 Logs",              callback_data="logs")],
        [InlineKeyboardButton("🤝 Manage Access",     callback_data="manage_access"),
         InlineKeyboardButton("❌ Close",              callback_data="close")],
    ])

def back_keyboard(cb: str = "back_main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=cb)]])

def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Cancel", callback_data="back_main")]])

# ════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════
class Database:
    def __init__(self):
        self.client = None
        self.db = None

    async def connect(self, retries: int = 10, delay: int = 5):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            MONGODB_URL, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
        )
        self.db = self.client[DB_NAME]
        for attempt in range(1, retries + 1):
            try:
                await self.client.admin.command("ping")
                print(f"✅ MongoDB connected (attempt {attempt})")
                break
            except Exception as e:
                print(f"⚠️  MongoDB attempt {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    raise RuntimeError(f"❌ Cannot reach MongoDB after {retries} attempts.") from e
                await asyncio.sleep(delay)
        try:
            await self.db.accounts.create_index("owner_user_id")
            await self.db.accounts.create_index("phone", unique=True)
            await self.db.logs.create_index([("user_id", 1), ("timestamp", -1)])
            await self.db.schedules.create_index("user_id")
        except Exception as e:
            print(f"⚠️  Index creation skipped: {e}")

    async def get_user(self, user_id):
        return await self.db.users.find_one({"user_id": user_id})

    async def upsert_user(self, user_id, username=None):
        await self.db.users.update_one(
            {"user_id": user_id},
            {"$setOnInsert": {"user_id": user_id, "username": username,
                              "is_admin": False, "created_at": datetime.utcnow()}},
            upsert=True,
        )

    async def set_admin(self, user_id, is_admin):
        await self.db.users.update_one(
            {"user_id": user_id}, {"$set": {"is_admin": is_admin}}, upsert=True,
        )

    async def get_all_admins(self):
        return await self.db.users.find({"is_admin": True}).to_list(None)

    async def add_account(self, owner_user_id, phone, session_string, name, username=None):
        await self.db.accounts.update_one(
            {"phone": phone},
            {"$set": {"owner_user_id": owner_user_id, "session_string": session_string,
                      "status": "online", "name": name, "username": username,
                      "last_seen": datetime.utcnow(), "cooldown_until": None,
                      "updated_at": datetime.utcnow()},
             "$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
        )

    async def get_accounts(self, owner_user_id=None):
        q = {"owner_user_id": owner_user_id} if owner_user_id else {}
        return await self.db.accounts.find(q).to_list(None)

    async def get_account_count(self):
        return await self.db.accounts.count_documents({})

    async def get_online_count(self):
        return await self.db.accounts.count_documents({"status": "online"})

    async def get_dead_count(self):
        return await self.db.accounts.count_documents({"status": "dead"})

    async def delete_account(self, phone):
        await self.db.accounts.delete_one({"phone": phone})

    async def update_account_status(self, phone, status):
        await self.db.accounts.update_one(
            {"phone": phone}, {"$set": {"status": status, "last_seen": datetime.utcnow()}},
        )

    async def set_cooldown(self, phone, until):
        await self.db.accounts.update_one({"phone": phone}, {"$set": {"cooldown_until": until}})

    async def get_settings(self, user_id) -> dict:
        s = await self.db.settings.find_one({"user_id": user_id})
        if not s:
            s = {"user_id": user_id, "ad_message": None, "cycle_interval": 300,
                 "wait_time": 20, "is_active": False, "total_sent": 0, "total_failed": 0}
            await self.db.settings.insert_one(s)
        return s

    async def update_settings(self, user_id, updates):
        await self.db.settings.update_one({"user_id": user_id}, {"$set": updates}, upsert=True)

    async def inc_stats(self, user_id, sent=0, failed=0):
        await self.db.settings.update_one(
            {"user_id": user_id}, {"$inc": {"total_sent": sent, "total_failed": failed}}, upsert=True,
        )

    async def add_log(self, user_id, account_phone, group_name, status, error=None):
        await self.db.logs.insert_one({
            "user_id": user_id, "account_phone": account_phone, "group_name": group_name,
            "status": status, "error": error, "timestamp": datetime.utcnow(),
        })

    async def get_logs(self, user_id, limit=50):
        return await self.db.logs.find({"user_id": user_id}).sort("timestamp", -1).limit(limit).to_list(limit)

    async def clear_logs(self, user_id):
        await self.db.logs.delete_many({"user_id": user_id})

    async def add_schedule(self, user_id, message, groups, interval):
        r = await self.db.schedules.insert_one({
            "user_id": user_id, "message": message, "groups": groups,
            "interval": interval, "is_active": False,
            "last_run": None, "next_run": None, "created_at": datetime.utcnow(),
        })
        return str(r.inserted_id)

    async def get_schedules(self, user_id):
        return await self.db.schedules.find({"user_id": user_id}).to_list(None)

    async def update_schedule(self, schedule_id, updates):
        await self.db.schedules.update_one({"_id": ObjectId(schedule_id)}, {"$set": updates})

    async def delete_schedule(self, schedule_id):
        await self.db.schedules.delete_one({"_id": ObjectId(schedule_id)})

    async def get_active_schedules(self):
        return await self.db.schedules.find({"is_active": True}).to_list(None)

    async def get_auto_replies(self, user_id):
        return await self.db.auto_replies.find({"user_id": user_id}).to_list(None)

    async def set_auto_reply(self, user_id, trigger, reply):
        await self.db.auto_replies.update_one(
            {"user_id": user_id, "trigger": trigger},
            {"$set": {"reply": reply, "is_active": True}}, upsert=True,
        )

    async def export_all(self, user_id) -> dict:
        accounts  = await self.get_accounts(user_id)
        settings  = await self.get_settings(user_id)
        logs      = await self.get_logs(user_id, limit=500)
        schedules = await self.get_schedules(user_id)
        def clean(docs):
            for d in docs:
                d["_id"] = str(d["_id"])
                for k, v in d.items():
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()
            return docs
        settings.pop("_id", None)
        return {"accounts": clean(accounts), "settings": settings,
                "logs": clean(logs), "schedules": clean(schedules)}

# ════════════════════════════════════════════════════════════════
# SESSION MANAGER
# ════════════════════════════════════════════════════════════════
_pending: dict[str, TelegramClient] = {}

async def start_login(phone: str):
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    await client.send_code_request(phone)
    _pending[phone] = client
    return client

async def complete_login(phone: str, code: str):
    client = _pending.get(phone)
    if not client:
        raise ValueError("No pending login for this phone. Start over.")
    await client.sign_in(phone, code)
    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()
    _pending.pop(phone, None)
    name = f"{me.first_name or ''} {me.last_name or ''}".strip()
    return session_string, name, me.username

async def complete_2fa(phone: str, password: str):
    client = _pending.get(phone)
    if not client:
        raise ValueError("No pending login for this phone. Start over.")
    await client.sign_in(password=password)
    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()
    _pending.pop(phone, None)
    name = f"{me.first_name or ''} {me.last_name or ''}".strip()
    return session_string, name, me.username

def cancel_login(phone: str):
    client = _pending.pop(phone, None)
    if client:
        asyncio.create_task(client.disconnect())

async def _get_client(session_string: str) -> TelegramClient:
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    return client

async def report_user(session_string: str, target: str, reason_key: str):
    from telethon.tl.functions.account import ReportPeerRequest
    from telethon.tl.types import (
        InputReportReasonSpam, InputReportReasonViolence,
        InputReportReasonPornography, InputReportReasonOther,
        InputReportReasonCopyright, InputReportReasonIllegalDrugs,
        InputReportReasonIllegalWeapons, InputReportReasonPersonalDetails,
        InputReportReasonFake,
    )
    reason_map = {
        "spam": InputReportReasonSpam(), "violence": InputReportReasonViolence(),
        "porn": InputReportReasonPornography(), "copyright": InputReportReasonCopyright(),
        "drugs": InputReportReasonIllegalDrugs(), "illegal_weapons": InputReportReasonIllegalWeapons(),
        "personal_data": InputReportReasonPersonalDetails(), "fake": InputReportReasonFake(),
        "other": InputReportReasonOther(),
    }
    reason = reason_map.get(reason_key, InputReportReasonSpam())
    client = await _get_client(session_string)
    try:
        entity = await client.get_entity(target)
        await client(ReportPeerRequest(peer=entity, reason=reason, message=""))
    finally:
        await client.disconnect()

async def send_message_to_groups(session_string, message, wait_time, db, user_id, phone):
    sent = failed = 0
    try:
        client = await _get_client(session_string)
        try:
            async for dialog in client.iter_dialogs():
                if not (dialog.is_group or dialog.is_channel):
                    continue
                try:
                    msg_type = message.get("type", "text")
                    if msg_type == "text":
                        await client.send_message(dialog.entity, message["text"])
                    else:
                        await client.send_file(
                            dialog.entity, message["file_id"],
                            caption=message.get("caption", ""),
                        )
                    sent += 1
                    await db.add_log(user_id, phone, dialog.name, "sent")
                    await asyncio.sleep(wait_time)
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 5)
                    failed += 1
                    await db.add_log(user_id, phone, dialog.name, "failed", f"FloodWait {e.seconds}s")
                except Exception as e:
                    failed += 1
                    await db.add_log(user_id, phone, dialog.name, "failed", str(e)[:100])
        finally:
            await client.disconnect()
    except AuthKeyUnregisteredError:
        await db.update_account_status(phone, "dead")
    except Exception as e:
        await db.add_log(user_id, phone, "—", "failed", str(e)[:100])
        failed += 1
    return sent, failed

# ════════════════════════════════════════════════════════════════
# AD ENGINE
# ════════════════════════════════════════════════════════════════
_ad_tasks: dict[int, asyncio.Task] = {}

def ad_is_running(user_id: int) -> bool:
    t = _ad_tasks.get(user_id)
    return t is not None and not t.done()

async def _ad_run_loop(user_id, db, bot):
    try:
        while True:
            settings = await db.get_settings(user_id)
            if not settings.get("is_active"):
                break
            ad_message = settings.get("ad_message")
            if not ad_message:
                await asyncio.sleep(10)
                continue
            cycle_interval = settings.get("cycle_interval", 300)
            wait_time = settings.get("wait_time", 20)
            accounts = await db.get_accounts(user_id)
            if not accounts:
                await asyncio.sleep(30)
                continue
            total_sent = total_failed = 0
            for acc in accounts:
                if acc.get("status") == "dead":
                    continue
                cd = acc.get("cooldown_until")
                if cd and datetime.utcnow() < cd:
                    continue
                s, f = await send_message_to_groups(
                    acc["session_string"], ad_message, wait_time, db, user_id, acc["phone"]
                )
                total_sent += s
                total_failed += f
            await db.inc_stats(user_id, sent=total_sent, failed=total_failed)
            await asyncio.sleep(cycle_interval)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[AdEngine] Error user {user_id}: {e}")
    finally:
        await db.update_settings(user_id, {"is_active": False})

async def ad_start(user_id, db, bot):
    if ad_is_running(user_id):
        return
    await db.update_settings(user_id, {"is_active": True})
    _ad_tasks[user_id] = asyncio.create_task(_ad_run_loop(user_id, db, bot))

async def ad_stop(user_id, db):
    await db.update_settings(user_id, {"is_active": False})
    task = _ad_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

async def ad_restore(db, bot):
    try:
        active = await db.db.settings.find({"is_active": True}).to_list(None)
        for s in active:
            uid = s["user_id"]
            if not ad_is_running(uid):
                await ad_start(uid, db, bot)
    except Exception as e:
        print(f"[AdEngine] Restore error: {e}")

# ════════════════════════════════════════════════════════════════
# SCHEDULER ENGINE
# ════════════════════════════════════════════════════════════════
_sched_tasks: dict[str, asyncio.Task] = {}

def sched_is_running(sched_id: str) -> bool:
    t = _sched_tasks.get(sched_id)
    return t is not None and not t.done()

async def _sched_run_loop(sched_id, user_id, db):
    try:
        while True:
            schedule = await db.db.schedules.find_one({"_id": ObjectId(sched_id)})
            if not schedule or not schedule.get("is_active"):
                break
            
            accounts = await db.get_accounts(user_id)
            message = schedule["message"]
            interval = schedule.get("interval", 300)
            settings = await db.get_settings(user_id)
            wait_time = settings.get("wait_time", 20)
            
            for acc in accounts:
                if acc.get("status") == "dead":
                    continue
                cd = acc.get("cooldown_until")
                if cd and datetime.utcnow() < cd:
                    continue
                
                # Execute the scheduled ad
                s, f = await send_message_to_groups(
                    acc["session_string"], message, wait_time, db, user_id, acc["phone"]
                )
                
            # Sleep until the next interval
            await asyncio.sleep(interval)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[SchedEngine] Error user {user_id}: {e}")

# ════════════════════════════════════════════════════════════════
# KEEP-ALIVE SERVER (The "Jaadu")
# ════════════════════════════════════════════════════════════════
from flask import Flask
from threading import Thread

app = Flask("")

@app.route('/')
def home():
    return "Faah Ads Bot is alive and running 24/7!"

def run():
    # Render assigns a port dynamically via the PORT env var. 
    # Fallback to 8080 if not running on Render.
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()

# ════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════
async def main():
    # 1. Initialize Database
    db = Database()
    await db.connect()
    
    # Ensure owner is set as admin
    await db.set_admin(OWNER_ID, True)
    
    # 2. Build the Bot Application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # (If you have specific command handlers, ensure they are registered here)
    # e.g., application.add_handler(CommandHandler("start", start_handler))

    # 3. Restore any active background tasks
    await ad_restore(db, application.bot)

    log.info(f"✅ {BOT_NAME} v{BOT_VERSION} is starting...")
    
    # 4. Start polling
    await application.initialize()
    await application.start()
    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    # Keep the application running
    stop_signal = asyncio.Event()
    await stop_signal.wait()

if __name__ == "__main__":
    # 1. Start the Flask web server in a background thread FIRST
    print("🪄 Starting the Keep-Alive server...")
    keep_alive()
    
    # 2. Start the Telegram bot
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
