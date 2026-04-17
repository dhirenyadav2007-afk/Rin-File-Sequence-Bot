# -*- coding: utf-8 -*-

import logging
import asyncio
import os
import re
from datetime import timedelta, datetime
from threading import Thread
from flask import Flask

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue
)
from telegram.error import RetryAfter
from telegram import constants
from pymongo import MongoClient

#=========== configurations ===========

BOT_TOKEN = os.getenv("BOT_TOKEN", "8704095477:AAFBwS3K13kyLX6mObkJfUFOqNew-mH0vZg")
PHOTO_MAIN = "https://i.ibb.co/21TbBYYb/download.jpg"
PHOTO_HELP = "https://i.ibb.co/LXyXRVp6/656002.jpg"
RESTART_PHOTO_ID = "https://i.ibb.co/gbXCJDZy/download-73.jpg"
PHOTO_STATUS = "https://i.ibb.co/kgpzHjHb/download-71.jpg"
OWNER_ID = int(os.getenv("OWNER_ID", "7156099919"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://ANI_OTAKU:ANI_OTAKU@cluster0.t3frstc.mongodb.net/?appName=Cluster0")
DB_NAME = os.getenv("DB_NAME", "RIN_FILE_SEQUENCE_BOT")

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

settings_col = db["settings"]
settings_col = db["settings"]
caps_col = db["captions"]   # {_id: user_id, caption_text}
users_col = db["users"]     # {_id: user_id, first_name, username, joined_at}
stats_col = db["stats"]     # {_id:"bot", total_sorted_files, last_restarted}
dump_col = db["dump"]       # {_id: user_id, channel_id, channel_link}
stickers_col = db["stickers"]        # {_id: user_id, sticker_id: str}
leaderboard_col = db["leaderboard"]  # per-user counters
modes_col = db["modes"]              # {_id: user_id, mode: "episode"}
smodes_col = db["smodes"]            # {_id: user_id, mode: "default"|"quality"}
# ================ LOGGING ================
logging.basicConfig(level=logging.INFO)

# ================== IN-MEMORY QUEUE ==================
# Per-user queue: { user_id: [ {"chat_id": int, "message_id": int, "meta": str} , ... ] }
USER_QUEUE = {}
BOT_START_TIME = datetime.now()
SETDUMP_WAIT = set()

# ---------- FLASK ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!", 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))

# ---------- HELPERS ----------

def fmt_timedelta(seconds: float) -> str:
    # 0:00:03 style
    return str(timedelta(seconds=int(max(0, seconds))))

def extract_episode_number(text: str) -> int | None:
    """
    Try to extract an episode number from filename/caption/text.

    Handles patterns like:
    - "E01", "EP01", "EP 01", "Episode 01"
    - standalone 2-digit/3-digit tokens "01", "12", "104" (fallback)
    """
    if not text:
        return None

    t = text.lower()

    # Strong patterns first
    patterns = [
        r"(?:episode|ep|e)\s*[-:#]?\s*(\d{1,4})",   # ep 12, episode-12, e12
    ]
    for p in patterns:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except:
                pass

    # Common anime release: " - 01 " or "[01]" or "(01)"
    m = re.search(r"[\[\(\s\-_\.](\d{1,4})[\]\)\s\-_\.]", t)
    if m:
        try:
            return int(m.group(1))
        except:
            pass

    # Last resort: any standalone 1-4 digit number (avoid years like 1080 / 720)
    # Prefer 1-3 digits and not 360/480/720/1080
    nums = re.findall(r"\b(\d{1,4})\b", t)
    for n in nums:
        try:
            x = int(n)
            if x in (360, 480, 720, 1080):
                continue
            # ignore obvious years
            if 1900 <= x <= 2100:
                continue
            return x
        except:
            continue

    return None

def get_message_meta(msg) -> str:
    # Collect text for episode detection from file_name/caption/text
    parts = []
    if getattr(msg, "caption", None):
        parts.append(msg.caption)
    if getattr(msg, "text", None):
        parts.append(msg.text)

    # document/video/audio file name
    if msg.document and msg.document.file_name:
        parts.append(msg.document.file_name)
    if msg.video and msg.video.file_name:
        parts.append(msg.video.file_name)
    if msg.audio and msg.audio.file_name:
        parts.append(msg.audio.file_name)

    return " ".join([p for p in parts if p])

def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

def save_user_to_db(user):
    users_col.update_one(
        {"_id": user.id},
        {"$set": {
            "_id": user.id,
            "first_name": user.first_name or "",
            "username": user.username or "",
            "joined_at": datetime.utcnow()
        }},
        upsert=True
    )

def get_total_users() -> int:
    return users_col.count_documents({})

def get_stats_doc():
    doc = stats_col.find_one({"_id": "bot"})
    if not doc:
        doc = {
            "_id": "bot",
            "total_sorted_files": 0,
            "last_restarted": datetime.utcnow()
        }
        stats_col.insert_one(doc)
    return doc

def set_user_dump(uid: int, dump_id: int):
    dump_col.update_one(
        {"_id": uid},
        {"$set": {"dump_id": int(dump_id)}},
        upsert=True
    )

def get_user_dump(uid: int) -> int | None:
    doc = dump_col.find_one({"_id": uid})
    return int(doc["dump_id"]) if doc and "dump_id" in doc else None

def set_user_caption(uid: int, template: str):
    caps_col.update_one(
        {"_id": uid},
        {"$set": {"template": template}},
        upsert=True
    )

def get_user_caption(uid: int) -> str | None:
    doc = caps_col.find_one({"_id": uid})
    return doc.get("template") if doc else None

def extract_quality(text: str) -> str | None:
    if not text:
        return None
    t = text.lower()
    for q in ("360p", "480p", "720p", "1080p"):
        if q in t:
            return q
    return None

def extract_filename_from_meta(meta: str) -> str:
    # best-effort: return first long-ish token or full meta
    return (meta or "").strip()[:200] or "File"

def build_caption(template: str | None, meta: str, is_document: bool=False) -> str | None:
    if not template:
        return None

    ep = extract_episode_number(meta)
    q = extract_quality(meta)
    fname = extract_filename_from_meta(meta)

    out = template
    # 🔥 remove filename for documents only
    if is_document:
        out = out.replace("{file_name}", "")
    else:
        out = out.replace("{file_name}", fname)
    out = out.replace("{episode}", str(ep) if ep is not None else "")
    out = out.replace("{quality}", q or "")

    # ✅ Convert \n to real Telegram line break
    out = out.replace("\\n", "\n")

    out = out.strip()

    if not out:
        return None

    return out

def set_user_sticker(uid: int, sticker_id: str):
    stickers_col.update_one(
        {"_id": uid},
        {"$set": {"sticker_id": sticker_id}},
        upsert=True
    )

def get_user_sticker(uid: int) -> str | None:
    doc = stickers_col.find_one({"_id": uid})
    return doc.get("sticker_id") if doc else None

def _today_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def _week_key(dt: datetime) -> str:
    iso = dt.isocalendar()  # (year, week, weekday)
    return f"{iso.year}-W{iso.week:02d}"

def mention_clickable(user_id: int, first_name: str, username: str) -> str:
    # clickable even without username
    name = (first_name or "User").replace("<", "").replace(">", "")
    if username:
        # keep it clickable to profile via @ too, but tg://user is always safe
        return f"<a href='tg://user?id={user_id}'>{name}</a>"
    return f"<a href='tg://user?id={user_id}'>{name}</a>"

def leaderboard_keyboard(active: str) -> InlineKeyboardMarkup:
    # active: "today"|"week"|"month"|"all"
    def btn(text, key):
        label = f"• {text} •" if key == active else text
        return InlineKeyboardButton(label, callback_data=f"lb:{key}")

    return InlineKeyboardMarkup([
        [btn("Today", "today"), btn("Weekly", "week")],
        [btn("Monthly", "month"), btn("All Time", "all")],
        [InlineKeyboardButton("✗ ƈʅσʂҽ ✗", callback_data="close_msg")]
    ])

def leaderboard_title(active: str) -> str:
    return {
        "today": "📈 LEADERBOARD: TODAY",
        "week":  "📈 LEADERBOARD: WEEKLY",
        "month": "📈 LEADERBOARD: MONTHLY",
        "all":   "📈 LEADERBOARD: ALL TIME"
    }.get(active, "📈 LEADERBOARD: TODAY")

def build_leaderboard_text(active: str, rows: list[dict], total_sorted: int) -> str:
    # rows: list of docs from Mongo, already sorted descending
    lines = []
    lines.append(f"<b>{leaderboard_title(active)}</b>\n")
    lines.append("<b>Top 20 Users With Most Files Sorted:</b>\n")

    if not rows:
        lines.append("<blockquote>No data yet.</blockquote>\n")
    else:
        for i, doc in enumerate(rows, start=1):
            uid = doc["_id"]
            fn = doc.get("first_name", "User")
            un = doc.get("username", "")
            name = mention_clickable(uid, fn, un)

            if active == "today":
                c = int((doc.get("today") or {}).get("count", 0))
            elif active == "week":
                c = int((doc.get("week") or {}).get("count", 0))
            elif active == "month":
                c = int((doc.get("month") or {}).get("count", 0))
            else:
                c = int(doc.get("all_time", 0))

            # style like your screenshot: « Name » 123
            lines.append(f"👤 « {name} » <b>{c}</b>")

    lines.append(f"\n<b>Total Sorted Files:</b> <code>{int(total_sorted)}</code>")
    return "\n".join(lines)

def get_leaderboard_rows(active: str, now: datetime, limit: int = 20) -> list[dict]:
    if active == "today":
        key = _today_key(now)
        return list(leaderboard_col.find(
            {"today.date": key, "today.count": {"$gt": 0}},
            {"first_name": 1, "username": 1, "today": 1}
        ).sort("today.count", -1).limit(limit))

    if active == "week":
        key = _week_key(now)
        return list(leaderboard_col.find(
            {"week.key": key, "week.count": {"$gt": 0}},
            {"first_name": 1, "username": 1, "week": 1}
        ).sort("week.count", -1).limit(limit))

    if active == "month":
        key = _month_key(now)
        return list(leaderboard_col.find(
            {"month.key": key, "month.count": {"$gt": 0}},
            {"first_name": 1, "username": 1, "month": 1}
        ).sort("month.count", -1).limit(limit))

    # all time
    return list(leaderboard_col.find(
        {"all_time": {"$gt": 0}},
        {"first_name": 1, "username": 1, "all_time": 1}
    ).sort("all_time", -1).limit(limit))

def get_leaderboard_total(active: str, now: datetime) -> int:
    pipeline = []
    if active == "today":
        key = _today_key(now)
        pipeline = [
            {"$match": {"today.date": key}},
            {"$group": {"_id": None, "sum": {"$sum": "$today.count"}}}
        ]
    elif active == "week":
        key = _week_key(now)
        pipeline = [
            {"$match": {"week.key": key}},
            {"$group": {"_id": None, "sum": {"$sum": "$week.count"}}}
        ]
    elif active == "month":
        key = _month_key(now)
        pipeline = [
            {"$match": {"month.key": key}},
            {"$group": {"_id": None, "sum": {"$sum": "$month.count"}}}
        ]
    else:
        pipeline = [
            {"$group": {"_id": None, "sum": {"$sum": "$all_time"}}}
        ]

    out = list(leaderboard_col.aggregate(pipeline))
    return int(out[0]["sum"]) if out else 0

def update_leaderboard_counters(user, added_count: int):
    if added_count <= 0:
        return

    now = datetime.utcnow()
    td = _today_key(now)
    wk = _week_key(now)
    mo = _month_key(now)

    # Reset period keys automatically if changed
    doc = leaderboard_col.find_one({"_id": user.id}) or {}

    today_doc = doc.get("today") or {}
    week_doc = doc.get("week") or {}
    month_doc = doc.get("month") or {}

    today_count = int(today_doc.get("count", 0)) if today_doc.get("date") == td else 0
    week_count = int(week_doc.get("count", 0)) if week_doc.get("key") == wk else 0
    month_count = int(month_doc.get("count", 0)) if month_doc.get("key") == mo else 0

    leaderboard_col.update_one(
        {"_id": user.id},
        {"$set": {
            "first_name": user.first_name or "",
            "username": user.username or "",
            "today": {"date": td, "count": today_count + added_count},
            "week":  {"key": wk, "count": week_count + added_count},
            "month": {"key": mo, "count": month_count + added_count},
        },
         "$inc": {"all_time": int(added_count)}
        },
        upsert=True
    )

def get_user_mode(uid: int) -> str | None:
    d = modes_col.find_one({"_id": uid})
    return d.get("mode") if d else None

def set_user_mode(uid: int, mode: str):
    modes_col.update_one({"_id": uid}, {"$set": {"mode": mode}}, upsert=True)

def extract_season_number(text: str) -> int | None:
    """
    Extract season number from filename/caption/text.

    Handles patterns like:
    - S01, S1, S 01
    - Season 1, Season-02
    - 1st Season / 2nd Season (basic)
    """
    if not text:
        return None

    t = text.lower()

    # Strong patterns
    patterns = [
        r"\bseason\s*[-:#]?\s*(\d{1,2})\b",   # season 1, season-02
        r"\bs\s*[-:#]?\s*(\d{1,2})\b",       # s1, s 01, s-02
        r"\b(\d{1,2})(?:st|nd|rd|th)\s+season\b",  # 1st season
    ]

    for p in patterns:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except:
                pass

    return None


def extract_title(meta: str) -> str:
    """
    Best-effort title extraction for sorting by title.

    Tries to:
    - remove bracket groups: [ ... ], ( ... )
    - remove common tokens: quality, episode markers, season markers
    - remove extra separators
    Returns a clean-ish title string.
    """
    if not meta:
        return "unknown"

    t = meta

    # remove bracket groups (often contain fansub/codec info)
    t = re.sub(r"\[[^\]]*\]", " ", t)
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"\{[^}]*\}", " ", t)

    # remove quality tokens
    t = re.sub(r"\b(360p|480p|720p|1080p|2160p|4k)\b", " ", t, flags=re.IGNORECASE)

    # remove episode markers like ep 01, e01, episode 01
    t = re.sub(r"\b(?:episode|ep|e)\s*[-:#]?\s*\d{1,4}\b", " ", t, flags=re.IGNORECASE)

    # remove season markers like s01, season 1
    t = re.sub(r"\bseason\s*[-:#]?\s*\d{1,2}\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\bs\s*[-:#]?\s*\d{1,2}\b", " ", t, flags=re.IGNORECASE)

    # remove common codec/source tokens (optional but helps)
    t = re.sub(r"\b(x264|x265|h\.?264|h\.?265|hevc|avc|hdrip|webrip|web\-dl|bluray|bdrip)\b",
               " ", t, flags=re.IGNORECASE)

    # normalize separators to spaces
    t = t.replace("_", " ").replace(".", " ").replace("-", " ")

    # collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # keep it reasonable
    return t[:120] if t else "unknown"

def get_user_smode(uid: int) -> str:
    doc = smodes_col.find_one({"_id": uid})
    m = (doc.get("mode") if doc else None) or "default"
    return m if m in ("default", "quality") else "default"

def set_user_smode(uid: int, mode: str):
    if mode not in ("default", "quality"):
        mode = "default"
    smodes_col.update_one({"_id": uid}, {"$set": {"mode": mode}}, upsert=True)

# ---------- KEYBOARDS ----------
def start_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("ԋҽʅρ", callback_data="help_text"),
                InlineKeyboardButton("Dҽʋҽʅσρҽɾ", url="https://t.me/ITSANIMEN")
            ],
            [
                InlineKeyboardButton("Cԋαɳɳҽʅ", url="https://t.me/BotifyX_Pro_Botz")
            ]
        ]
    )

def help_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✗ Ⴆαƈƙ ✗", callback_data="back_to_start"),
                InlineKeyboardButton("✗ ƈʅσʂҽ ✗", callback_data="close_msg")
            ]
        ]
    )

# ✅ add keyboard
def status_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("✗ ƈʅσʂҽ ✗", callback_data="close_msg")]])

def smode_keyboard(cur: str) -> InlineKeyboardMarkup:
    def label(name: str, key: str) -> str:
        return f"✓ {name}" if key == cur else name

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(label("Quality", "quality"), callback_data="smode:quality"),
            InlineKeyboardButton(label("Default", "default"), callback_data="smode:default"),
        ],
        [InlineKeyboardButton("✗ ƈʅσʂҽ ✗", callback_data="close_msg")]
    ])

# ---------- START ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user_to_db(update.effective_user)

    await update.message.reply_photo(
        photo=PHOTO_MAIN,
        caption=(
            "<blockquote>Wᴇʟᴄᴏᴍᴇ ᴛᴏ ʏᴏᴜʀ ᴀʟʟ-ɪɴ-ᴏɴᴇ Fɪʟᴇ Mᴀɴᴀɢᴇᴍᴇɴᴛ Assɪsᴛᴀɴᴛ! 📂✨</blockquote>\n\n"
            "<blockquote>Eᴀsɪʟʏ ᴍᴀɴᴀɢᴇ, ᴏʀɢᴀɴɪᴢᴇ, ᴀɴᴅ sʜᴀʀᴇ ʏᴏᴜʀ ꜰɪʟᴇs ᴡɪᴛʜᴏᴜᴛ ᴀɴʏ \n"
            "ʜᴀssʟᴇ. Sᴀʏ ɢᴏᴏᴅʙʏᴇ ᴛᴏ ᴍᴇssʏ ꜰɪʟᴇ ɴᴀᴍᴇs ᴀɴᴅ ᴄᴏɴꜰᴜsɪɴɢ \n"
            "ʀᴇsᴏʟᴜᴛɪᴏɴs – ᴡᴇ’ᴠᴇ ɢᴏᴛ ʏᴏᴜ ᴄᴏᴠᴇʀᴇᴅ!</blockquote>\n\n"
            "<blockquote><b>➥ MAINTAINED BY : "
            "<a href='https://t.me/ITSANIMEN'>彡 ΔNI_OTΔKU 彡</a>"
            "</b></blockquote>"
        ),
        reply_markup=start_keyboard(),
        parse_mode=constants.ParseMode.HTML
    )

# ---------- SORT ----------
async def sort_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id

    data = USER_QUEUE.get(uid, {"files": [], "queue_msgs": []})
    files = data.get("files", [])
    queue_msgs = data.get("queue_msgs", [])

    total = len(files)
    if total == 0:
        await update.message.reply_text(
            "<b>Your queue is empty. Please add files first.</b>",
            parse_mode=constants.ParseMode.HTML
        )
        return

    start_time = datetime.now()
    mode = get_user_mode(uid) or "episode"

    # ✅ sticker display mode (default/quality)
    smode = get_user_smode(uid) if "get_user_smode" in globals() else "default"

    # Build sortable list (mode-based)
    sortable = []
    for item in files:
        meta = item.get("meta", "")

        ep = extract_episode_number(meta)
        ep_key = ep if ep is not None else 10**9

        q = extract_quality(meta)
        q_map = {"360p": 360, "480p": 480, "720p": 720, "1080p": 1080}
        q_key = q_map.get((q or "").lower(), 10**9)

        title = extract_title(meta) if "extract_title" in globals() else meta
        title_key = (title or "").lower()

        season = extract_season_number(meta) if "extract_season_number" in globals() else None
        season_key = season if season is not None else 10**9

        if mode == "quality":
            key = (q_key, ep_key)
        elif mode == "title":
            key = (title_key, ep_key)
        elif mode == "both":
            key = (title_key, q_key, ep_key)
        elif mode == "season":
            key = (season_key, q_key, ep_key)
        else:
            key = (ep_key,)

        sortable.append((key, item))

    sortable.sort(key=lambda x: x[0])
    sorted_items = [x[1] for x in sortable]

    dump_id = get_user_dump(uid)
    template = get_user_caption(uid)  # ✅ user caption template (Mongo)
    sticker_id = get_user_sticker(uid)  # ✅ user sticker id (Mongo)

    # helper for quality-group sticker sending
    def _q_group(meta: str) -> str:
        q = extract_quality(meta) or ""
        return q.lower()

    # ✅ quality stickers active only when smode=quality AND sorting uses quality in key
    can_quality_sticker = (smode == "quality") and (mode in ("quality", "both", "season"))

    # choose where stickers go:
    # - if dump set: stickers go to dump channel (NOT user chat)
    # - else: stickers go to user chat
    sticker_chat_id = dump_id if dump_id else chat_id

    # ✅ If dump is set -> send to dump channel
    if dump_id:
        sending_msg = await update.message.reply_text(
            "sending files to dump...",
            parse_mode=constants.ParseMode.HTML
        )

        sent_count = 0
        last_group = None

        for it in sorted_items:
            is_doc = ".pdf" in (it.get("meta","").lower())
            cap = build_caption(template, it.get("meta", ""), is_doc)  # ✅ build caption per file

            cur_group = _q_group(it.get("meta", "")) if can_quality_sticker else None

            # ✅ if group changes, send sticker for the previous quality group (to dump)
            if can_quality_sticker and sticker_id:
                if last_group is None:
                    last_group = cur_group
                elif cur_group != last_group:
                    try:
                        await context.bot.send_sticker(chat_id=sticker_chat_id, sticker=sticker_id)
                    except:
                        pass
                    last_group = cur_group

            try:
                if cap:
                    await context.bot.copy_message(
                        chat_id=dump_id,
                        from_chat_id=it["chat_id"],
                        message_id=it["message_id"],
                        caption=cap,
                        parse_mode=constants.ParseMode.HTML
                    )
                else:
                    await context.bot.copy_message(
                        chat_id=dump_id,
                        from_chat_id=it["chat_id"],
                        message_id=it["message_id"]
                    )
                sent_count += 1

            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    if cap:
                        await context.bot.copy_message(
                            chat_id=dump_id,
                            from_chat_id=it["chat_id"],
                            message_id=it["message_id"],
                            caption=cap,
                            parse_mode=constants.ParseMode.HTML
                        )
                    else:
                        await context.bot.copy_message(
                            chat_id=dump_id,
                            from_chat_id=it["chat_id"],
                            message_id=it["message_id"]
                        )
                    sent_count += 1
                except:
                    pass
            except:
                pass

        # ✅ send sticker for the last quality group (to dump)
        if can_quality_sticker and sticker_id and total > 0:
            try:
                await context.bot.send_sticker(chat_id=sticker_chat_id, sticker=sticker_id)
            except:
                pass

        try:
            update_leaderboard_counters(update.effective_user, sent_count)
        except:
            pass

        # delete "sending..." message
        try:
            await sending_msg.delete()
        except:
            pass

        # ✅ if smode is NOT quality, send 1 sticker at end (to dump)
        if sticker_id and smode != "quality":
            try:
                await context.bot.send_sticker(chat_id=dump_id, sticker=sticker_id)
            except:
                pass

        await update.message.reply_text(
            "Fɪʟᴇꜱ Sᴏʀᴛᴇᴅ 🎉",
            parse_mode=constants.ParseMode.HTML
        )

    # ✅ If dump NOT set -> send to user chat
    else:
        sent_count = 0
        last_group = None

        for it in sorted_items:
            is_doc = ".pdf" in (it.get("meta","").lower())
            cap = build_caption(template, it.get("meta", ""), is_doc)  # ✅ build caption per file

            cur_group = _q_group(it.get("meta", "")) if can_quality_sticker else None

            # ✅ if group changes, send sticker for the previous quality group (to user chat)
            if can_quality_sticker and sticker_id:
                if last_group is None:
                    last_group = cur_group
                elif cur_group != last_group:
                    try:
                        await context.bot.send_sticker(chat_id=sticker_chat_id, sticker=sticker_id)
                    except:
                        pass
                    last_group = cur_group

            try:
                if cap:
                    await context.bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=it["chat_id"],
                        message_id=it["message_id"],
                        caption=cap,
                        parse_mode=constants.ParseMode.HTML
                    )
                else:
                    await context.bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=it["chat_id"],
                        message_id=it["message_id"]
                    )
                sent_count += 1    

            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    if cap:
                        await context.bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=it["chat_id"],
                            message_id=it["message_id"],
                            caption=cap,
                            parse_mode=constants.ParseMode.HTML
                        )
                    else:
                        await context.bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=it["chat_id"],
                            message_id=it["message_id"]
                        )
                    sent_count += 1    
                except:
                    pass
            except:
                pass

        # ✅ send sticker for the last quality group (to user chat)
        if can_quality_sticker and sticker_id and total > 0:
            try:
                await context.bot.send_sticker(chat_id=sticker_chat_id, sticker=sticker_id)
            except:
                pass

        try:
            update_leaderboard_counters(update.effective_user, sent_count)
        except:
            pass

        # ✅ if smode is NOT quality, send 1 sticker at end (to user chat)
        if sticker_id and smode != "quality":
            try:
                await context.bot.send_sticker(chat_id=chat_id, sticker=sticker_id)
            except:
                pass

        time_taken = (datetime.now() - start_time).total_seconds()

        await update.message.reply_text(
            f"Fɪʟᴇꜱ Sᴏʀᴛᴇᴅ {sent_count}/{total}\n"
            f"Mᴏᴅᴇ: {mode}\n"
            f"Tɪᴍᴇ Tᴀᴋᴇɴ: {fmt_timedelta(time_taken)}",
            parse_mode=constants.ParseMode.HTML
        )

    # ✅ Delete old unsorted USER file messages
    for it in files:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=it["message_id"])
        except:
            pass

    # ✅ Delete the bot "X File Added In Queue" messages
    for mid in queue_msgs:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass

    # ✅ Increment total sorted files in Mongo (lifetime)
    try:
        stats_col.update_one(
            {"_id": "bot"},
            {"$inc": {"total_sorted_files": int(sent_count)}},
            upsert=True
        )
    except:
        pass

    # keep queue clean: keep sorted, clear queue messages list
    USER_QUEUE[uid] = {"files": [], "queue_msgs": []}

# ---------- CLEAR ----------
async def clear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id

    data = USER_QUEUE.get(uid)

    # no user data or no files
    if not data or (not data.get("files") and not data.get("queue_msgs")):
        await update.message.reply_text(
            "Yᴏᴜʀ ꜰɪʟᴇ qᴜᴇᴜᴇ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴇᴍᴘᴛʏ.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    files = data.get("files", [])
    queue_msgs = data.get("queue_msgs", [])

    # ✅ delete USER sent file messages (best effort)
    for it in files:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=it["message_id"])
        except:
            pass

    # ✅ delete bot "X File Added In Queue" messages (best effort)
    for mid in queue_msgs:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except:
            pass

    # ✅ clear everything
    USER_QUEUE[uid] = {"files": [], "queue_msgs": []}

    await update.message.reply_text(
        "Yᴏᴜʀ ꜰɪʟᴇ qᴜᴇᴜᴇ ʜᴀꜱ ʙᴇᴇɴ ᴄʟᴇᴀʀᴇᴅ.",
        parse_mode=constants.ParseMode.HTML
    )


# ---------- STATUS (OWNER ONLY) ----------
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return

    uptime_seconds = (datetime.now() - BOT_START_TIME).total_seconds()

    # Mongo stats
    doc = stats_col.find_one({"_id": "bot"}) or {}
    total_sorted = int(doc.get("total_sorted_files", 0))
    last_restarted = doc.get("last_restarted")

    if isinstance(last_restarted, datetime):
        last_restarted_str = last_restarted.strftime("%d-%m-%Y %H:%M:%S")
    else:
        last_restarted_str = str(last_restarted) if last_restarted else "N/A"

    total_users = users_col.count_documents({})

    caption = (
        "<b>🤖 BOT STATUS</b>\n\n"
        f"⏱ <b>Uptime:</b> <code>{fmt_timedelta(uptime_seconds)}</code>\n"
        f"♻️ <b>Last Restarted:</b> <code>{last_restarted_str} UTC</code>\n"
        f"👥 <b>Total Users:</b> <code>{total_users}</code>\n"
        f"📂 <b>Total Files Sorted:</b> <code>{total_sorted}</code>\n"
    )

    await update.message.reply_photo(
        photo=PHOTO_STATUS,
        caption=caption,
        reply_markup=status_keyboard(),
        parse_mode=constants.ParseMode.HTML
    )

# ---------- SETDUMP ----------
async def setdump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # If parameter given: /setdump -100...
    if context.args and len(context.args) >= 1:
        try:
            dump_id = int(context.args[0])
            set_user_dump(uid, dump_id)
            await update.message.reply_text(
                f"Dᴜᴍᴘ Cʜᴀɴɴᴇʟ Aᴅᴅᴇᴅ: <code>{dump_id}</code>\n\n"
                "Yᴏᴜʀ Fɪʟᴇs Wɪʟʟ Nᴏᴡ Bᴇ Sᴇɴᴛ Tᴏ Tʜᴇ Sᴇʟᴇᴄᴛᴇᴅ Cʜᴀɴɴᴇʟ.",
                parse_mode=constants.ParseMode.HTML
            )
            return
        except:
            pass

    # If reply to forwarded message from channel
    msg = update.message
    if msg and msg.reply_to_message:
        r = msg.reply_to_message

        ch_id = None
        if r.forward_origin and r.forward_origin.chat:
            ch_id = r.forward_origin.chat.id
        elif r.forward_from_chat:
            ch_id = r.forward_from_chat.id

        if ch_id:
            set_user_dump(uid, int(ch_id))
            await update.message.reply_text(
                f"Dᴜᴍᴘ Cʜᴀɴɴᴇʟ Aᴅᴅᴇᴅ: <code>{ch_id}</code>\n\n"
                "Yᴏᴜʀ Fɪʟᴇs Wɪʟʟ Nᴏᴡ Bᴇ Sᴇɴᴛ Tᴏ Tʜᴇ Sᴇʟᴇᴄᴛᴇᴅ Cʜᴀɴɴᴇʟ.",
                parse_mode=constants.ParseMode.HTML
            )
            return

    # Otherwise show instruction + set wait mode
    SETDUMP_WAIT.add(uid)
    await update.message.reply_text(
        "Pʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴄʜᴀɴɴᴇʟ ID ᴀꜱ ᴀ ᴘᴀʀᴀᴍᴇᴛᴇʀ (e.g., /setdump -1001234567890) "
        "ᴏʀ ᴛᴏ ꜰᴏʀᴡᴀʀᴅᴇᴅ ᴍᴇꜱꜱᴀɢᴇ ꜰʀᴏᴍ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ.",
        parse_mode=constants.ParseMode.HTML
    )

# ---------- GETDUMP ----------
async def getdump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    dump_id = get_user_dump(uid)
    if not dump_id:
        await update.message.reply_text(
            "Yᴏᴜ ʜᴀᴠᴇ ɴᴏᴛ sᴇᴛ ᴀ Dᴜᴍᴘ Cʜᴀɴɴᴇʟ. Please set one using /setdump.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    # try to fetch channel title + link
    title = "Dump Channel"
    link = None
    try:
        chat = await context.bot.get_chat(dump_id)
        title = chat.title or "Dump Channel"
        if chat.username:
            link = f"https://t.me/{chat.username}"
    except:
        pass

    if link:
        channel_line = f"<a href='{link}'>{title}</a>"
    else:
        # no public link for private channel → show title only
        channel_line = f"<b>{title}</b>"

    await update.message.reply_text(
        "Yᴏᴜʀ Dᴜᴍᴘ Cʜᴀɴɴᴇʟ:\n\n"
        f"{channel_line}\n"
        f"<code>{dump_id}</code>",
        parse_mode=constants.ParseMode.HTML,
        disable_web_page_preview=True
    )

# ---------- DELDUMP ----------
async def deldump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    dump_id = get_user_dump(uid)
    if not dump_id:
        await update.message.reply_text(
            "You don't have a dump channel set. Please set one using /setdump.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    # remove from DB
    try:
        dump_col.delete_one({"_id": uid})
    except:
        pass

    await update.message.reply_text(
        "Dᴜᴍᴘ Cʜᴀɴɴᴇʟ Dᴇʟᴇᴛᴇᴅ.\n\n"
        "Pʟᴇᴀsᴇ Sᴇᴛ A Nᴇᴡ Oɴᴇ ᴜsɪɴɢ /setdump.",
        parse_mode=constants.ParseMode.HTML
    )
# ---------- SETCAP ----------
async def setcap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not context.args:
        await update.message.reply_text(
            "<b>Now Supports Advanced HTML Styling!</b>\n\n"
            "<b>Available Variables:</b>\n"
            "<code>{file_name}</code>\n"
            "<code>{episode}</code>\n"
            "<code>{quality}</code>\n\n"
            "<b>Supported Tags:</b>\n"
            "<blockquote>"
            "&lt;b&gt;Bold&lt;/b&gt;\n"
            "&lt;i&gt;Italic&lt;/i&gt;\n"
            "&lt;u&gt;Underline&lt;/u&gt;\n"
            "&lt;s&gt;Strike&lt;/s&gt;\n"
            "&lt;code&gt;Code&lt;/code&gt;\n"
            "&lt;pre&gt;Pre&lt;/pre&gt;\n"
            "&lt;blockquote&gt;Quote&lt;/blockquote&gt;\n"
            "&lt;blockquote expandable&gt;Expandable&lt;/blockquote&gt;"
            "</blockquote>\n\n"
            "<b>Use \\n for New Line</b>\n\n"
            "<b>Example:</b>\n"
            "<code>/setcap &lt;b&gt;{file_name}&lt;/b&gt;\\nEpisode: {episode}</code>",
            parse_mode=constants.ParseMode.HTML
        )
        return

    template = " ".join(context.args)

    # ✅ convert \n to real linebreak before preview & saving
    template = template.replace("\\n", "\n").strip()

    # ---------- PREVIEW TEST ----------
    # fake sample preview values
    preview = template
    preview = preview.replace("{file_name}", "Naruto_Ep01.mkv")
    preview = preview.replace("{episode}", "01")
    preview = preview.replace("{quality}", "1080p")

    try:
        # try sending preview to detect broken HTML
        test = await update.message.reply_text(
            "<b>Caption Preview:</b>\n\n" + preview,
            parse_mode=constants.ParseMode.HTML
        )
        await test.delete()
    except Exception as e:
        await update.message.reply_text(
            "<b>Invalid HTML detected!</b>\n"
            "Please check your tags and try again.\n\n"
            "<blockquote>Example:\n"
            "&lt;b&gt;Episode {episode}&lt;/b&gt;</blockquote>",
            parse_mode=constants.ParseMode.HTML
        )
        return

    # ---------- SAVE ----------
    set_user_caption(uid, template)

    await update.message.reply_text(
        "<b>Your Caption Has Been Saved Successfully!</b>\n\n"
        "<b>Preview:</b>\n\n" + preview,
        parse_mode=constants.ParseMode.HTML
    )

# ---------- GETCAP ----------
async def getcap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    template = get_user_caption(uid)

    if not template:
        await update.message.reply_text(
            "Yᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀ ᴄᴜꜱᴛᴏᴍ ꜰᴏʀᴍᴀᴛ ꜱᴇᴛ.\n\n"
            "Tʜᴇ ᴅᴇꜰᴀᴜʟᴛ ꜰᴏʀᴍᴀᴛ ᴡɪʟʟ ʙᴇ ᴜꜱᴇᴅ.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    await update.message.reply_text(
        "Yᴏᴜʀ ᴄᴜʀʀᴇɴᴛ ꜰᴏʀᴍᴀᴛ ɪꜱ:\n\n"
        f"<code>{template}</code>",
        parse_mode=constants.ParseMode.HTML
    )

# ---------- RESETCAP ----------
async def resetcap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # remove custom template from DB
    try:
        caps_col.delete_one({"_id": uid})
    except:
        pass

    await update.message.reply_text(
        "Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ꜰᴏʀᴍᴀᴛ ʜᴀꜱ ʙᴇᴇɴ ʀᴇꜱᴇᴛ ᴛᴏ ᴛʜᴇ ᴅᴇꜰᴀᴜʟᴛ ꜰᴏʀᴍᴀᴛ.",
        parse_mode=constants.ParseMode.HTML
    )

# ---------- SETSTICKER ----------
async def setsticker_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # must be a reply to a sticker
    if not update.message.reply_to_message or not update.message.reply_to_message.sticker:
        await update.message.reply_text(
            "Pʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ꜱᴛɪᴄᴋᴇʀ ᴛᴏ ꜱᴇᴛ ɪᴛ ᴀꜱ ʏᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ꜱᴛɪᴄᴋᴇʀ.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    sticker_id = update.message.reply_to_message.sticker.file_id
    set_user_sticker(uid, sticker_id)

    await update.message.reply_text(
        "✅ Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ꜱᴛɪᴄᴋᴇʀ ʜᴀꜱ ʙᴇᴇɴ ꜱᴀᴠᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ.",
        parse_mode=constants.ParseMode.HTML
    )

# ---------- GETSTICKER ----------
async def getsticker_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    st = get_user_sticker(uid)
    if not st:
        await update.message.reply_text(
            "Yᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀ ᴄᴜꜱᴛᴏᴍ ꜱᴛɪᴄᴋᴇʀ ꜱᴇᴛ.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    try:
        await update.message.reply_sticker(sticker=st)
    except:
        # fallback: send as normal sticker
        await context.bot.send_sticker(chat_id=update.effective_chat.id, sticker=st)

# ---------- DELSTICKER ----------
async def delsticker_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    try:
        stickers_col.delete_one({"_id": uid})
    except:
        pass

    await update.message.reply_text(
        "Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ꜱᴛɪᴄᴋᴇʀ ʜᴀꜱ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.",
        parse_mode=constants.ParseMode.HTML
    )

# ---------- LEADERBOARD ----------
async def leaderboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    active = "today"

    rows = get_leaderboard_rows(active, now, limit=20)
    total_sorted = get_leaderboard_total(active, now)
    text = build_leaderboard_text(active, rows, total_sorted)

    await update.message.reply_text(
        text,
        reply_markup=leaderboard_keyboard(active),
        parse_mode=constants.ParseMode.HTML,
        disable_web_page_preview=True
    )

# ---------- BROADCAST ----------
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # OWNER ONLY
    if uid != OWNER_ID:
        return

    # must reply to a message
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "<blockquote>Reply to a message to broadcast it</blockquote>",
            parse_mode=constants.ParseMode.HTML
        )
        return

    msg = update.message.reply_to_message

    total = users_col.count_documents({})
    success = 0
    blocked = 0
    deleted = 0
    failed = 0

    for user in users_col.find({}, {"_id": 1}):
        chat = user["_id"]
        try:
            await context.bot.copy_message(
                chat_id=chat,
                from_chat_id=msg.chat.id,
                message_id=msg.message_id
            )
            success += 1

        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await context.bot.copy_message(
                    chat_id=chat,
                    from_chat_id=msg.chat.id,
                    message_id=msg.message_id
                )
                success += 1
            except Exception as e2:
                failed += 1

        except Exception as e:
            err = str(e).lower()
            if "blocked" in err:
                blocked += 1
            elif "deactivated" in err or "deleted" in err:
                deleted += 1
            else:
                failed += 1

    report = (
        "Broadcast completed\n\n"
        f"◇ Total Users: {total}\n"
        f"◇ Successful: {success}\n"
        f"◇ Blocked Users: {blocked}\n"
        f"◇ Deleted Accounts: {deleted}\n"
        f"◇ Unsuccessful: {failed}"
    )

    await update.message.reply_text(
        report,
        parse_mode=constants.ParseMode.HTML
    )

# ---------- MODE ----------
async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    cur = get_user_mode(uid) or "episode"

    text = (
        f"<b>Select Sorting Mode (Current: {cur.capitalize()})</b>\n\n"
        "<blockquote>• Quality: Sort by quality then episode\n"
        "• Title: Sort by title then episode\n"
        "• Both: Sort by title, quality, then episode\n"
        "• Episode: Default sorting by episode only\n"
        "• Season: Sort by season, then quality, then episode</blockquote>"
    )

    await update.message.reply_text(
        text,
        reply_markup=mode_keyboard(cur),
        parse_mode=constants.ParseMode.HTML
    )


def mode_keyboard(cur: str) -> InlineKeyboardMarkup:
    def label(name: str, key: str) -> str:
        return f"✓ {name}" if key == cur else name

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label("Quality", "quality"), callback_data="mode:quality"),
         InlineKeyboardButton(label("Title", "title"), callback_data="mode:title")],
        [InlineKeyboardButton(label("Both", "both"), callback_data="mode:both"),
         InlineKeyboardButton(label("Episode", "episode"), callback_data="mode:episode")],
        [InlineKeyboardButton(label("Season", "season"), callback_data="mode:season")],
        [InlineKeyboardButton("➥ CLOSE", callback_data="close_msg")]
    ])

async def smode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cur = get_user_smode(uid)

    text = (
        "<b>Sticker Display Settings</b>\n\n"
        "<blockquote>• Quality: Send stickers between quality groups\n"
        "• Default: Send sticker at end of processing</blockquote>\n\n"
        f"<b>Current mode:</b> {cur.capitalize()}"
    )

    await update.message.reply_text(
        text,
        reply_markup=smode_keyboard(cur),
        parse_mode=constants.ParseMode.HTML
    )

# ---------- PRIVATE HANDLER ----------
async def private_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    msg = update.message
    uid = update.effective_user.id

    # Ignore commands
    if msg.text and msg.text.strip().startswith("/"):
        return

    # ---------- SETDUMP PROCESS ----------
    if uid in SETDUMP_WAIT:
        text = (msg.text or "").strip()

        # user sent channel id
        if text and re.fullmatch(r"-100\d{6,}", text):
            SETDUMP_WAIT.discard(uid)
            dump_id = int(text)
            set_user_dump(uid, dump_id)

            await msg.reply_text(
                f"Dᴜᴍᴘ Cʜᴀɴɴᴇʟ Aᴅᴅᴇᴅ: <code>{dump_id}</code>\n\n"
                "Yᴏᴜʀ Fɪʟᴇs Wɪʟʟ Nᴏᴡ Bᴇ Sᴇɴᴛ Tᴏ Tʜᴇ Sᴇʟᴇᴄᴛᴇᴅ Cʜᴀɴɴᴇʟ.",
                parse_mode=constants.ParseMode.HTML
            )
            return

        # user forwarded a message from channel
        ch_id = None
        if msg.forward_origin and msg.forward_origin.chat:
            ch_id = msg.forward_origin.chat.id
        elif msg.forward_from_chat:
            ch_id = msg.forward_from_chat.id

        if ch_id:
            SETDUMP_WAIT.discard(uid)
            set_user_dump(uid, int(ch_id))

            await msg.reply_text(
                f"Dᴜᴍᴘ Cʜᴀɴɴᴇʟ Aᴅᴅᴇᴅ: <code>{ch_id}</code>\n\n"
                "Yᴏᴜʀ Fɪʟᴇs Wɪʟʟ Nᴏᴡ Bᴇ Sᴇɴᴛ Tᴏ Tʜᴇ Sᴇʟᴇᴄᴛᴇᴅ Cʜᴀɴɴᴇʟ.",
                parse_mode=constants.ParseMode.HTML
            )
            return

        # invalid input
        await msg.reply_text(
            "❌ Please send a valid channel ID like <code>-1001234567890</code> "
            "or forward a message from the channel.",
            parse_mode=constants.ParseMode.HTML
        )
        return

    # Accept these as "files"
    is_file = any([
        msg.document,
        msg.video,
        msg.audio,
        msg.photo,
    ])

    if not is_file:
        return

    meta = get_message_meta(msg)

    # init user data
    if uid not in USER_QUEUE:
        USER_QUEUE[uid] = {"files": [], "queue_msgs": []}

    # store file message
    USER_QUEUE[uid]["files"].append({
        "chat_id": msg.chat.id,
        "message_id": msg.message_id,
        "meta": meta
    })

    total = len(USER_QUEUE[uid]["files"])

    # send queue count + store that bot message id too
    r = await msg.reply_text(
        f"{total} Fɪʟᴇ Aᴅᴅᴇᴅ Iɴ Qᴜᴇᴜᴇ",
        parse_mode=constants.ParseMode.HTML
    )

    USER_QUEUE[uid]["queue_msgs"].append(r.message_id)

# ---------- CALLBACK HANDLER ----------
async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # ---------- CLOSE ----------
    if query.data == "close_msg":
        try:
            await query.message.delete()
        except:
            pass
        return

    # ---------- HELP ----------
    if query.data == "help_text":
        await query.edit_message_media(
            media=InputMediaPhoto(
                media=PHOTO_HELP,
                caption=(
                    "<b>Ƈᴏᴍᴍᴀɴᴅs:</b>\n"
                    "<blockquote expandable>/start - Sᴛᴀʀᴛ Tʜᴇ Bᴏᴛ\n"
                    "/help - Sʜᴏᴡ ᴛʜɪꜱ ᴍᴇꜱꜱᴀɢᴇ\n"
                    "/clear - Cʟᴇᴀʀ ʏᴏᴜʀ ꜰɪʟᴇ qᴜᴇᴜᴇ\n"
                    "/sort - Process your queued files\n"
                    "/setdump - Sᴇᴛ A Dᴜᴍᴘ Cʜᴀɴɴᴇʟ\n"
                    "/getdump - Gᴇᴛ Dᴜᴍᴘ Cʜᴀɴɴᴇʟ ID & Lɪɴᴋ\n"
                    "/deldump - Dᴇʟᴇᴛᴇ Yᴏᴜʀ Dᴜᴍᴘ Cʜᴀɴɴᴇʟ\n"
                    "/setcap - Sᴇᴛ ᴀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ\n"
                    "/getcap - Gᴇᴛ ʏᴏᴜʀ ꜰɪʟᴇ ᴄᴀᴘᴛɪᴏɴ\n"
                    "/resetcap - Dᴇʟᴇᴛᴇ ʏᴏᴜʀ ᴄᴀᴘᴛɪᴏɴ\n"
                    "/setsticker - Sᴇᴛ ᴀ ᴄᴜꜱᴛᴏᴍ ꜱᴛɪᴄᴋᴇʀ\n"
                    "/getsticker - Gᴇᴛ ʏᴏᴜʀ ꜱᴛɪᴄᴋᴇʀ\n"
                    "/delsticker - Dᴇʟᴇᴛᴇ ʏᴏᴜʀ ꜱᴛɪᴄᴋᴇʀ</blockquote>\n\n"
                    "<b>Fᴇᴀᴛᴜʀᴇs:</b>\n"
                    "<blockquote>1. Aᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴇxᴛʀᴀᴄᴛ ᴇᴘɪsᴏᴅᴇ ɴᴜᴍʙᴇʀs ᴀɴᴅ ϙᴜᴀʟɪᴛɪᴇs.\n"
                    "2. Sᴏʀᴛ ꜰɪʟᴇs ʙʏ ᴇᴘɪsᴏᴅᴇ ɴᴜᴍʙᴇʀ.\n"
                    "3. Cᴜsᴛᴏᴍ Dᴜᴍᴘ Cʜᴀɴɴᴇʟ,ᴄᴀᴘᴛɪᴏɴs ᴀɴᴅ sᴛɪᴄᴋᴇʀs sᴜᴘᴘᴏʀᴛ.\n"
                    "4. Cʟᴇᴀʀ ʏᴏᴜʀ ǫᴜᴇᴜᴇ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴀꜰᴛᴇʀ ᴘʀᴏᴄᴇssɪɴɢ.</blockquote>\n\n"
                    "<blockquote>Cᴏɴᴛᴀᴄᴛ Fᴏʀ Mᴏᴅɪꜰɪᴄᴀᴛɪᴏɴꜱ - @ITSANIMEN</blockquote>"
                    

                ),
                parse_mode=constants.ParseMode.HTML
            ),
            reply_markup=help_keyboard()
        )
        return

    # ---------- BACK ----------
    if query.data == "back_to_start":
        await query.edit_message_media(
            media=InputMediaPhoto(
                media=PHOTO_MAIN,
                caption=(
                    "<blockquote>Wᴇʟᴄᴏᴍᴇ ᴛᴏ ʏᴏᴜʀ ᴀʟʟ-ɪɴ-ᴏɴᴇ Fɪʟᴇ Mᴀɴᴀɢᴇᴍᴇɴᴛ Assɪsᴛᴀɴᴛ! 📂✨</blockquote>\n\n"
                    "<blockquote>Eᴀsɪʟʏ ᴍᴀɴᴀɢᴇ, ᴏʀɢᴀɴɪᴢᴇ, ᴀɴᴅ sʜᴀʀᴇ ʏᴏᴜʀ ꜰɪʟᴇs ᴡɪᴛʜᴏᴜᴛ ᴀɴʏ \n"
                    "ʜᴀssʟᴇ. Sᴀʏ ɢᴏᴏᴅʙʏᴇ ᴛᴏ ᴍᴇssʏ ꜰɪʟᴇ ɴᴀᴍᴇs ᴀɴᴅ ᴄᴏɴꜰᴜsɪɴɢ \n"
                    "ʀᴇsᴏʟᴜᴛɪᴏɴs – ᴡᴇ’ᴠᴇ ɢᴏᴛ ʏᴏᴜ ᴄᴏᴠᴇʀᴇᴅ!</blockquote>\n\n"
                    "<blockquote><b>◈ MAINTAINED BY : "
                    "<a href='https://t.me/ITSANIMEN'>彡 ΔNI_OTΔKU 彡</a>"
                    "</b></blockquote>"
                ),
                parse_mode=constants.ParseMode.HTML
            ),
            reply_markup=start_keyboard()
        )
        return
    
    # ---------- LEADERBOARD BUTTONS ----------
    if query.data.startswith("lb:"):
        active = query.data.split(":", 1)[1]  # today|week|month|all
        if active not in ("today", "week", "month", "all"):
            return

        now = datetime.utcnow()
        rows = get_leaderboard_rows(active, now, limit=20)
        total_sorted = get_leaderboard_total(active, now)
        text = build_leaderboard_text(active, rows, total_sorted)

        await query.edit_message_text(
            text=text,
            reply_markup=leaderboard_keyboard(active),
            parse_mode=constants.ParseMode.HTML,
            disable_web_page_preview=True
        )
        return
    
    # ---------- MODE SELECT ----------
    if query.data.startswith("mode:"):
        uid = query.from_user.id
        picked = query.data.split(":", 1)[1]  # quality|title|both|episode|season

        if picked not in ("quality", "title", "both", "episode", "season"):
            return

        set_user_mode(uid, picked)

        cur = picked
        text = (
            f"<b>Select Sorting Mode (Current: {cur.capitalize()})</b>\n\n"
            "<blockquote>• Quality: Sort by quality then episode\n"
            "• Title: Sort by title then episode\n"
            "• Both: Sort by title, quality, then episode\n"
            "• Episode: Default sorting by episode only\n"
            "• Season: Sort by season, then quality, then episode</blockquote>"
        )

        await query.edit_message_text(
            text,
            reply_markup=mode_keyboard(cur),
            parse_mode=constants.ParseMode.HTML
        )
        return
    
    # ---------- SMODE SELECT ----------
    if query.data.startswith("smode:"):
        uid = query.from_user.id
        picked = query.data.split(":", 1)[1]  # default|quality
        if picked not in ("default", "quality"):
            return

        set_user_smode(uid, picked)

        cur = picked
        text = (
            "<b>Sticker Display Settings</b>\n\n"
            "<blockquote>• Quality: Send stickers between quality groups\n"
            "• Default: Send sticker at end of processing</blockquote>\n\n"
            f"<b>Current mode:</b> {cur.capitalize()}"
        )

        await query.edit_message_text(
            text,
            reply_markup=smode_keyboard(cur),
            parse_mode=constants.ParseMode.HTML
        )
        return

# ---------- RESTART BROADCAST (ALWAYS ON REDEPLOY) ----------
async def broadcast_restart(application: Application):
    RE_caption = (
        "<blockquote expandable>"
        "🔄 <b>Bot Restarted Successfully!\n\n"
        "✅ New changes have been deployed.\n"
        "🚀 Bot is now online and running smoothly.\n\n"
        "Thank you for your patience.</b>"
        "</blockquote>"
    )

    buttons = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("ʂυρρσɾƚ", url="https://t.me/BotifyX_support"),
                InlineKeyboardButton("Cԋαɳɳҽʅ", url="https://t.me/BotifyX_Pro_Botz")
            ]
        ]
    )

    for user in users_col.find({}):
        try:
            await application.bot.send_photo(
                chat_id=user["_id"],
                photo=RESTART_PHOTO_ID,
                caption=RE_caption,
                reply_markup=buttons,
                parse_mode=constants.ParseMode.HTML
            )
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except:
            continue

# ---------- POST INIT ----------
async def post_init(application: Application):
    # ✅ ensure stats doc exists + mark last restarted (Mongo)
    try:
        stats_col.update_one(
            {"_id": "bot"},
            {
                "$set": {"last_restarted": datetime.utcnow()},
                "$setOnInsert": {"total_sorted_files": 0}
            },
            upsert=True
        )
    except:
        pass

    try:
        await application.bot.send_message(
            OWNER_ID,
            "<b>🤖 Bot has started successfully!</b>",
            parse_mode=constants.ParseMode.HTML
        )
    except:
        pass

    await broadcast_restart(application)

# ---------- MAIN ----------
def main():
    Thread(target=run_flask, daemon=True).start()
    
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("sort", sort_cmd))
    application.add_handler(CommandHandler("clear", clear_cmd))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("setdump", setdump_cmd))
    application.add_handler(CommandHandler("getdump", getdump_cmd))
    application.add_handler(CommandHandler("deldump", deldump_cmd))
    application.add_handler(CommandHandler("setcap", setcap_cmd))
    application.add_handler(CommandHandler("getcap", getcap_cmd))
    application.add_handler(CommandHandler("resetcap", resetcap_cmd))
    application.add_handler(CommandHandler("setsticker", setsticker_cmd))
    application.add_handler(CommandHandler("getsticker", getsticker_cmd))
    application.add_handler(CommandHandler("delsticker", delsticker_cmd))
    application.add_handler(CommandHandler("leaderboard", leaderboard_cmd))
    application.add_handler(CommandHandler("broadcast", broadcast_cmd))
    application.add_handler(CommandHandler("mode", mode_cmd))
    application.add_handler(CommandHandler("smode", smode_cmd))
    application.add_handler(CallbackQueryHandler(handle_callbacks))

    application.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, private_handler)
    )

    application.run_polling()

if __name__ == "__main__":
    main()
