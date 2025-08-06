import asyncio
import logging
import asyncpg
import time
import csv
import re
from io import StringIO
from telegram import Update, ReplyKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler
)
import os
from dotenv import load_dotenv
load_dotenv()

# ======================
# Configuration
# ======================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "6784563936"))
POSTGRES_URL = os.environ.get("POSTGRES_URL")

ASK_NAME, ASK_EMAIL, ASK_ACCOUNT, CHANGE_NAME, CHANGE_EMAIL = range(5)
ASK_BANK_NAME, ASK_ACCOUNT_NUMBER, ASK_ACCOUNT_NAME, CHOOSE_BALANCE, ASK_WITHDRAW_AMOUNT = range(100, 105)

CHANNEL_USERNAME = "@cashearnify"
GROUP_USERNAME = "@homeofupdatez"

# ======================
# Logging
# ======================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ======================
# Database Setup
# ======================

db_pool = None  # Global variable for the connection pool

async def init_db():
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                referrals INTEGER DEFAULT 0,
                balance INTEGER DEFAULT 0,
                completed_tasks INTEGER DEFAULT 0,
                name TEXT,
                email TEXT,
                gender TEXT,
                change_count INTEGER DEFAULT 0,
                last_daily_claim BIGINT DEFAULT 0,
                main_balance INTEGER DEFAULT 0,
                reward_balance INTEGER DEFAULT 0,
                earning_balance INTEGER DEFAULT 0,
                referral_balance INTEGER DEFAULT 0
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_tasks (
                user_id BIGINT,
                task_name TEXT,
                PRIMARY KEY (user_id, task_name)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                bank_name TEXT,
                account_number TEXT,
                account_name TEXT,
                balance_type TEXT,
                amount BIGINT,
                status TEXT DEFAULT 'pending',
                requested_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT,
                reward_amount INTEGER,
                referred_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_banks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                bank_name TEXT,
                account_number TEXT,
                account_name TEXT
            )
            """
        )

async def has_completed_task(user_id, task_name):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM user_tasks WHERE user_id = $1 AND task_name = $2", user_id, task_name)
    return row is not None

async def mark_task_completed(user_id, task_name):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO user_tasks (user_id, task_name) VALUES ($1, $2) ON CONFLICT DO NOTHING", user_id, task_name)

# ======================
# Keyboards
# ======================

def get_main_keyboard(user_id=None):
    keyboard = [
        ["👤 Profile", "💰 Balance"],
        ["🏧 Withdrawal", "🔗 Referrals"],
        ["💳 Deposit", "📝 Tasks", "🛎️ Services"]
    ]
    if user_id == ADMIN_ID:
        keyboard.append(["🛠️ Admin Panel"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_tasks_keyboard():
    keyboard = [
        ["🆕 New User Tasks"],
        ["🗓️ Daily Tasks"],
        ["📈 Earning History"],
        ["⬅️ Go Back", "🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_new_user_tasks_keyboard():
    keyboard = [
        ["✅ Join Channel (₦1000)"],
        ["✅ Join Group (₦1000)"],
        ["⬅️ Go Back", "🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_daily_tasks_keyboard():
    keyboard = [
        ["🎁 Daily Login Reward"],
        ["⬅️ Go Back", "🏠 Main Menu"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_gender_keyboard():
    keyboard = [
        ["Male", "Female"],
        ["Other"]
    ]
    return ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

def get_admin_keyboard():
    keyboard = [
        ["👥 User Stats", "📢 Broadcast"],
        ["🔍 Search User", "💸 Edit Balance"],
        ["❌ Ban User", "📤 Export Users"],
        ["⬅️ Back to Main"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_export_keyboard():
    keyboard = [
        ["All Users", "By Balance"],
        ["By Gender", "By Referrals"],
        ["⬅️ Cancel Export"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_go_back_keyboard():
    return ReplyKeyboardMarkup([["⬅️ Go Back", "🏠 Main Menu"]], resize_keyboard=True)


# ======================
# Bot Handlers
# ======================

user_last_action = {}

def is_rate_limited(user_id, seconds=2):
    now = time.time()
    last = user_last_action.get(user_id, 0)
    if now - last < seconds:
        return True
    user_last_action[user_id] = now
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id

    # Save referral_id in user_data for later use
    referral_id = None
    if context.args:
        try:
            referral_id = int(context.args[0])
        except ValueError:
            referral_id = None
    context.user_data["referral_id"] = referral_id

    # Check if user already registered
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT name FROM users WHERE user_id = $1", user_id)

    if not row or not row["name"]:
        await update.message.reply_text("✅ Welcome! Please enter your full name to begin registration:")
        return ASK_NAME

    await update.message.reply_text(
        "🎉 Welcome back to the Giveaway Bot!\nUse the buttons below to get started.",
        reply_markup=get_main_keyboard(user_id)
    )
    return ConversationHandler.END

async def joined(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ℹ️ Joining channels is now a task. Please use the 📝 Tasks button to see available tasks.")
    return ConversationHandler.END

async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name or len(name.split()) < 2:
        await update.message.reply_text("❌ Please enter your full name (at least two words):")
        return ASK_NAME
    context.user_data["name"] = name
    await update.message.reply_text("📧 Now enter your email address:")
    return ASK_EMAIL

async def ask_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    email = update.message.text
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        await update.message.reply_text("❌ Please enter a valid email address:")
        return ASK_EMAIL
    context.user_data["email"] = email
    await update.message.reply_text("🚻 Please select your gender:", reply_markup=get_gender_keyboard())
    return ASK_ACCOUNT  # We'll reuse ASK_ACCOUNT as ASK_GENDER for simplicity

async def ask_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This is now the gender step
    context.user_data["gender"] = update.message.text
    user = update.effective_user
    user_id = user.id
    name = context.user_data["name"]
    email = context.user_data["email"]
    gender = context.user_data["gender"]
    referral_id = context.user_data.get("referral_id")

    earning_balance = 0
    referral_balance = 0
    bonus_msg = ""
    async with db_pool.acquire() as conn:
        # If joined with a valid referral
        if referral_id and referral_id != user_id:
            ref_exists = await conn.fetchrow("SELECT 1 FROM users WHERE user_id = $1", referral_id)
            if ref_exists:
                # Give referrer 500 to referral_balance
                await conn.execute("UPDATE users SET referrals = referrals + 1, referral_balance = referral_balance + 500 WHERE user_id = $1", referral_id)
                # Give new user 1500 to referral_balance
                referral_balance = 1500
                # Log the referral
                await conn.execute(
                    "INSERT INTO referrals (referrer_id, referred_id, reward_amount) VALUES ($1, $2, $3)",
                    referral_id, user_id, 500
                )
                bonus_msg = "\n\n🎉 You received a ₦1500 welcome bonus for joining with a referral!"
        await conn.execute(
            "INSERT INTO users (user_id, name, email, gender, completed_tasks, balance, referrals, change_count, main_balance, reward_balance, earning_balance, referral_balance) "
            "VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, $5, $6) "
            "ON CONFLICT (user_id) DO UPDATE SET name = $2, email = $3, gender = $4, earning_balance = $5, referral_balance = $6",
            user_id, name, email, gender, earning_balance, referral_balance
        )

    await update.message.reply_text(
        f"🎉 Registration complete!{bonus_msg}\n\nName: {name}\nEmail: {email}\nGender: {gender}\n\nUse the buttons below to get started.",
        reply_markup=get_main_keyboard(user_id)
    )
    return ConversationHandler.END

async def changeinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT change_count FROM users WHERE user_id = $1", user_id)
        if not row:
            await update.message.reply_text("❌ You are not registered.")
            return ConversationHandler.END
        if row["change_count"] >= 1:
            await update.message.reply_text("❌ You can only change your name and email once.")
            return ConversationHandler.END
    await update.message.reply_text("Enter your new full name:")
    return CHANGE_NAME

async def change_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_name"] = update.message.text
    await update.message.reply_text("Enter your new email address:")
    return CHANGE_EMAIL

async def change_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    new_name = context.user_data["new_name"]
    new_email = update.message.text
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name = $1, email = $2, change_count = change_count + 1 WHERE user_id = $3",
            new_name, new_email, user_id
        )
    await update.message.reply_text("✅ Your name and email have been updated.")
    return ConversationHandler.END

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if is_rate_limited(user_id):
        await update.message.reply_text("⏳ Please wait a moment before sending another command.")
        return

    # --- ADMIN ADVANCED FEATURES ---
    if user_id == ADMIN_ID:
        if context.user_data.get("admin_action") == "search_user":
            query = text.strip()
            context.user_data["admin_action"] = None
            async with db_pool.acquire() as conn:
                if query.isdigit():
                    user = await conn.fetchrow("SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE user_id = $1", int(query))
                else:
                    user = await conn.fetchrow("SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE name ILIKE $1", f"%{query}%")
            if user:
                await update.message.reply_text(
                    f"👤 User Info:\nID: {user['user_id']}\nName: {user['name']}\nEmail: {user['email']}\nGender: {user['gender']}\n"
                    f"Main: ₦{user['main_balance']}, Reward: ₦{user['reward_balance']}, Earning: ₦{user['earning_balance']}\nReferrals: {user['referrals']}",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await update.message.reply_text("❌ User not found.", reply_markup=get_admin_keyboard())
            return

        if context.user_data.get("admin_action") == "edit_balance_id":
            if text.isdigit():
                context.user_data["edit_balance_user"] = int(text)
                context.user_data["admin_action"] = "edit_balance_type"
                await update.message.reply_text("Which balance do you want to edit? (main/reward/earning)", reply_markup=get_admin_keyboard())
            else:
                context.user_data["admin_action"] = None
                await update.message.reply_text("❌ Invalid user ID.", reply_markup=get_admin_keyboard())
            return

        if context.user_data.get("admin_action") == "edit_balance_type":
            balance_type = text.strip().lower()
            if balance_type not in ["main", "reward", "earning"]:
                context.user_data["admin_action"] = None
                await update.message.reply_text("❌ Invalid balance type. Use main, reward, or earning.", reply_markup=get_admin_keyboard())
                return
            context.user_data["edit_balance_type"] = balance_type
            context.user_data["admin_action"] = "edit_balance_amount"
            await update.message.reply_text(f"Enter the new amount for {balance_type} balance:", reply_markup=get_admin_keyboard())
            return

        if context.user_data.get("admin_action") == "edit_balance_amount":
            try:
                amount = int(text)
                user_id_to_edit = context.user_data.get("edit_balance_user")
                balance_type = context.user_data.get("edit_balance_type")
                col = {"main": "main_balance", "reward": "reward_balance", "earning": "earning_balance"}[balance_type]
                async with db_pool.acquire() as conn:
                    await conn.execute(f"UPDATE users SET {col} = $1 WHERE user_id = $2", amount, user_id_to_edit)
                await update.message.reply_text(f"✅ {balance_type.capitalize()} balance updated to {amount} for user {user_id_to_edit}.", reply_markup=get_admin_keyboard())
            except Exception:
                await update.message.reply_text("❌ Invalid amount.", reply_markup=get_admin_keyboard())
            context.user_data["admin_action"] = None
            context.user_data["edit_balance_user"] = None
            context.user_data["edit_balance_type"] = None
            return

        if context.user_data.get("admin_action") == "ban_user":
            if text.isdigit():
                ban_id = int(text)
                async with db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM users WHERE user_id = $1", ban_id)
                    await conn.execute("DELETE FROM user_tasks WHERE user_id = $1", ban_id)
                await update.message.reply_text(f"🚫 User {ban_id} has been banned and removed.", reply_markup=get_admin_keyboard())
            else:
                await update.message.reply_text("❌ Invalid user ID.", reply_markup=get_admin_keyboard())
            context.user_data["admin_action"] = None
            return

    # --- MAIN USER/ADMIN MENU ---
    if text == "👤 Profile":
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT completed_tasks, main_balance, reward_balance, earning_balance, referral_balance, name, email, gender FROM users WHERE user_id = $1",
                    user_id
                )
            tasks = row["completed_tasks"] if row else 0
            main_balance = row["main_balance"] if row else 0
            reward_balance = row["reward_balance"] if row else 0
            earning_balance = row["earning_balance"] if row else 0
            referral_balance = row["referral_balance"] if row else 0
            name = row["name"] if row else "N/A"
            email = row["email"] if row else "N/A"
            gender = row["gender"] if row else "N/A"
            await update.message.reply_text(
                f"👤 Profile\nID: {user_id}\nName: {name}\nEmail: {email}\nGender: {gender}\n"
                f"✅ Tasks Done: {tasks}\n"
                f"💰 Main Balance: ₦{main_balance}\n"
                f"🎁 Reward Balance: ₦{reward_balance}\n"
                f"🪙 Earning Balance: ₦{earning_balance}\n"
                f"👥 Referral Balance: ₦{referral_balance}"
            )
        except Exception as e:
            logging.error(f"DB error in Profile: {e}")
            await update.message.reply_text("⚠️ Sorry, something went wrong fetching your profile. Please try again later.")

    elif text == "💰 Balance":
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT main_balance, reward_balance, earning_balance, referral_balance FROM users WHERE user_id = $1",
                    user_id
                )
            main_balance = row["main_balance"] if row else 0
            reward_balance = row["reward_balance"] if row else 0
            earning_balance = row["earning_balance"] if row else 0
            referral_balance = row["referral_balance"] if row else 0
            await update.message.reply_text(
                f"💰 Main Balance: ₦{main_balance}\n"
                f"🎁 Reward Balance: ₦{reward_balance}\n"
                f"🪙 Earning Balance: ₦{earning_balance}\n"
                f"👥 Referral Balance: ₦{referral_balance}"
            )
        except Exception as e:
            logging.error(f"DB error in Balance: {e}")
            await update.message.reply_text("⚠️ Sorry, something went wrong fetching your balance. Please try again later.")

    elif text == "🏧 Withdrawal":
        try:
            async with db_pool.acquire() as conn:
                banks = await conn.fetch("SELECT id, bank_name, account_number, account_name FROM user_banks WHERE user_id = $1", user_id)
            keyboard = []
            for bank in banks:
                keyboard.append([f"{bank['bank_name']} | {bank['account_number']} | {bank['account_name']}"])
            if len(banks) < 2:
                keyboard.append(["➕ Add Account"])
            keyboard.append(["⬅️ Go Back", "🏠 Main Menu"])
            await update.message.reply_text(
                "🏦 Select a bank account for withdrawal or add a new one:",
                reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
            )
            context.user_data["withdraw_state"] = "choose_bank"
            return
        except Exception as e:
            logging.error(f"DB error in Withdrawal (fetch banks): {e}")
            await update.message.reply_text("⚠️ Sorry, something went wrong fetching your bank accounts. Please try again later.")
            return

    elif context.user_data.get("withdraw_state") == "choose_bank":
        if text == "➕ Add Account":
            await update.message.reply_text("🏦 Enter your Bank Name:", reply_markup=get_go_back_keyboard())
            context.user_data["withdraw_state"] = "add_bank_name"
            return
        elif text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        else:
            try:
                async with db_pool.acquire() as conn:
                    bank = await conn.fetchrow(
                        "SELECT * FROM user_banks WHERE user_id = $1 AND CONCAT(bank_name, ' | ', account_number, ' | ', account_name) = $2",
                        user_id, text
                    )
                if not bank:
                    await update.message.reply_text("❌ Invalid selection. Please try again.", reply_markup=get_go_back_keyboard())
                    return
                context.user_data["withdraw_bank"] = bank
                # Fetch balances and referrals
                async with db_pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT main_balance, reward_balance, earning_balance, referral_balance, referrals FROM users WHERE user_id = $1", user_id)
                main = row["main_balance"] if row else 0
                reward = row["reward_balance"] if row else 0
                earning = row["earning_balance"] if row else 0
                referral = row["referral_balance"] if row else 0
                referrals = row["referrals"] if row else 0
                keyboard = [
                    [f"Main Balance (₦{main})"],
                    [f"Reward Balance (₦{reward})"],
                    [f"Earning Balance (₦{earning})"],
                    [f"Referral Balance (₦{referral})"],
                    ["⬅️ Go Back", "🏠 Main Menu"]
                ]
                await update.message.reply_text(
                    "💸 <b>Which balance do you want to withdraw from?</b>\n\n"
                    "• <b>Main Balance</b>: <i>Withdraw anytime, any amount.</i>\n"
                    "• <b>Reward Balance</b>: <i>Withdraw anytime, any amount — <b>after 10 referrals</b>.</i>\n"
                    "• <b>Earning Balance</b>: <i>Withdraw only if you have <b>10 referrals</b> and a minimum of ₦30,000.</i>\n"
                    "• <b>Referral Balance</b>: <i>Withdraw only if you have <b>10 referrals</b> and a minimum of ₦15,000.</i>",
                    reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
                    parse_mode="HTML"
                )
                context.user_data["withdraw_state"] = CHOOSE_BALANCE
                context.user_data["withdraw"] = {
                    "referrals": referrals,
                    "main": main,
                    "reward": reward,
                    "earning": earning,
                    "referral": referral
                }
                return
            except Exception as e:
                logging.error(f"DB error in Withdrawal (choose bank): {e}")
                await update.message.reply_text("⚠️ Sorry, something went wrong. Please try again later.")
                return

    elif context.user_data.get("withdraw_state") == "add_bank_name":
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        # Validate bank name: only letters and spaces
        if not re.match(r"^[A-Za-z ]+$", text.strip()):
            await update.message.reply_text("❌ Please enter a valid bank name (letters and spaces only).", reply_markup=get_go_back_keyboard())
            return
        context.user_data["new_bank"] = {"bank_name": text}
        await update.message.reply_text("🔢 Enter your Account Number:", reply_markup=get_go_back_keyboard())
        context.user_data["withdraw_state"] = "add_account_number"
        return

    elif context.user_data.get("withdraw_state") == "add_account_number":
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        # Validate account number: must be 10 digits
        if not text.isdigit() or len(text) != 10:
            await update.message.reply_text("❌ Please enter a valid 10-digit account number.", reply_markup=get_go_back_keyboard())
            return
        context.user_data["new_bank"]["account_number"] = text
        await update.message.reply_text("👤 Enter your Account Name:", reply_markup=get_go_back_keyboard())
        context.user_data["withdraw_state"] = "add_account_name"
        return

    elif context.user_data.get("withdraw_state") == "add_account_name":
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        # Validate account name: only letters and spaces, at least two words
        if not re.match(r"^[A-Za-z ]+$", text.strip()) or len(text.strip().split()) < 2:
            await update.message.reply_text("❌ Please enter a valid account name (letters and spaces only, at least two words).", reply_markup=get_go_back_keyboard())
            return
        context.user_data["new_bank"]["account_name"] = text
        try:
            async with db_pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM user_banks WHERE user_id = $1", user_id)
                if count >= 2:
                    await update.message.reply_text("❌ You can only save up to 2 bank accounts.", reply_markup=get_main_keyboard(user_id))
                    context.user_data["withdraw_state"] = None
                    return
                await conn.execute(
                    "INSERT INTO user_banks (user_id, bank_name, account_number, account_name) VALUES ($1, $2, $3, $4)",
                    user_id,
                    context.user_data["new_bank"]["bank_name"],
                    context.user_data["new_bank"]["account_number"],
                    context.user_data["new_bank"]["account_name"]
                )
            await update.message.reply_text("✅ Bank account added!", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        except Exception as e:
            logging.error(f"DB error in Withdrawal (add bank): {e}")
            await update.message.reply_text("⚠️ Sorry, something went wrong saving your bank account. Please try again later.")
            context.user_data["withdraw_state"] = None
            return

    elif context.user_data.get("withdraw_state") == CHOOSE_BALANCE:
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        balance_map = {
            "Main Balance": "main_balance",
            "Reward Balance": "reward_balance",
            "Earning Balance": "earning_balance",
            "Referral Balance": "referral_balance"
        }
        for key in balance_map:
            if text.startswith(key):
                context.user_data["withdraw"]["balance_type"] = balance_map[key]
                context.user_data["withdraw"]["balance_label"] = key
                break
        else:
            await update.message.reply_text("❌ Please select a valid balance option.", reply_markup=get_go_back_keyboard())
            return

        # Show criteria prompt
        if context.user_data["withdraw"]["balance_label"] == "Main Balance":
            await update.message.reply_text("✅ You can withdraw any amount from your Main Balance at any time.", reply_markup=get_go_back_keyboard())
        elif context.user_data["withdraw"]["balance_label"] == "Reward Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Reward Balance after referring 10 people.", reply_markup=get_go_back_keyboard()
            )
        elif context.user_data["withdraw"]["balance_label"] == "Earning Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Earning Balance after referring 10 people and the minimum withdrawal is ₦30,000.", reply_markup=get_go_back_keyboard()
            )
        elif context.user_data["withdraw"]["balance_label"] == "Referral Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Referral Balance after referring 10 people and the minimum withdrawal is ₦15,000.", reply_markup=get_go_back_keyboard()
            )

        await update.message.reply_text(
            f"Enter the amount you want to withdraw from your {context.user_data['withdraw']['balance_label']}:",
            reply_markup=get_go_back_keyboard()
        )
        context.user_data["withdraw_state"] = ASK_WITHDRAW_AMOUNT
        return

    elif context.user_data.get("withdraw_state") == ASK_WITHDRAW_AMOUNT:
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        try:
            amount = int(text)
            if amount <= 0 or amount > 500000:
                raise ValueError()
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid positive amount (max ₦500,000).", reply_markup=get_go_back_keyboard())
            return

        details = context.user_data["withdraw"]
        referrals = details["referrals"]
        balance_type = details["balance_type"]
        balance_label = details["balance_label"]
        main = details["main"]
        reward = details["reward"]
        earning = details["earning"]
        referral = details["referral"]

        # Criteria checks
        if balance_type == "reward_balance" and referrals < 10:
            await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Reward Balance.", reply_markup=get_go_back_keyboard())
            return
        if balance_type == "earning_balance":
            if referrals < 10:
                await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Earning Balance.", reply_markup=get_go_back_keyboard())
                return
            if amount < 30000:
                await update.message.reply_text("❌ Minimum withdrawal from Earning Balance is ₦30,000.", reply_markup=get_go_back_keyboard())
                return
        if balance_type == "referral_balance":
            if referrals < 10:
                await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Referral Balance.", reply_markup=get_go_back_keyboard())
                return
            if amount < 15000:
                await update.message.reply_text("❌ Minimum withdrawal from Referral Balance is ₦15,000.", reply_markup=get_go_back_keyboard())
                return

        # Check sufficient balance
        if balance_type == "main_balance" and amount > main:
            await update.message.reply_text("❌ Insufficient Main Balance.", reply_markup=get_go_back_keyboard())
            context.user_data["withdraw_state"] = None
            return
        if balance_type == "reward_balance" and amount > reward:
            await update.message.reply_text("❌ Insufficient Reward Balance.", reply_markup=get_go_back_keyboard())
            return
        if balance_type == "earning_balance" and amount > earning:
            await update.message.reply_text("❌ Insufficient Earning Balance.", reply_markup=get_go_back_keyboard())
            return
        if balance_type == "referral_balance" and amount > referral:
            await update.message.reply_text("❌ Insufficient Referral Balance.", reply_markup=get_go_back_keyboard())
            return

        # Show account selection again for confirmation
        async with db_pool.acquire() as conn:
            banks = await conn.fetch("SELECT id, bank_name, account_number, account_name FROM user_banks WHERE user_id = $1", user_id)
        keyboard = []
        for bank in banks:
            keyboard.append([f"{bank['bank_name']} | {bank['account_number']} | {bank['account_name']}"])
        keyboard.append(["⬅️ Go Back", "🏠 Main Menu"])
        await update.message.reply_text(
            "✅ Requirements met!\nSelect the account to receive your withdrawal:",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        )
        context.user_data["withdraw_state"] = "final_account_select"
        context.user_data["withdraw"]["amount"] = amount
        return

    elif context.user_data.get("withdraw_state") == "final_account_select":
        if text == "⬅️ Go Back" or text == "🏠 Main Menu":
            await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))
            context.user_data["withdraw_state"] = None
            return
        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    bank = await conn.fetchrow(
                        "SELECT * FROM user_banks WHERE user_id = $1 AND CONCAT(bank_name, ' | ', account_number, ' | ', account_name) = $2",
                        user_id, text
                    )
                    if not bank:
                        await update.message.reply_text("❌ Invalid selection. Please try again.", reply_markup=get_go_back_keyboard())
                        return
                    details = context.user_data["withdraw"]
                    amount = details["amount"]
                    balance_type = details["balance_type"]
                    balance_label = details["balance_label"]
                    # Double-check balance in DB and lock row
                    valid_cols = {
                        "main_balance": "main_balance",
                        "reward_balance": "reward_balance",
                        "earning_balance": "earning_balance",
                        "referral_balance": "referral_balance"
                    }
                    col = valid_cols.get(balance_type)
                    if not col:
                        await update.message.reply_text("❌ Invalid balance type.")
                        return
                    row = await conn.fetchrow(f"SELECT {col} FROM users WHERE user_id = $1 FOR UPDATE", user_id)
                    if not row or row[balance_type] < amount:
                        await update.message.reply_text("❌ Insufficient balance. Please try again.", reply_markup=get_go_back_keyboard())
                        return
                    # Deduct balance
                    await conn.execute(f"UPDATE users SET {balance_type} = {balance_type} - $1 WHERE user_id = $2", amount, user_id)
                    # Insert withdrawal request
                    await conn.execute(
                        "INSERT INTO withdrawals (user_id, bank_name, account_number, account_name, balance_type, amount) VALUES ($1, $2, $3, $4, $5, $6)",
                        user_id, bank['bank_name'], bank['account_number'], bank['account_name'], balance_type, amount
                    )
            # Notify user and admin (outside transaction)
            await update.message.reply_text(
                f"✅ Withdrawal Request:\n"
                f"Bank: {bank['bank_name']}\n"
                f"Account Number: {bank['account_number']}\n"
                f"Account Name: {bank['account_name']}\n"
                f"Balance: {balance_label}\n"
                f"Amount: ₦{amount}\n\n"
                "Your request has been received. An admin will process it soon.",
                reply_markup=get_main_keyboard(user_id)
            )
            user = update.effective_user
            admin_msg = (
                f"💸 New Withdrawal Request\n"
                f"User: {user.full_name} (@{user.username})\n"
                f"User ID: {user_id}\n"
                f"Bank: {bank['bank_name']}\n"
                f"Account Number: {bank['account_number']}\n"
                f"Account Name: {bank['account_name']}\n"
                f"Balance: {balance_label}\n"
                f"Amount: ₦{amount}\n\n"
                "Your request has been received. An admin will process it soon."
            )
            await context.bot.send_message(ADMIN_ID, admin_msg)
            context.user_data["withdraw_state"] = None
            context.user_data["withdraw"] = {}
            return
        except Exception as e:
            logging.error(f"DB error in Withdrawal (final account select): {e}")
            await update.message.reply_text("⚠️ Sorry, something went wrong processing your withdrawal. Please try again later.")
            context.user_data["withdraw_state"] = None
            context.user_data["withdraw"] = {}
            return

    elif text == "🔗 Referrals":
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT referrals FROM users WHERE user_id = $1", user_id)
            referrals_count = row["referrals"] if row else 0
            bot_username = (await context.bot.get_me()).username
            referral_link = f"https://t.me/{bot_username}?start={user_id}"
            referred_rows = await conn.fetch(
                "SELECT referred_id, reward_amount, referred_at FROM referrals WHERE referrer_id = $1", user_id
            )
        if referred_rows:
            referred_list = "\n".join(
                [f"• {r['referred_id']} | ₦{r['reward_amount']} | {r['referred_at'].strftime('%Y-%m-%d')}" for r in referred_rows]
            )
            referred_text = f"\n\n👥 Your Referrals:\n{referred_list}"
        else:
            referred_text = "\n\nYou have not referred anyone yet."
        await update.message.reply_text(
            f"📊 You have referred {referrals_count} users.\n"
            f"🔗 Your referral link:\n{referral_link}"
            f"{referred_text}"
        )

    elif text == "📝 Tasks":
        await update.message.reply_text(
            "📝 Tasks Menu:",
            reply_markup=get_tasks_keyboard()
        )

    elif text == "🆕 New User Tasks":
        await update.message.reply_text(
            "🆕 Complete these tasks to get started:",
            reply_markup=get_new_user_tasks_keyboard()
        )

    elif text == "✅ Join Channel (₦1000)":
        if await has_completed_task(user_id, "joined_channel"):
            await update.message.reply_text(
                "✅ You have already claimed this reward.",
                reply_markup=get_new_user_tasks_keyboard()
            )
        else:
            # Validate channel join
            try:
                member = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    raise Exception()
            except Exception:
                await update.message.reply_text(
                    f"❌ You must join the channel {CHANNEL_USERNAME} to claim this reward.",
                    reply_markup=get_new_user_tasks_keyboard()
                )
                return
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET completed_tasks = completed_tasks + 1, earning_balance = earning_balance + 1000 WHERE user_id = $1",
                    user_id
                )
            await mark_task_completed(user_id, "joined_channel")
            await update.message.reply_text(
                "🎉 Task completed!\n🪙 You earned ₦1000 for joining our channel.",
                reply_markup=get_new_user_tasks_keyboard()
            )

    elif text == "✅ Join Group (₦1000)":
        if await has_completed_task(user_id, "joined_group"):
            await update.message.reply_text(
                "✅ You have already claimed this reward.",
                reply_markup=get_new_user_tasks_keyboard()
            )
        else:
            # Validate group join
            try:
                member = await context.bot.get_chat_member(chat_id=GROUP_USERNAME, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    raise Exception()
            except Exception:
                await update.message.reply_text(
                    f"❌ You must join the group {GROUP_USERNAME} to claim this reward.",
                    reply_markup=get_new_user_tasks_keyboard()
                )
                return
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET completed_tasks = completed_tasks + 1, earning_balance = earning_balance + 1000 WHERE user_id = $1",
                    user_id
                )
            await mark_task_completed(user_id, "joined_group")
            await update.message.reply_text(
                "🎉 Task completed!\n🪙 You earned ₦1000 for joining our group.",
                reply_markup=get_new_user_tasks_keyboard()
            )

    elif text == "🗓️ Daily Tasks":
        await update.message.reply_text(
            "🗓️ Daily Tasks Menu:",
            reply_markup=get_daily_tasks_keyboard()
        )

    elif text == "🎁 Daily Login Reward":
        now = int(time.time())
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_daily_claim, referrals FROM users WHERE user_id = $1", user_id)
            last_claim = row["last_daily_claim"] if row else 0
            referrals = row["referrals"] if row else 0
            reward = 100 + (referrals * 50)
            if now - last_claim >= 86400:  # 24 hours
                await conn.execute(
                    "UPDATE users SET earning_balance = earning_balance + $1, last_daily_claim = $2 WHERE user_id = $3",
                    reward, now, user_id
                )
                await update.message.reply_text(
                    f"🎉 Daily login reward claimed!\n"
                    f"Reward: ₦{reward}\n"
                    f"Come back in 24 hours for your next reward.",
                    reply_markup=get_daily_tasks_keyboard()
                )
            else:
                next_claim = last_claim + 86400
                wait_time = max(0, next_claim - now)
                hours = wait_time // 3600
                minutes = (wait_time % 3600) // 60
                await update.message.reply_text(
                    f"⏳ You have already claimed your daily reward.\n"
                    f"Come back in {hours}h {minutes}m.",
                    reply_markup=get_daily_tasks_keyboard()
                )

    elif text == "📈 Earning History":
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT completed_tasks, earning_balance FROM users WHERE user_id = $1", user_id)
        tasks = row["completed_tasks"] if row else 0
        earning_balance = row["earning_balance"] if row else 0
        await update.message.reply_text(
            f"📈 Earning History:\n"
            f"Tasks Completed: {tasks}\n"
            f"Total Earned: ₦{earning_balance}",
            reply_markup=get_go_back_keyboard()
        )

    elif text == "⬅️ Go Back":
        await update.message.reply_text(
            "🔙 Back to main menu.",
            reply_markup=get_main_keyboard(user_id)
        )

    elif text == "🏠 Main Menu":
        await update.message.reply_text(
            "🏠 Main Menu",
            reply_markup=get_main_keyboard(user_id)
        )

    elif text == "🛠️ Admin Panel":
        await admin_panel(update, context)

    elif text == "👥 User Stats":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.")
            return
        async with db_pool.acquire() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            sums = await conn.fetchrow("SELECT SUM(main_balance) AS main, SUM(reward_balance) AS reward, SUM(earning_balance) AS earning FROM users")
        await update.message.reply_text(
            f"👥 Total users: {user_count}\n"
            f"💰 Main: {sums['main'] or 0} | Reward: {sums['reward'] or 0} | Earning: {sums['earning'] or 0}",
            reply_markup=get_admin_keyboard()
        )

    elif text == "📢 Broadcast":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.")
            return
        context.user_data["broadcast"] = True
        await update.message.reply_text("✍️ Send the message you want to broadcast to all users.", reply_markup=get_admin_keyboard())

    elif context.user_data.get("broadcast"):
        if user_id == ADMIN_ID:
            broadcast_message = text
            context.user_data["broadcast"] = False
            sent = 0
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT user_id FROM users")
            for row in rows:
                try:
                    await context.bot.send_message(row["user_id"], broadcast_message)
                    sent += 1
                except Exception as e:
                    logging.warning(f"Failed to send broadcast to {row['user_id']}: {e}")
                    continue
            await update.message.reply_text(f"✅ Broadcast sent to {sent} users.", reply_markup=get_admin_keyboard())
        else:
            context.user_data["broadcast"] = False

    elif text == "🔍 Search User":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.", reply_markup=get_main_keyboard(user_id))
            return
        context.user_data["admin_action"] = "search_user"
        await update.message.reply_text("🔍 Enter the user ID or name to search:", reply_markup=get_admin_keyboard())

    elif text == "💸 Edit Balance":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.", reply_markup=get_main_keyboard(user_id))
            return
        context.user_data["admin_action"] = "edit_balance_id"
        await update.message.reply_text("💸 Enter the user ID to edit balance:", reply_markup=get_admin_keyboard())

    elif text == "❌ Ban User":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.", reply_markup=get_main_keyboard(user_id))
            return
        context.user_data["admin_action"] = "ban_user"
        await update.message.reply_text("❌ Enter the user ID to ban:", reply_markup=get_admin_keyboard())

    elif text == "📤 Export Users":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.", reply_markup=get_main_keyboard(user_id))
            return
        context.user_data["admin_action"] = "export_choose"
        await update.message.reply_text(
            "📤 Choose export filter:",
            reply_markup=get_export_keyboard()
        )

    elif context.user_data.get("admin_action") == "export_choose":
        if text == "All Users":
            query = "SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users"
            params = ()
            caption = "📤 Exported all users"
        elif text == "By Balance":
            context.user_data["admin_action"] = "export_balance"
            await update.message.reply_text("Enter minimum earning balance for export:", reply_markup=get_export_keyboard())
            return
        elif text == "By Gender":
            context.user_data["admin_action"] = "export_gender"
            await update.message.reply_text("Enter gender to export (Male/Female/Other):", reply_markup=get_export_keyboard())
            return
        elif text == "By Referrals":
            context.user_data["admin_action"] = "export_referrals"
            await update.message.reply_text("Enter minimum referral count for export:", reply_markup=get_export_keyboard())
            return
        elif text == "⬅️ Cancel Export":
            context.user_data["admin_action"] = None
            await update.message.reply_text("❌ Export cancelled.", reply_markup=get_admin_keyboard())
            return
        else:
            await update.message.reply_text("❌ Invalid option.", reply_markup=get_export_keyboard())
            return

        # Export all users
        async with db_pool.acquire() as conn:
            users = await conn.fetch(query)
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "name", "email", "gender", "main_balance", "reward_balance", "earning_balance", "referrals"])
        for user in users:
            writer.writerow([user["user_id"], user["name"], user["email"], user["gender"], user["main_balance"], user["reward_balance"], user["earning_balance"], user["referrals"]])
        output.seek(0)
        await context.bot.send_document(
            chat_id=user_id,
            document=InputFile(output, filename="users.csv"),
            caption=caption
        )
        context.user_data["admin_action"] = None

    elif context.user_data.get("admin_action") == "export_balance":
        try:
            min_balance = int(text)
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid number.", reply_markup=get_export_keyboard())
            return
        query = "SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE earning_balance >= $1"
        async with db_pool.acquire() as conn:
            users = await conn.fetch(query, min_balance)
        caption = f"📤 Exported users with earning balance ≥ {min_balance}"
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "name", "email", "gender", "main_balance", "reward_balance", "earning_balance", "referrals"])
        for user in users:
            writer.writerow([user["user_id"], user["name"], user["email"], user["gender"], user["main_balance"], user["reward_balance"], user["earning_balance"], user["referrals"]])
        output.seek(0)
        await context.bot.send_document(
            chat_id=user_id,
            document=InputFile(output, filename="users_by_earning_balance.csv"),
            caption=caption
        )
        context.user_data["admin_action"] = None

    elif context.user_data.get("admin_action") == "export_gender":
        gender = text.strip().capitalize()
        if gender not in ["Male", "Female", "Other"]:
            await update.message.reply_text("❌ Please enter Male, Female, or Other.", reply_markup=get_export_keyboard())
            return
        query = "SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE gender = $1"
        async with db_pool.acquire() as conn:
            users = await conn.fetch(query, gender)
        caption = f"📤 Exported users with gender: {gender}"
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "name", "email", "gender", "main_balance", "reward_balance", "earning_balance", "referrals"])
        for user in users:
            writer.writerow([user["user_id"], user["name"], user["email"], user["gender"], user["main_balance"], user["reward_balance"], user["earning_balance"], user["referrals"]])
        output.seek(0)
        await context.bot.send_document(
            chat_id=user_id,
            document=InputFile(output, filename=f"users_{gender.lower()}.csv"),
            caption=caption
        )
        context.user_data["admin_action"] = None

    elif context.user_data.get("admin_action") == "export_referrals":
        try:
            min_ref = int(text)
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid number.", reply_markup=get_export_keyboard())
            return
        query = "SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE referrals >= $1"
        async with db_pool.acquire() as conn:
            users = await conn.fetch(query, min_ref)
        caption = f"📤 Exported users with referrals ≥ {min_ref}"
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "name", "email", "gender", "main_balance", "reward_balance", "earning_balance", "referrals"])
        for user in users:
            writer.writerow([user["user_id"], user["name"], user["email"], user["gender"], user["main_balance"], user["reward_balance"], user["earning_balance"], user["referrals"]])
        output.seek(0)
        await context.bot.send_document(
            chat_id=user_id,
            document=InputFile(output, filename="users_by_referrals.csv"),
            caption=caption
        )
        context.user_data["admin_action"] = None

    elif text == "⬅️ Back to Main":
        await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))

    elif text == "💳 Deposit":
        await update.message.reply_text(
            "💳 To deposit, please send your payment to the following account:\n\n"
            "Bank: Example Bank\n"
            "Account Number: 1234567890\n"
            "Account Name: Your Company Name\n\n"
            "After payment, send your proof of payment here and an admin will credit your main balance."
        )

    elif text == "🛎️ Services":
        await update.message.reply_text(
            "🛎️ Services coming soon!",
            reply_markup=get_go_back_keyboard()
        )

conv_handler = ConversationHandler(
    entry_points=[CommandHandler("start", start)],
    states={
        ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name)],
        ASK_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_email)],
        ASK_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_account)],
        CHANGE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_name)],
        CHANGE_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_email)],
    },
    fallbacks=[],
)

# ======================
# Admin Command
# ======================

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ You are not authorized to access this command.")
        return

    async with db_pool.acquire() as conn:
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
    await update.message.reply_text(f"👥 Total registered users: {user_count}")

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ You are not authorized to access this panel.")
        return
    await update.message.reply_text(
        "🛠️ Admin Panel:\nChoose an option below.",
        reply_markup=get_admin_keyboard()
    )

# ======================
# Main Entry
# ======================

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()

    async def on_startup(app):
        global db_pool
        db_pool = await asyncpg.create_pool(POSTGRES_URL)
        await init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(on_startup).build()
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    print("✅ Bot is running...")
    app.run_polling()