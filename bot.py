import asyncio
import logging
import asyncpg
import nest_asyncio
import time
import csv
from io import StringIO
nest_asyncio.apply()
from telegram import Update, ReplyKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler
)

# ======================
# Configuration
# ======================

BOT_TOKEN = "7646819105:AAHMBAwR7SSA5zCnjpiOqHuc5bqIVYfX9xc"
ADMIN_ID = 6784563936  # Replace with your actual Telegram user ID

CHANNEL_USERNAME = "@cashearnify"
GROUP_USERNAME = "@homeofupdatez"

POSTGRES_URL = "postgresql://giveaway_bot_user:ViuIAmXCDCg2wG0mfRSuTVMXPkiGfaiM@dpg-d0ju8o7fte5s7386gtrg-a/giveaway_bot"  # <-- Set your PostgreSQL URL here

ASK_NAME, ASK_EMAIL, ASK_ACCOUNT, CHANGE_NAME, CHANGE_EMAIL = range(5)
ASK_BANK_NAME, ASK_ACCOUNT_NUMBER, ASK_ACCOUNT_NAME, CHOOSE_BALANCE, ASK_WITHDRAW_AMOUNT = range(100, 105)

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

async def init_db():
    conn = await asyncpg.connect(POSTGRES_URL)
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
    await conn.close()

async def has_completed_task(user_id, task_name):
    conn = await asyncpg.connect(POSTGRES_URL)
    row = await conn.fetchrow("SELECT 1 FROM user_tasks WHERE user_id = $1 AND task_name = $2", user_id, task_name)
    await conn.close()
    return row is not None

async def mark_task_completed(user_id, task_name):
    conn = await asyncpg.connect(POSTGRES_URL)
    await conn.execute("INSERT INTO user_tasks (user_id, task_name) VALUES ($1, $2) ON CONFLICT DO NOTHING", user_id, task_name)
    await conn.close()

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
        ["🎁 Daily Login Reward"],
        ["✅ Join Channel (₦1000)"],
        ["✅ Join Group (₦1000)"],
        ["📈 Earning History"],
        ["🗓️ Daily Tasks"],
        ["⬅️ Back"]
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

# ======================
# Bot Handlers
# ======================

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
    conn = await asyncpg.connect(POSTGRES_URL)
    row = await conn.fetchrow("SELECT name FROM users WHERE user_id = $1", user_id)
    await conn.close()

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
    context.user_data["name"] = update.message.text
    await update.message.reply_text("📧 Now enter your email address:")
    return ASK_EMAIL

async def ask_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["email"] = update.message.text
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
    conn = await asyncpg.connect(POSTGRES_URL)
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
    await conn.close()

    await update.message.reply_text(
        f"🎉 Registration complete!{bonus_msg}\n\nName: {name}\nEmail: {email}\nGender: {gender}\n\nUse the buttons below to get started.",
        reply_markup=get_main_keyboard(user_id)
    )
    return ConversationHandler.END

async def changeinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = await asyncpg.connect(POSTGRES_URL)
    row = await conn.fetchrow("SELECT change_count FROM users WHERE user_id = $1", user_id)
    if not row:
        await update.message.reply_text("❌ You are not registered.")
        await conn.close()
        return ConversationHandler.END
    if row["change_count"] >= 1:
        await update.message.reply_text("❌ You can only change your name and email once.")
        await conn.close()
        return ConversationHandler.END
    await conn.close()
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
    conn = await asyncpg.connect(POSTGRES_URL)
    await conn.execute(
        "UPDATE users SET name = $1, email = $2, change_count = change_count + 1 WHERE user_id = $3",
        new_name, new_email, user_id
    )
    await conn.close()
    await update.message.reply_text("✅ Your name and email have been updated.")
    return ConversationHandler.END

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id

    # --- ADMIN ADVANCED FEATURES ---
    if user_id == ADMIN_ID:
        if context.user_data.get("admin_action") == "search_user":
            query = text.strip()
            context.user_data["admin_action"] = None
            conn = await asyncpg.connect(POSTGRES_URL)
            if query.isdigit():
                user = await conn.fetchrow("SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE user_id = $1", int(query))
            else:
                user = await conn.fetchrow("SELECT user_id, name, email, gender, main_balance, reward_balance, earning_balance, referrals FROM users WHERE name ILIKE $1", f"%{query}%")
            await conn.close()
            if user:
                await update.message.reply_text(
                    f"👤 User Info:\nID: {user['user_id']}\nName: {user['name']}\nEmail: {user['email']}\nGender: {user['gender']}\n"
                    f"Main: {user['main_balance']}, Reward: {user['reward_balance']}, Earning: {user['earning_balance']}\nReferrals: {user['referrals']}",
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
                conn = await asyncpg.connect(POSTGRES_URL)
                await conn.execute(f"UPDATE users SET {col} = $1 WHERE user_id = $2", amount, user_id_to_edit)
                await conn.close()
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
                conn = await asyncpg.connect(POSTGRES_URL)
                await conn.execute("DELETE FROM users WHERE user_id = $1", ban_id)
                await conn.execute("DELETE FROM user_tasks WHERE user_id = $1", ban_id)
                await conn.close()
                await update.message.reply_text(f"🚫 User {ban_id} has been banned and removed.", reply_markup=get_admin_keyboard())
            else:
                await update.message.reply_text("❌ Invalid user ID.", reply_markup=get_admin_keyboard())
            context.user_data["admin_action"] = None
            return

    # --- MAIN USER/ADMIN MENU ---
    if text == "👤 Profile":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT completed_tasks, main_balance, reward_balance, earning_balance, referral_balance, name, email, gender FROM users WHERE user_id = $1", user_id)
        await conn.close()
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
            f"💰 Main Balance: {main_balance}\n"
            f"🎁 Reward Balance: {reward_balance}\n"
            f"🪙 Earning Balance: {earning_balance}\n"
            f"👥 Referral Balance: {referral_balance}"
        )

    elif text == "💰 Balance":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT main_balance, reward_balance, earning_balance, referral_balance FROM users WHERE user_id = $1", user_id)
        await conn.close()
        main_balance = row["main_balance"] if row else 0
        reward_balance = row["reward_balance"] if row else 0
        earning_balance = row["earning_balance"] if row else 0
        referral_balance = row["referral_balance"] if row else 0
        await update.message.reply_text(
            f"💰 Main Balance: {main_balance}\n"
            f"🎁 Reward Balance: {reward_balance}\n"
            f"🪙 Earning Balance: {earning_balance}\n"
            f"👥 Referral Balance: {referral_balance}"
        )

    elif text == "🏧 Withdrawal":
        context.user_data["withdraw"] = {}
        await update.message.reply_text("🏦 Enter your Bank Name:")
        context.user_data["withdraw_state"] = ASK_BANK_NAME
        return

    elif context.user_data.get("withdraw_state") == ASK_BANK_NAME:
        context.user_data["withdraw"]["bank_name"] = text
        await update.message.reply_text("🔢 Enter your Account Number:")
        context.user_data["withdraw_state"] = ASK_ACCOUNT_NUMBER
        return

    elif context.user_data.get("withdraw_state") == ASK_ACCOUNT_NUMBER:
        context.user_data["withdraw"]["account_number"] = text
        await update.message.reply_text("👤 Enter your Account Name:")
        context.user_data["withdraw_state"] = ASK_ACCOUNT_NAME
        return

    elif context.user_data.get("withdraw_state") == ASK_ACCOUNT_NAME:
        context.user_data["withdraw"]["account_name"] = text
        # Fetch balances and referrals
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT main_balance, reward_balance, earning_balance, referral_balance, referrals FROM users WHERE user_id = $1", user_id)
        await conn.close()
        main = row["main_balance"] if row else 0
        reward = row["reward_balance"] if row else 0
        earning = row["earning_balance"] if row else 0
        referral = row["referral_balance"] if row else 0
        referrals = row["referrals"] if row else 0
        keyboard = [
            [f"Main Balance (₦{main})"],
            [f"Reward Balance (₦{reward})"],
            [f"Earning Balance (₦{earning})"],
            [f"Referral Balance (₦{referral})"]
        ]
        await update.message.reply_text(
            "💸 Which balance do you want to withdraw from?\n\n"
            "• Main Balance: Withdraw anytime, any amount.\n"
            "• Reward Balance: Withdraw anytime, any amount **after 10 referrals**.\n"
            "• Earning Balance: Withdraw only if you have **10 referrals** and minimum ₦45,000.\n"
            "• Referral Balance: Withdraw only if you have **10 referrals** and minimum ₦20,000.",
            reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        )
        context.user_data["withdraw_state"] = CHOOSE_BALANCE
        context.user_data["withdraw"]["referrals"] = referrals
        context.user_data["withdraw"]["main"] = main
        context.user_data["withdraw"]["reward"] = reward
        context.user_data["withdraw"]["earning"] = earning
        context.user_data["withdraw"]["referral"] = referral
        return

    elif context.user_data.get("withdraw_state") == CHOOSE_BALANCE:
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
            await update.message.reply_text("❌ Please select a valid balance option.")
            return

        # Show criteria prompt
        if context.user_data["withdraw"]["balance_label"] == "Main Balance":
            await update.message.reply_text("✅ You can withdraw any amount from your Main Balance at any time.")
        elif context.user_data["withdraw"]["balance_label"] == "Reward Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Reward Balance after referring 10 people."
            )
        elif context.user_data["withdraw"]["balance_label"] == "Earning Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Earning Balance after referring 10 people and the minimum withdrawal is ₦45,000."
            )
        elif context.user_data["withdraw"]["balance_label"] == "Referral Balance":
            await update.message.reply_text(
                "ℹ️ You can only withdraw from Referral Balance after referring 10 people and the minimum withdrawal is ₦20,000."
            )

        await update.message.reply_text(
            f"Enter the amount you want to withdraw from your {context.user_data['withdraw']['balance_label']}:"
        )
        context.user_data["withdraw_state"] = ASK_WITHDRAW_AMOUNT
        return

    elif context.user_data.get("withdraw_state") == ASK_WITHDRAW_AMOUNT:
        try:
            amount = int(text)
        except ValueError:
            await update.message.reply_text("❌ Please enter a valid amount.")
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
            await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Reward Balance.")
            return
        if balance_type == "earning_balance":
            if referrals < 10:
                await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Earning Balance.")
                return
            if amount < 45000:
                await update.message.reply_text("❌ Minimum withdrawal from Earning Balance is ₦45,000.")
                return
        if balance_type == "referral_balance":
            if referrals < 10:
                await update.message.reply_text("❌ You need at least 10 referrals to withdraw from Referral Balance.")
                return
            if amount < 20000:
                await update.message.reply_text("❌ Minimum withdrawal from Referral Balance is ₦20,000.")
                return

        # Check sufficient balance
        if balance_type == "main_balance" and amount > main:
            await update.message.reply_text("❌ Insufficient Main Balance.")
            return
        if balance_type == "reward_balance" and amount > reward:
            await update.message.reply_text("❌ Insufficient Reward Balance.")
            return
        if balance_type == "earning_balance" and amount > earning:
            await update.message.reply_text("❌ Insufficient Earning Balance.")
            return
        if balance_type == "referral_balance" and amount > referral:
            await update.message.reply_text("❌ Insufficient Referral Balance.")
            return

        # Save withdrawal request to DB
        conn = await asyncpg.connect(POSTGRES_URL)
        await conn.execute(
            "INSERT INTO withdrawals (user_id, bank_name, account_number, account_name, balance_type, amount) VALUES ($1, $2, $3, $4, $5, $6)",
            user_id, details['bank_name'], details['account_number'], details['account_name'], balance_type, amount
        )
        await conn.close()

        # Notify user
        await update.message.reply_text(
            f"✅ Withdrawal Request:\n"
            f"Bank: {details['bank_name']}\n"
            f"Account Number: {details['account_number']}\n"
            f"Account Name: {details['account_name']}\n"
            f"Balance: {balance_label}\n"
            f"Amount: ₦{amount}\n\n"
            "Your request has been received. An admin will process it soon.",
            reply_markup=get_main_keyboard(user_id)
        )

        # Notify admin
        admin_msg = (
            f"💸 New Withdrawal Request\n"
            f"User ID: {user_id}\n"
            f"Bank: {details['bank_name']}\n"
            f"Account Number: {details['account_number']}\n"
            f"Account Name: {details['account_name']}\n"
            f"Balance: {balance_label}\n"
            f"Amount: ₦{amount}\n"
            f"Referrals: {referrals}"
        )
        await context.bot.send_message(ADMIN_ID, admin_msg)

        context.user_data["withdraw_state"] = None
        context.user_data["withdraw"] = {}
        return

    # --- MAIN USER/ADMIN MENU ---
    elif text == "👤 Profile":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT completed_tasks, main_balance, reward_balance, earning_balance, referral_balance, name, email, gender FROM users WHERE user_id = $1", user_id)
        await conn.close()
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
            f"💰 Main Balance: {main_balance}\n"
            f"🎁 Reward Balance: {reward_balance}\n"
            f"🪙 Earning Balance: {earning_balance}\n"
            f"👥 Referral Balance: {referral_balance}"
        )

    elif text == "📝 Tasks":
        await update.message.reply_text(
            "📝 Available Tasks:\n"
            "🎁 Daily Login Reward (₦100)\n"
            f"✅ Join our Telegram channel: {CHANNEL_USERNAME} (₦1000)\n"
            f"✅ Join our Telegram group: {GROUP_USERNAME} (₦1000)\n"
            "📈 Earning History\n"
            "🗓️ Daily Tasks (updated every day)\n\n"
            "Press a button below after completing a task to claim your reward.",
            reply_markup=get_tasks_keyboard()
        )

    elif text == "💰 Balance":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT main_balance, reward_balance, earning_balance FROM users WHERE user_id = $1", user_id)
        await conn.close()
        main_balance = row["main_balance"] if row else 0
        reward_balance = row["reward_balance"] if row else 0
        earning_balance = row["earning_balance"] if row else 0
        await update.message.reply_text(
            f"💰 Main Balance: {main_balance}\n"
            f"🎁 Reward Balance: {reward_balance}\n"
            f"🪙 Earning Balance: {earning_balance}"
        )

    elif text == "🔗 Referrals":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT referrals FROM users WHERE user_id = $1", user_id)
        referrals_count = row["referrals"] if row else 0
        bot_username = (await context.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start={user_id}"
        referred_rows = await conn.fetch(
            "SELECT referred_id, reward_amount, referred_at FROM referrals WHERE referrer_id = $1", user_id
        )
        await conn.close()
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

    elif text == "🏧 Withdrawal":
        await update.message.reply_text("🏧 Withdrawal options coming soon!")

    elif text == "📈 Earning History":
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT completed_tasks, earning_balance FROM users WHERE user_id = $1", user_id)
        await conn.close()
        tasks = row["completed_tasks"] if row else 0
        earning_balance = row["earning_balance"] if row else 0
        await update.message.reply_text(
            f"📈 Earning History:\n"
            f"Tasks Completed: {tasks}\n"
            f"Total Earned: {earning_balance} coins"
        )

    elif text == "🎁 Daily Login Reward":
        now = int(time.time())
        conn = await asyncpg.connect(POSTGRES_URL)
        row = await conn.fetchrow("SELECT last_daily_claim, referrals FROM users WHERE user_id = $1", user_id)
        last_claim = row["last_daily_claim"] if row else 0
        referrals = row["referrals"] if row else 0
        reward = 100 + (referrals * 50)
        if now - last_claim >= 86400:  # 24 hours
            await conn.execute(
                "UPDATE users SET earning_balance = earning_balance + $1, last_daily_claim = $2 WHERE user_id = $3",
                reward, now, user_id
        )
            await conn.close()
            await update.message.reply_text(
                f"🎉 Daily login reward claimed!\n"
                f"Reward: ₦{reward}\n"
                f"Come back in 24 hours for your next reward.",
                reply_markup=get_main_keyboard(user_id)
            )
        else:
            await conn.close()
            next_claim = last_claim + 86400
            wait_time = max(0, next_claim - now)
            hours = wait_time // 3600
            minutes = (wait_time % 3600) // 60
            await update.message.reply_text(
                f"⏳ You have already claimed your daily reward.\n"
                f"Come back in {hours}h {minutes}m.",
                reply_markup=get_main_keyboard(user_id)
        )
    elif text == "✅ Join Channel (₦1000)":
        if await has_completed_task(user_id, "joined_channel"):
            await update.message.reply_text("✅ You have already claimed this reward.", reply_markup=get_main_keyboard(user_id))
        else:
            # Validate channel join
            try:
                member = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    raise Exception()
            except Exception:
                await update.message.reply_text(f"❌ You must join the channel {CHANNEL_USERNAME} to claim this reward.", reply_markup=get_main_keyboard(user_id))
                return
            conn = await asyncpg.connect(POSTGRES_URL)
            await conn.execute(
                "UPDATE users SET completed_tasks = completed_tasks + 1, earning_balance = earning_balance + 1000 WHERE user_id = $1",
                user_id
            )
            await conn.close()
            await mark_task_completed(user_id, "joined_channel")
            await update.message.reply_text("🎉 Task completed!\n🪙 You earned ₦1000 for joining our channel.", reply_markup=get_main_keyboard(user_id))

    elif text == "✅ Join Group (₦1000)":
        if await has_completed_task(user_id, "joined_group"):
            await update.message.reply_text("✅ You have already claimed this reward.", reply_markup=get_main_keyboard(user_id))
        else:
            # Validate group join
            try:
                member = await context.bot.get_chat_member(chat_id=GROUP_USERNAME, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    raise Exception()
            except Exception:
                await update.message.reply_text(f"❌ You must join the group {GROUP_USERNAME} to claim this reward.", reply_markup=get_main_keyboard(user_id))
                return
            conn = await asyncpg.connect(POSTGRES_URL)
            await conn.execute(
                "UPDATE users SET completed_tasks = completed_tasks + 1, earning_balance = earning_balance + 1000 WHERE user_id = $1",
                user_id
            )
            await conn.close()
            await mark_task_completed(user_id, "joined_group")
            await update.message.reply_text("🎉 Task completed!\n🪙 You earned ₦1000 for joining our group.", reply_markup=get_main_keyboard(user_id))

    elif text == "🗓️ Daily Tasks":
        await update.message.reply_text(
            "🗓️ Here are today's special daily tasks:\n"
            "👉 [Update this section daily with your new tasks!]",
            reply_markup=get_tasks_keyboard()
        )

    elif text == "⬅️ Back":
        await update.message.reply_text("🔙 Back to main menu.", reply_markup=get_main_keyboard(user_id))

    elif text == "🛠️ Admin Panel":
        await admin_panel(update, context)

    elif text == "👥 User Stats":
        if user_id != ADMIN_ID:
            await update.message.reply_text("⛔ You are not authorized.")
            return
        conn = await asyncpg.connect(POSTGRES_URL)
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        sums = await conn.fetchrow("SELECT SUM(main_balance) AS main, SUM(reward_balance) AS reward, SUM(earning_balance) AS earning FROM users")
        await conn.close()
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
            conn = await asyncpg.connect(POSTGRES_URL)
            rows = await conn.fetch("SELECT user_id FROM users")
            await conn.close()
            for row in rows:
                try:
                    await context.bot.send_message(row["user_id"], broadcast_message)
                    sent += 1
                except Exception:
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
        conn = await asyncpg.connect(POSTGRES_URL)
        users = await conn.fetch(query)
        await conn.close()
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
        conn = await asyncpg.connect(POSTGRES_URL)
        users = await conn.fetch(query, min_balance)
        await conn.close()
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
        conn = await asyncpg.connect(POSTGRES_URL)
        users = await conn.fetch(query, gender)
        await conn.close()
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
        conn = await asyncpg.connect(POSTGRES_URL)
        users = await conn.fetch(query, min_ref)
        await conn.close()
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
            "🛎️ Our Services:\n"
            "- Service 1\n"
            "- Service 2\n"
            "- Service 3\n"
            "Contact admin for more info."
        )

# ======================
# Admin Command
# ======================

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ You are not authorized to access this command.")
        return

    conn = await asyncpg.connect(POSTGRES_URL)
    user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
    await conn.close()
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

async def main():
    await init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    registration_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("joined", joined)],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name)],
            ASK_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_email)],
            ASK_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_account)],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    changeinfo_conv = ConversationHandler(
        entry_points=[CommandHandler("changeinfo", changeinfo)],
        states={
            CHANGE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_name)],
            CHANGE_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_email)],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    app.add_handler(registration_conv)
    app.add_handler(changeinfo_conv)
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("adminpanel", admin_panel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))

    print("✅ Bot is running...")
    await app.run_polling()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
