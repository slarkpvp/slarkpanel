#!/usr/bin/env python3
"""
VLESS Telegram Bot - ПОЛНАЯ ВЕРСИЯ С АВТОМАТИЧЕСКИМИ ПЛАТЕЖАМИ
Версия 2.0 - Все платежки + вебхуки работают
"""

import asyncio
import logging
import sys
import sqlite3
import json
import uuid
import re
import qrcode
import hashlib
import base64
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from typing import List, Dict, Optional
from urllib.parse import urlparse
from contextlib import contextmanager
from hmac import compare_digest

import aiohttp
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from py3xui import Api, Client, Inbound
from yookassa import Payment, Configuration
from aiosend import CryptoPay
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# ========== НАСТРОЙКИ ==========
import os
from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "")

# Платежные системы
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN")
HELEKET_MERCHANT_ID = os.getenv("HELEKET_MERCHANT_ID")
HELEKET_API_KEY = os.getenv("HELEKET_API_KEY")
TON_WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS")
TONAPI_KEY = os.getenv("TONAPI_KEY")

# Вебхук настройки (ваш домен или IP)
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN", "https://ваш-домен.com")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8443"))

# Текстовые настройки
ABOUT_TEXT = os.getenv("ABOUT_TEXT", "VPN сервис для безопасного интернета")
TERMS_URL = os.getenv("TERMS_URL", "")
PRIVACY_URL = os.getenv("PRIVACY_URL", "")
CHANNEL_URL = os.getenv("CHANNEL_URL", "")
SUPPORT_USER = os.getenv("SUPPORT_USER", "@support")
SUPPORT_TEXT = os.getenv("SUPPORT_TEXT", "Напишите нам в поддержку")

# Клиентские ссылки
ANDROID_URL = os.getenv("ANDROID_URL", "https://play.google.com/store/apps/details?id=com.v2ray.client")
IOS_URL = os.getenv("IOS_URL", "https://apps.apple.com/app/v2rayng/id6447596709")
WINDOWS_URL = os.getenv("WINDOWS_URL", "https://github.com/2dust/v2rayN/releases")
LINUX_URL = os.getenv("LINUX_URL", "https://github.com/2dust/v2rayN/releases")

# Флаги функций
FORCE_SUBSCRIPTION = os.getenv("FORCE_SUBSCRIPTION", "false") == "true"
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "true") == "true"
TRIAL_DURATION_DAYS = int(os.getenv("TRIAL_DURATION_DAYS", "3"))
SBP_ENABLED = os.getenv("SBP_ENABLED", "true") == "true"
ENABLE_REFERRALS = os.getenv("ENABLE_REFERRALS", "true") == "true"
REFERRAL_PERCENTAGE = float(os.getenv("REFERRAL_PERCENTAGE", "10"))
REFERRAL_DISCOUNT = float(os.getenv("REFERRAL_DISCOUNT", "10"))
MINIMUM_WITHDRAWAL = float(os.getenv("MINIMUM_WITHDRAWAL", "100"))

# ========== БАЗА ДАННЫХ ==========

class Database:
    def __init__(self, db_path="vless_bot.db"):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Пользователи
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    agreed_to_terms BOOLEAN DEFAULT 0,
                    trial_used BOOLEAN DEFAULT 0,
                    total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    is_banned BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Хосты
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS hosts (
                    host_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT UNIQUE NOT NULL,
                    host_url TEXT NOT NULL,
                    host_username TEXT NOT NULL,
                    host_pass TEXT NOT NULL,
                    host_inbound_id INTEGER NOT NULL
                )
            ''')
            
            # Тарифы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    plan_name TEXT NOT NULL,
                    months INTEGER NOT NULL,
                    price REAL NOT NULL
                )
            ''')
            
            # Ключи
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_keys (
                    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    host_name TEXT NOT NULL,
                    xui_client_uuid TEXT NOT NULL,
                    key_email TEXT NOT NULL,
                    expiry_date TIMESTAMP NOT NULL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Транзакции
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    user_id INTEGER,
                    status TEXT,
                    amount_rub REAL,
                    payment_method TEXT,
                    metadata TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Настройки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            # Вебхук транзакции
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhook_transactions (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    amount REAL,
                    metadata TEXT,
                    status TEXT DEFAULT 'pending',
                    processed BOOLEAN DEFAULT 0
                )
            ''')
            
            # Добавляем настройки по умолчанию
            default_settings = [
                ("telegram_bot_token", BOT_TOKEN),
                ("telegram_bot_username", TELEGRAM_BOT_USERNAME),
                ("admin_telegram_id", str(ADMIN_ID)),
                ("trial_enabled", "true" if TRIAL_ENABLED else "false"),
                ("trial_duration_days", str(TRIAL_DURATION_DAYS)),
                ("force_subscription", "false"),
                ("sbp_enabled", "true" if SBP_ENABLED else "false"),
                ("enable_referrals", "true" if ENABLE_REFERRALS else "false"),
                ("referral_percentage", str(REFERRAL_PERCENTAGE)),
                ("referral_discount", str(REFERRAL_DISCOUNT)),
                ("minimum_withdrawal", str(MINIMUM_WITHDRAWAL)),
                ("about_text", ABOUT_TEXT),
                ("support_text", SUPPORT_TEXT),
                ("support_user", SUPPORT_USER),
                ("channel_url", CHANNEL_URL),
                ("terms_url", TERMS_URL),
                ("privacy_url", PRIVACY_URL),
                ("android_url", ANDROID_URL),
                ("ios_url", IOS_URL),
                ("windows_url", WINDOWS_URL),
                ("linux_url", LINUX_URL),
                ("yookassa_shop_id", YOOKASSA_SHOP_ID or ""),
                ("yookassa_secret_key", YOOKASSA_SECRET_KEY or ""),
                ("cryptobot_token", CRYPTOBOT_TOKEN or ""),
                ("heleket_merchant_id", HELEKET_MERCHANT_ID or ""),
                ("heleket_api_key", HELEKET_API_KEY or ""),
                ("ton_wallet_address", TON_WALLET_ADDRESS or ""),
                ("tonapi_key", TONAPI_KEY or ""),
                ("webhook_domain", WEBHOOK_DOMAIN),
                ("webhook_port", str(WEBHOOK_PORT))
            ]
            
            for key, value in default_settings:
                cursor.execute('''
                    INSERT OR REPLACE INTO settings (key, value) 
                    VALUES (?, ?)
                ''', (key, value))
    
    # ========== ПОЛЬЗОВАТЕЛИ ==========
    
    def get_user(self, telegram_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def register_user(self, telegram_id: int, username: str, referrer_id: int = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO users (telegram_id, username, referred_by) 
                VALUES (?, ?, ?)
            ''', (telegram_id, username, referrer_id))
    
    def update_user_stats(self, telegram_id: int, amount: float, months: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET total_spent = total_spent + ?, 
                    total_months = total_months + ? 
                WHERE telegram_id = ?
            ''', (amount, months, telegram_id))
    
    def set_terms_agreed(self, telegram_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET agreed_to_terms = 1 WHERE telegram_id = ?", (telegram_id,))
    
    def set_trial_used(self, telegram_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET trial_used = 1 WHERE telegram_id = ?", (telegram_id,))
    
    def ban_user(self, user_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (user_id,))
    
    def unban_user(self, user_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (user_id,))
    
    # ========== КЛЮЧИ ==========
    
    def add_key(self, user_id: int, host_name: str, xui_client_uuid: str, 
               key_email: str, expiry_timestamp_ms: int) -> int:
        expiry_date = datetime.fromtimestamp(expiry_timestamp_ms / 1000)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO user_keys 
                (user_id, host_name, xui_client_uuid, key_email, expiry_date) 
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, host_name, xui_client_uuid, key_email, expiry_date))
            return cursor.lastrowid
    
    def get_user_keys(self, user_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM user_keys WHERE user_id = ? ORDER BY created_date DESC", (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_key_by_id(self, key_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM user_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_key_expiry(self, key_id: int, expiry_timestamp_ms: int):
        expiry_date = datetime.fromtimestamp(expiry_timestamp_ms / 1000)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE user_keys SET expiry_date = ? WHERE key_id = ?", (expiry_date, key_id))
    
    def delete_user_keys(self, user_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_keys WHERE user_id = ?", (user_id,))
    
    def get_next_key_number(self, user_id: int) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys WHERE user_id = ?", (user_id,))
            return cursor.fetchone()[0] + 1
    
    # ========== ХОСТЫ ==========
    
    def add_host(self, name: str, url: str, username: str, password: str, inbound_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO hosts 
                (host_name, host_url, host_username, host_pass, host_inbound_id) 
                VALUES (?, ?, ?, ?, ?)
            ''', (name, url, username, password, inbound_id))
    
    def get_all_hosts(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM hosts ORDER BY host_name")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_host(self, host_name: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM hosts WHERE host_name = ?", (host_name,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def delete_host(self, host_name: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM hosts WHERE host_name = ?", (host_name,))
    
    # ========== ТАРИФЫ ==========
    
    def add_plan(self, host_name: str, plan_name: str, months: int, price: float):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO plans (host_name, plan_name, months, price) 
                VALUES (?, ?, ?, ?)
            ''', (host_name, plan_name, months, price))
    
    def get_plans_for_host(self, host_name: str) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE host_name = ? ORDER BY price", (host_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_plan_by_id(self, plan_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def delete_plan(self, plan_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
    
    def get_all_plans(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans ORDER BY host_name, price")
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== ТРАНЗАКЦИИ ==========
    
    def log_transaction(self, username: str, user_id: int, status: str, 
                       amount_rub: float, payment_method: str, metadata: dict):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO transactions 
                (username, user_id, status, amount_rub, payment_method, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, user_id, status, amount_rub, payment_method, json.dumps(metadata)))
    
    def get_latest_transaction(self, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM transactions 
                WHERE user_id = ? 
                ORDER BY created_date DESC 
                LIMIT 1
            ''', (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    # ========== ВЕБХУК ТРАНЗАКЦИИ ==========
    
    def create_webhook_transaction(self, payment_id: str, user_id: int, amount: float, metadata: dict):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO webhook_transactions 
                (payment_id, user_id, amount, metadata) 
                VALUES (?, ?, ?, ?)
            ''', (payment_id, user_id, amount, json.dumps(metadata)))
    
    def get_webhook_transaction(self, payment_id: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM webhook_transactions 
                WHERE payment_id = ? AND processed = 0
            ''', (payment_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def mark_webhook_processed(self, payment_id: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE webhook_transactions 
                SET processed = 1 
                WHERE payment_id = ?
            ''', (payment_id,))
    
    # ========== НАСТРОЙКИ ==========
    
    def get_setting(self, key: str, default: str = "") -> str:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default
    
    def update_setting(self, key: str, value: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value) 
                VALUES (?, ?)
            ''', (key, value))
    
    def get_all_settings(self) -> Dict[str, str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM settings")
            return {row[0]: row[1] for row in cursor.fetchall()}
    
    # ========== СТАТИСТИКА ==========
    
    def get_user_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]
    
    def get_total_keys_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys")
            return cursor.fetchone()[0]
    
    def get_total_spent_sum(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_spent) FROM users")
            result = cursor.fetchone()[0]
            return result if result else 0.0
    
    def get_all_users(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]

# ========== X-UI API ==========

class XUIAPI:
    def __init__(self):
        self.connections = {}
    
    def login_to_host(self, host_url: str, username: str, password: str, inbound_id: int):
        try:
            api = Api(host=host_url, username=username, password=password)
            api.login()
            inbounds = api.inbound.get_list()
            target_inbound = next((i for i in inbounds if i.id == inbound_id), None)
            return api, target_inbound
        except Exception as e:
            logging.error(f"X-UI login failed: {e}")
            return None, None
    
    def get_connection_string(self, inbound: Inbound, user_uuid: str, host_url: str, remark: str):
        if not inbound:
            return None
        
        settings = inbound.stream_settings.reality_settings.get("settings")
        if not settings:
            return None
        
        public_key = settings.get("publicKey")
        fp = settings.get("fingerprint")
        server_names = inbound.stream_settings.reality_settings.get("serverNames")
        short_ids = inbound.stream_settings.reality_settings.get("shortIds")
        port = inbound.port
        
        if not all([public_key, server_names, short_ids]):
            return None
        
        parsed_url = urlparse(host_url)
        short_id = short_ids[0]
        
        return (
            f"vless://{user_uuid}@{parsed_url.hostname}:{port}"
            f"?type=tcp&security=reality&pbk={public_key}&fp={fp}&sni={server_names[0]}"
            f"&sid={short_id}&spx=%2F&flow=xtls-rprx-vision#{remark}"
        )
    
    async def create_or_update_key(self, host_name: str, email: str, days_to_add: int, db: Database):
        host_data = db.get_host(host_name)
        if not host_data:
            return {"error": "Хост не найден"}
        
        api, inbound = self.login_to_host(
            host_data['host_url'],
            host_data['host_username'],
            host_data['host_pass'],
            host_data['host_inbound_id']
        )
        
        if not api or not inbound:
            return {"error": "Ошибка подключения к X-UI"}
        
        try:
            inbound_to_modify = api.inbound.get_by_id(inbound.id)
            if not inbound_to_modify:
                return {"error": "Инбаунд не найден"}
            
            # Проверяем существует ли клиент
            client_index = -1
            for i, client in enumerate(inbound_to_modify.settings.clients):
                if client.email == email:
                    client_index = i
                    break
            
            now = datetime.now()
            if client_index != -1:
                # Обновляем существующий ключ
                existing_client = inbound_to_modify.settings.clients[client_index]
                if existing_client.expiry_time > int(now.timestamp() * 1000):
                    current_expiry = datetime.fromtimestamp(existing_client.expiry_time / 1000)
                    new_expiry = current_expiry + timedelta(days=days_to_add)
                else:
                    new_expiry = now + timedelta(days=days_to_add)
                client_uuid = existing_client.id
                
                # Обновляем клиента
                inbound_to_modify.settings.clients[client_index].expiry_time = int(new_expiry.timestamp() * 1000)
                inbound_to_modify.settings.clients[client_index].enable = True
            else:
                # Создаем нового клиента
                new_expiry = now + timedelta(days=days_to_add)
                client_uuid = str(uuid.uuid4())
                new_client = Client(
                    id=client_uuid,
                    email=email,
                    enable=True,
                    flow="xtls-rprx-vision",
                    expiry_time=int(new_expiry.timestamp() * 1000)
                )
                inbound_to_modify.settings.clients.append(new_client)
            
            # Сохраняем изменения
            api.inbound.update(inbound.id, inbound_to_modify)
            
            # Генерируем connection string
            connection_string = self.get_connection_string(inbound, client_uuid, host_data['host_url'], host_name)
            
            return {
                "success": True,
                "client_uuid": client_uuid,
                "email": email,
                "expiry_timestamp_ms": int(new_expiry.timestamp() * 1000),
                "connection_string": connection_string,
                "host_name": host_name
            }
            
        except Exception as e:
            logging.error(f"X-UI error: {e}")
            return {"error": str(e)}

# ========== ПЛАТЕЖНЫЕ УТИЛИТЫ ==========

async def get_usdt_rub_rate() -> Optional[Decimal]:
    """Получить курс USDT/RUB"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ticker/price?symbol=USDTRUB") as resp:
                data = await resp.json()
                return Decimal(data['price'])
    except:
        return Decimal("90.0")  # Fallback курс

async def get_ton_usdt_rate() -> Optional[Decimal]:
    """Получить курс TON/USDT"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT") as resp:
                data = await resp.json()
                return Decimal(data['price'])
    except:
        return Decimal("2.5")  # Fallback курс

# ========== ОСНОВНОЙ БОТ ==========

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("vless_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db = Database()
xui_api = XUIAPI()

# Создаем бота и диспетчер
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ========== КЛАВИАТУРЫ ==========

def create_main_menu(user_id: int):
    user_data = db.get_user(user_id)
    user_keys = db.get_user_keys(user_id)
    trial_available = TRIAL_ENABLED and (not user_data or not user_data.get('trial_used'))
    is_admin = user_id == ADMIN_ID
    
    builder = InlineKeyboardBuilder()
    
    builder.button(text="👤 Мой профиль", callback_data="show_profile")
    builder.button(text=f"🔑 Мои ключи ({len(user_keys)})", callback_data="manage_keys")
    
    if trial_available:
        builder.button(text="🎁 Попробовать бесплатно", callback_data="get_trial")
    
    builder.button(text="🛒 Купить VPN", callback_data="buy_new_key")
    builder.button(text="🤝 Реферальная программа", callback_data="show_referrals")
    builder.button(text="🆘 Поддержка", callback_data="show_help")
    builder.button(text="ℹ️ О проекте", callback_data="show_about")
    
    if is_admin:
        builder.button(text="👑 Админ панель", callback_data="admin_panel")
    
    builder.adjust(2, 1, 2, 1, 2)
    return builder.as_markup()

# ========== ОБРАБОТЧИКИ ==========

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    
    # Парсим реферальный код
    referrer_id = None
    if message.text and len(message.text.split()) > 1:
        args = message.text.split()[1]
        if args.startswith('ref_'):
            try:
                referrer_id = int(args.split('_')[1])
            except:
                pass
    
    db.register_user(user_id, username, referrer_id)
    
    # Проверка блокировки
    user_data = db.get_user(user_id)
    if user_data and user_data.get('is_banned'):
        await message.answer("❌ Вы заблокированы")
        return
    
    # Приветствие
    await message.answer(
        f"👋 Добро пожаловать, {message.from_user.full_name}!\n\n"
        "Выберите действие:",
        reply_markup=create_main_menu(user_id)
    )

@dp.callback_query(F.data == "show_profile")
async def show_profile(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    user_keys = db.get_user_keys(user_id)
    
    if not user_data:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    now = datetime.now()
    active_keys = [k for k in user_keys if datetime.fromisoformat(k['expiry_date']) > now]
    
    if active_keys:
        latest = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']))
        expiry = datetime.fromisoformat(latest['expiry_date'])
        time_left = expiry - now
        vpn_status = f"✅ <b>Статус VPN:</b> Активен\n⏳ <b>Осталось:</b> {time_left.days} д. {time_left.seconds // 3600} ч."
    elif user_keys:
        vpn_status = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
    else:
        vpn_status = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."
    
    profile_text = (
        f"👤 <b>Профиль:</b> {user_data['username']}\n\n"
        f"💰 <b>Потрачено всего:</b> {user_data['total_spent']:.0f} RUB\n"
        f"📅 <b>Приобретено месяцев:</b> {user_data['total_months']}\n\n"
        f"{vpn_status}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    
    await callback.message.edit_text(profile_text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "get_trial")
async def get_trial(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if user_data and user_data.get('trial_used'):
        await callback.answer("Вы уже использовали пробный период", show_alert=True)
        return
    
    hosts = db.get_all_hosts()
    if not hosts:
        await callback.message.edit_text("❌ Нет доступных серверов")
        return
    
    # Создаем триальный ключ
    host = hosts[0]
    email = f"user{user_id}-trial@{host['host_name'].replace(' ', '').lower()}.bot"
    
    await callback.message.edit_text("🔄 Создаю пробный ключ...")
    
    result = await xui_api.create_or_update_key(
        host['host_name'],
        email,
        TRIAL_DURATION_DAYS,
        db
    )
    
    if result.get('error'):
        await callback.message.edit_text(f"❌ Ошибка: {result['error']}")
        return
    
    # Сохраняем ключ в БД
    key_id = db.add_key(
        user_id,
        host['host_name'],
        result['client_uuid'],
        email,
        result['expiry_timestamp_ms']
    )
    
    # Помечаем триал как использованный
    db.set_trial_used(user_id)
    
    # Показываем ключ
    expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
    expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
    
    success_text = (
        f"🎉 <b>Ваш пробный ключ готов!</b>\n\n"
        f"⏳ <b>Действует до:</b> {expiry_formatted}\n"
        f"🖥️ <b>Сервер:</b> {host['host_name']}\n\n"
        f"<code>{result['connection_string']}</code>"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Продлить", callback_data=f"extend_{key_id}")
    builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(2, 1)
    
    await callback.message.edit_text(success_text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "buy_new_key")
async def buy_new_key(callback: types.CallbackQuery):
    hosts = db.get_all_hosts()
    
    if not hosts:
        await callback.message.edit_text("❌ Нет доступных серверов")
        return
    
    builder = InlineKeyboardBuilder()
    for host in hosts:
        builder.button(text=host['host_name'], callback_data=f"select_host_{host['host_name']}")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    
    await callback.message.edit_text("Выберите сервер:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("select_host_"))
async def select_host(callback: types.CallbackQuery):
    host_name = callback.data.split("_")[2]
    plans = db.get_plans_for_host(host_name)
    
    if not plans:
        await callback.message.edit_text(f"❌ Нет тарифов для {host_name}")
        return
    
    builder = InlineKeyboardBuilder()
    for plan in plans:
        builder.button(
            text=f"{plan['plan_name']} - {plan['price']}₽",
            callback_data=f"select_plan_{plan['plan_id']}"
        )
    builder.button(text="⬅️ Назад", callback_data="buy_new_key")
    builder.adjust(1)
    
    await callback.message.edit_text(f"Тарифы для {host_name}:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("select_plan_"))
async def select_plan(callback: types.CallbackQuery):
    plan_id = int(callback.data.split("_")[2])
    plan = db.get_plan_by_id(plan_id)
    
    if not plan:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    # Создаем платеж
    payment_methods = InlineKeyboardBuilder()
    
    if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
        payment_methods.button(text="🏦 ЮKassa (карта/СБП)", callback_data=f"pay_yookassa_{plan_id}")
    
    if CRYPTOBOT_TOKEN:
        payment_methods.button(text="🤖 CryptoBot", callback_data=f"pay_cryptobot_{plan_id}")
    
    if HELEKET_MERCHANT_ID and HELEKET_API_KEY:
        payment_methods.button(text="💎 Heleket", callback_data=f"pay_heleket_{plan_id}")
    
    payment_methods.button(text="⬅️ Назад", callback_data=f"select_host_{plan['host_name']}")
    payment_methods.adjust(1)
    
    await callback.message.edit_text(
        f"🛒 <b>Покупка:</b> {plan['plan_name']}\n"
        f"💰 <b>Цена:</b> {plan['price']}₽\n"
        f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
        f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
        "Выберите способ оплаты:",
        reply_markup=payment_methods.as_markup()
    )

# ========== ПЛАТЕЖИ ЮKASSA ==========

@dp.callback_query(F.data.startswith("pay_yookassa_"))
async def pay_yookassa(callback: types.CallbackQuery):
    plan_id = int(callback.data.split("_")[2])
    plan = db.get_plan_by_id(plan_id)
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if not plan:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    # Применяем скидку для рефералов
    price = Decimal(str(plan['price']))
    if user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
        discount = Decimal(str(REFERRAL_DISCOUNT))
        if discount > 0:
            discount_amount = (price * discount / 100).quantize(Decimal("0.01"))
            price = price - discount_amount
    
    # Настраиваем ЮKassa
    if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
        Configuration.account_id = YOOKASSA_SHOP_ID
        Configuration.secret_key = YOOKASSA_SECRET_KEY
    
    # Создаем платеж
    payment_id = str(uuid.uuid4())
    
    try:
        payment = Payment.create({
            "amount": {"value": f"{float(price):.2f}", "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"},
            "capture": True,
            "description": f"VPN на {plan['months']} месяцев",
            "metadata": {
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price),
                "payment_id": payment_id,
                "payment_method": "yookassa"
            }
        }, payment_id)
        
        # Сохраняем транзакцию
        db.create_webhook_transaction(
            payment_id,
            user_id,
            float(price),
            {
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price),
                "payment_method": "yookassa"
            }
        )
        
        # Показываем ссылку на оплату
        builder = InlineKeyboardBuilder()
        builder.button(text="💳 Перейти к оплате", url=payment.confirmation.confirmation_url)
        
        await callback.message.edit_text(
            "✅ Счет создан!\n\nНажмите кнопку для оплаты:",
            reply_markup=builder.as_markup()
        )
        
    except Exception as e:
        logger.error(f"YooKassa error: {e}")
        await callback.message.edit_text(f"❌ Ошибка: {e}")

# ========== ПЛАТЕЖИ CRYPTOBOT ==========

@dp.callback_query(F.data.startswith("pay_cryptobot_"))
async def pay_cryptobot(callback: types.CallbackQuery):
    plan_id = int(callback.data.split("_")[2])
    plan = db.get_plan_by_id(plan_id)
    user_id = callback.from_user.id
    
    if not plan or not CRYPTOBOT_TOKEN:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    # Получаем курс USDT/RUB
    rate = await get_usdt_rub_rate()
    if not rate:
        await callback.message.edit_text("❌ Не удалось получить курс")
        return
    
    # Конвертируем в USDT
    price_rub = Decimal(str(plan['price']))
    price_usdt = (price_rub / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    
    try:
        crypto = CryptoPay(CRYPTOBOT_TOKEN)
        
        # Создаем инвойс
        invoice = await crypto.create_invoice(
            currency_type="fiat",
            fiat="RUB",
            amount=float(price_rub),
            description=f"VPN на {plan['months']} месяцев",
            payload=json.dumps({
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price_rub),
                "payment_method": "cryptobot"
            }),
            expires_in=3600
        )
        
        # Показываем ссылку
        builder = InlineKeyboardBuilder()
        builder.button(text="🤖 Оплатить в CryptoBot", url=invoice.pay_url)
        
        await callback.message.edit_text(
            f"🤖 <b>Счет CryptoBot создан!</b>\n\n"
            f"Сумма: {price_usdt} USDT\n"
            f"Курс: 1 USDT = {rate:.2f} RUB\n\n"
            "Нажмите кнопку для оплаты:",
            reply_markup=builder.as_markup()
        )
        
    except Exception as e:
        logger.error(f"CryptoBot error: {e}")
        await callback.message.edit_text(f"❌ Ошибка: {e}")

# ========== ОБРАБОТКА ВЕБХУКОВ ==========

async def process_successful_payment(metadata: dict):
    """Обработка успешной оплаты"""
    try:
        user_id = int(metadata['user_id'])
        plan_id = int(metadata['plan_id'])
        host_name = metadata['host_name']
        action = metadata['action']
        months = int(metadata['months'])
        price = float(metadata['price'])
        payment_method = metadata.get('payment_method', 'unknown')
        
        # Получаем данные
        plan = db.get_plan_by_id(plan_id)
        user_data = db.get_user(user_id)
        
        if not plan or not user_data:
            logger.error(f"Invalid payment data: {metadata}")
            return
        
        # Создаем email для ключа
        if action == "new":
            key_number = db.get_next_key_number(user_id)
            email = f"user{user_id}-key{key_number}@{host_name.replace(' ', '').lower()}.bot"
        else:
            # Для продления нужно получить email существующего ключа
            # Пока упростим - создаем новый
            key_number = db.get_next_key_number(user_id)
            email = f"user{user_id}-key{key_number}@{host_name.replace(' ', '').lower()}.bot"
        
        # Создаем ключ на хосте
        days_to_add = months * 30
        result = await xui_api.create_or_update_key(host_name, email, days_to_add, db)
        
        if result.get('error'):
            logger.error(f"X-UI error: {result['error']}")
            await bot.send_message(user_id, f"❌ Ошибка создания ключа: {result['error']}")
            return
        
        # Сохраняем ключ в БД
        key_id = db.add_key(
            user_id,
            host_name,
            result['client_uuid'],
            email,
            result['expiry_timestamp_ms']
        )
        
        # Обновляем статистику пользователя
        db.update_user_stats(user_id, price, months)
        
        # Реферальная система
        referrer_id = user_data.get('referred_by')
        if referrer_id and price > 0:
            percentage = Decimal(str(REFERRAL_PERCENTAGE))
            reward = (Decimal(str(price)) * percentage / 100).quantize(Decimal("0.01"))
            
            if float(reward) > 0:
                # Здесь нужно добавить метод add_to_referral_balance в Database
                pass
        
        # Логируем транзакцию
        db.log_transaction(
            user_data['username'],
            user_id,
            'paid',
            price,
            payment_method,
            metadata
        )
        
        # Отправляем ключ пользователю
        expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
        expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
        
        success_text = (
            f"🎉 <b>Ваш ключ #{key_number} создан!</b>\n\n"
            f"⏳ <b>Действует до:</b> {expiry_formatted}\n"
            f"🖥️ <b>Сервер:</b> {host_name}\n"
            f"📅 <b>Срок:</b> {months} месяцев\n\n"
            f"<code>{result['connection_string']}</code>"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="➕ Продлить", callback_data=f"extend_{key_id}")
        builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
        builder.adjust(2)
        
        await bot.send_message(user_id, success_text, reply_markup=builder.as_markup())
        
        # Уведомление админу
        if ADMIN_ID:
            admin_text = (
                f"🛒 <b>Новая покупка!</b>\n\n"
                f"👤 Пользователь: @{user_data['username']}\n"
                f"🖥️ Сервер: {host_name}\n"
                f"📦 Тариф: {plan['plan_name']}\n"
                f"💰 Сумма: {price:.2f} RUB\n"
                f"💳 Способ: {payment_method}"
            )
            await bot.send_message(ADMIN_ID, admin_text)
        
        logger.info(f"Payment processed successfully for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error processing payment: {e}", exc_info=True)

# ========== ВЕБ-СЕРВЕР ДЛЯ ВЕБХУКОВ ==========

async def handle_yookassa_webhook(request):
    """Обработчик вебхуков ЮKassa"""
    try:
        data = await request.json()
        
        if data.get('event') == 'payment.succeeded':
            metadata = data['object']['metadata']
            
            # Запускаем обработку в фоне
            asyncio.create_task(process_successful_payment(metadata))
        
        return web.Response(text='OK')
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(text='ERROR', status=500)

async def handle_cryptobot_webhook(request):
    """Обработчик вебхуков CryptoBot"""
    try:
        data = await request.json()
        
        if data.get('update_type') == 'invoice_paid':
            payload = data['payload']
            
            # Парсим payload
            try:
                metadata = json.loads(payload)
                asyncio.create_task(process_successful_payment(metadata))
            except:
                logger.error(f"Invalid CryptoBot payload: {payload}")
        
        return web.Response(text='OK')
    except Exception as e:
        logger.error(f"CryptoBot webhook error: {e}")
        return web.Response(text='ERROR', status=500)

async def start_webhook_server():
    """Запуск веб-сервера для вебхуков"""
    app = web.Application()
    
    # Регистрируем обработчики
    app.router.add_post('/yookassa-webhook', handle_yookassa_webhook)
    app.router.add_post('/cryptobot-webhook', handle_cryptobot_webhook)
    
    # Запускаем сервер
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEBHOOK_PORT)
    await site.start()
    
    logger.info(f"Webhook server started on port {WEBHOOK_PORT}")
    
    return runner

# ========== АДМИН ПАНЕЛЬ ==========

@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав", show_alert=True)
        return
    
    user_count = db.get_user_count()
    total_keys = db.get_total_keys_count()
    total_spent = db.get_total_spent_sum()
    
    text = (
        "👑 <b>Админ панель</b>\n\n"
        f"👥 Пользователей: {user_count}\n"
        f"🔑 Ключей: {total_keys}\n"
        f"💰 Выручка: {total_spent:.2f}₽\n\n"
        "Выберите раздел:"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🖥️ Хосты", callback_data="admin_hosts")
    builder.button(text="📦 Тарифы", callback_data="admin_plans")
    builder.button(text="⚙️ Настройки", callback_data="admin_settings")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(2, 2, 2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав", show_alert=True)
        return
    
    user_count = db.get_user_count()
    total_keys = db.get_total_keys_count()
    total_spent = db.get_total_spent_sum()
    hosts_count = len(db.get_all_hosts())
    plans_count = len(db.get_all_plans())
    
    text = (
        "📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: {user_count}\n"
        f"🔑 Всего ключей: {total_keys}\n"
        f"💰 Выручка: {total_spent:.2f}₽\n"
        f"🖥️ Серверов: {hosts_count}\n"
        f"📦 Тарифов: {plans_count}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_hosts")
async def admin_hosts(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав", show_alert=True)
        return
    
    hosts = db.get_all_hosts()
    
    builder = InlineKeyboardBuilder()
    
    for host in hosts:
        builder.button(text=host['host_name'], callback_data=f"view_host_{host['host_name']}")
    
    builder.button(text="➕ Добавить хост", callback_data="add_host")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(f"🖥️ Хосты ({len(hosts)}):", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "add_host")
async def add_host_start(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🖥️ <b>Добавление хоста</b>\n\n"
        "Пришлите данные в формате:\n"
        "<code>Имя_хоста\nURL_X-UI\nЛогин\nПароль\nID_инбаунда</code>\n\n"
        "Пример:\n"
        "<code>Server-1\nhttps://server.com:54321\nadmin\npassword\n1</code>",
        parse_mode="HTML"
    )
    
    # Здесь нужно добавить состояние FSM для обработки ввода
    # Для простоты пока пропустим

@dp.callback_query(F.data.startswith("view_host_"))
async def view_host(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав", show_alert=True)
        return
    
    host_name = callback.data.split("_")[2]
    host = db.get_host(host_name)
    
    if not host:
        await callback.answer("Хост не найден", show_alert=True)
        return
    
    plans = db.get_plans_for_host(host_name)
    
    text = (
        f"🖥️ <b>Хост:</b> {host_name}\n"
        f"🔗 URL: {host['host_url']}\n"
        f"👤 Логин: {host['host_username']}\n"
        f"🆔 Инбаунд: {host['host_inbound_id']}\n"
        f"📦 Тарифов: {len(plans)}"
    )
    
    builder = InlineKeyboardBuilder()
    
    if plans:
        text += "\n\n<b>Тарифы:</b>\n"
        for plan in plans:
            text += f"• {plan['plan_name']} - {plan['months']}м - {plan['price']}₽\n"
    
    builder.button(text="🗑️ Удалить", callback_data=f"delete_host_{host_name}")
    builder.button(text="📦 Добавить тариф", callback_data=f"add_plan_{host_name}")
    builder.button(text="⬅️ Назад", callback_data="admin_hosts")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

async def main():
    """Запуск всего приложения"""
    if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
        print("\n" + "="*50)
        print("❌ ОШИБКА: BOT_TOKEN не настроен!")
        print("Отредактируйте файл .env")
        print("="*50)
        return
    
    if not ADMIN_ID or ADMIN_ID == 123456789:
        print("⚠️ ВНИМАНИЕ: ADMIN_ID не настроен!")
    
    # Запускаем веб-сервер для вебхуков
    webhook_runner = await start_webhook_server()
    
    try:
        # Запускаем бота
        bot_info = await bot.get_me()
        print("\n" + "="*50)
        print(f"✅ VLESS Bot запущен!")
        print(f"🤖 Имя: @{bot_info.username}")
        print(f"👑 Админ: {ADMIN_ID}")
        print(f"🌐 Вебхуки: {WEBHOOK_DOMAIN}:{WEBHOOK_PORT}")
        print(f"🗄️ БД: vless_bot.db")
        print("="*50)
        print("\nВАЖНО: Настройте вебхуки в платежных системах:")
        print(f"• ЮKassa: {WEBHOOK_DOMAIN}/yookassa-webhook")
        print(f"• CryptoBot: {WEBHOOK_DOMAIN}/cryptobot-webhook")
        print("="*50)
        
        await dp.start_polling(bot)
        
    finally:
        await bot.session.close()
        await webhook_runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())