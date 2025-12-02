#!/usr/bin/env python3
"""
VLESS Telegram Bot - ПОЛНАЯ РАБОЧАЯ ВЕРСИЯ
Версия 2.1 - Исправлены все ошибки, сохранена полная функциональность
"""

import asyncio
import logging
import sys
import sqlite3
import json
import uuid
import qrcode
import hashlib
import base64
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, quote
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
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
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
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID", "")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY", "")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")
HELEKET_MERCHANT_ID = os.getenv("HELEKET_MERCHANT_ID", "")
HELEKET_API_KEY = os.getenv("HELEKET_API_KEY", "")
TON_WALLET_ADDRESS = os.getenv("TON_WALLET_ADDRESS", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")

# Вебхук настройки
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN", "https://your-domain.com")
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
FORCE_SUBSCRIPTION = os.getenv("FORCE_SUBSCRIPTION", "false").lower() == "true"
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "true").lower() == "true"
TRIAL_DURATION_DAYS = int(os.getenv("TRIAL_DURATION_DAYS", "3"))
SBP_ENABLED = os.getenv("SBP_ENABLED", "true").lower() == "true"
ENABLE_REFERRALS = os.getenv("ENABLE_REFERRALS", "true").lower() == "true"
REFERRAL_PERCENTAGE = float(os.getenv("REFERRAL_PERCENTAGE", "10"))
REFERRAL_DISCOUNT = float(os.getenv("REFERRAL_DISCOUNT", "10"))
MINIMUM_WITHDRAWAL = float(os.getenv("MINIMUM_WITHDRAWAL", "100"))

# ========== СОСТОЯНИЯ FSM ==========

class Form(StatesGroup):
    waiting_for_host_data = State()
    waiting_for_plan_data = State()
    waiting_for_settings = State()
    waiting_for_support_message = State()

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
                    full_name TEXT,
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
                    processed BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    
    def register_user(self, telegram_id: int, username: str, full_name: str, referrer_id: int = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO users (telegram_id, username, full_name, referred_by) 
                VALUES (?, ?, ?, ?)
            ''', (telegram_id, username, full_name, referrer_id))
    
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
    
    def add_referral_balance(self, user_id: int, amount: float):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET referral_balance = referral_balance + ? 
                WHERE telegram_id = ?
            ''', (amount, user_id))
    
    def get_referrals(self, referrer_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE referred_by = ?", (referrer_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== КЛЮЧИ ==========
    
    def add_key(self, user_id: int, host_name: str, xui_client_uuid: str, 
               key_email: str, expiry_date: datetime) -> int:
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
    
    def update_key_expiry(self, key_id: int, expiry_date: datetime):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE user_keys SET expiry_date = ? WHERE key_id = ?", (expiry_date, key_id))
    
    def delete_key(self, key_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM user_keys WHERE key_id = ?", (key_id,))
    
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
            cursor.execute("DELETE FROM plans WHERE host_name = ?", (host_name,))
    
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
    
    def get_all_transactions(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM transactions 
                ORDER BY created_date DESC 
                LIMIT 100
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
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
    
    def get_active_users_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) 
                FROM user_keys 
                WHERE expiry_date > datetime('now')
            """)
            return cursor.fetchone()[0]
    
    def get_total_keys_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys")
            return cursor.fetchone()[0]
    
    def get_active_keys_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys WHERE expiry_date > datetime('now')")
            return cursor.fetchone()[0]
    
    def get_total_spent_sum(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_spent) FROM users")
            result = cursor.fetchone()[0]
            return result if result else 0.0
    
    def get_today_revenue(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT SUM(amount_rub) 
                FROM transactions 
                WHERE date(created_date) = date('now') 
                AND status = 'paid'
            """)
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
    
    def login_to_host(self, host_url: str, username: str, password: str, inbound_id: int) -> Tuple[Api, Inbound]:
        try:
            api = Api(host=host_url, username=username, password=password)
            api.login()
            inbounds = api.inbound.get_list()
            target_inbound = next((i for i in inbounds if i.id == inbound_id), None)
            if not target_inbound:
                raise Exception(f"Inbound with id {inbound_id} not found")
            return api, target_inbound
        except Exception as e:
            logging.error(f"X-UI login failed: {e}")
            raise
    
    def get_connection_string(self, inbound: Inbound, user_uuid: str, host_url: str, remark: str) -> str:
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
    
    async def create_or_update_key(self, host_name: str, email: str, days_to_add: int, db: Database) -> Dict:
        host_data = db.get_host(host_name)
        if not host_data:
            return {"error": "Хост не найден"}
        
        try:
            api, inbound = self.login_to_host(
                host_data['host_url'],
                host_data['host_username'],
                host_data['host_pass'],
                host_data['host_inbound_id']
            )
            
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
                "expiry_date": new_expiry,
                "connection_string": connection_string,
                "host_name": host_name
            }
            
        except Exception as e:
            logging.error(f"X-UI error: {e}")
            return {"error": str(e)}

# ========== ПЛАТЕЖНЫЕ УТИЛИТЫ ==========

async def get_usdt_rub_rate() -> Decimal:
    """Получить курс USDT/RUB"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ticker/price?symbol=USDTRUB", timeout=10) as resp:
                data = await resp.json()
                return Decimal(data['price'])
    except Exception as e:
        logging.error(f"Failed to get USDT/RUB rate: {e}")
        return Decimal("90.0")  # Fallback курс

async def get_ton_usdt_rate() -> Decimal:
    """Получить курс TON/USDT"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT", timeout=10) as resp:
                data = await resp.json()
                return Decimal(data['price'])
    except Exception as e:
        logging.error(f"Failed to get TON/USDT rate: {e}")
        return Decimal("2.5")  # Fallback курс

def format_currency(amount: float) -> str:
    """Форматирование суммы валюты"""
    return f"{amount:.2f}"

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
router = Router()
dp.include_router(router)

# ========== КЛАВИАТУРЫ ==========

def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
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

def create_back_button(target: str = "main_menu") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=target)
    return builder.as_markup()

def create_qr_code(connection_string: str) -> BytesIO:
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(connection_string)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    bio = BytesIO()
    img.save(bio, 'PNG')
    bio.seek(0)
    return bio

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    full_name = message.from_user.full_name
    
    # Парсим реферальный код
    referrer_id = None
    if message.text and len(message.text.split()) > 1:
        args = message.text.split()[1]
        if args.startswith('ref_'):
            try:
                referrer_id = int(args.split('_')[1])
                # Проверяем, существует ли реферер
                referrer_data = db.get_user(referrer_id)
                if not referrer_data:
                    referrer_id = None
            except:
                referrer_id = None
    
    db.register_user(user_id, username, full_name, referrer_id)
    
    # Проверка блокировки
    user_data = db.get_user(user_id)
    if user_data and user_data.get('is_banned'):
        await message.answer("❌ Вы заблокированы в системе.")
        return
    
    # Приветствие
    welcome_text = (
        f"👋 Добро пожаловать, {message.from_user.full_name}!\n\n"
        "🔐 <b>VPN сервис с протоколом VLESS</b>\n"
        "• Высокая скорость\n"
        "• Защита данных\n"
        "• Безлимитный трафик\n\n"
        "Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=create_main_menu(user_id))

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    
    if user_data and user_data.get('is_banned'):
        await message.answer("❌ Вы заблокированы")
        return
    
    await message.answer("Главное меню:", reply_markup=create_main_menu(user_id))

# ========== ОБРАБОТЧИКИ ПРОФИЛЯ ==========

@dp.callback_query(F.data == "show_profile")
async def show_profile(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    user_keys = db.get_user_keys(user_id)
    
    if not user_data:
        await callback.answer("Ошибка получения данных", show_alert=True)
        return
    
    now = datetime.now()
    active_keys = []
    for key in user_keys:
        expiry_date = datetime.fromisoformat(key['expiry_date']) if isinstance(key['expiry_date'], str) else key['expiry_date']
        if expiry_date > now:
            active_keys.append(key)
    
    if active_keys:
        latest = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']) if isinstance(k['expiry_date'], str) else k['expiry_date'])
        expiry = datetime.fromisoformat(latest['expiry_date']) if isinstance(latest['expiry_date'], str) else latest['expiry_date']
        time_left = expiry - now
        days_left = time_left.days
        hours_left = time_left.seconds // 3600
        vpn_status = f"✅ <b>Статус VPN:</b> Активен\n⏳ <b>Осталось:</b> {days_left} д. {hours_left} ч."
    elif user_keys:
        vpn_status = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
    else:
        vpn_status = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."
    
    # Форматируем дату создания
    if isinstance(user_data.get('created_at'), str):
        created_date = datetime.fromisoformat(user_data['created_at'])
    else:
        created_date = user_data.get('created_at', datetime.now())
    
    profile_text = (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"🆔 ID: <code>{user_data['telegram_id']}</code>\n"
        f"👤 Имя: {user_data['full_name'] or user_data['username']}\n"
        f"📅 Регистрация: {created_date.strftime('%d.%m.%Y')}\n\n"
        f"💰 <b>Потрачено всего:</b> {user_data['total_spent']:.0f} RUB\n"
        f"📅 <b>Приобретено месяцев:</b> {user_data['total_months']}\n"
        f"🎁 <b>Пробный период:</b> {'Использован' if user_data.get('trial_used') else 'Доступен'}\n\n"
        f"{vpn_status}\n\n"
        f"🔑 <b>Всего ключей:</b> {len(user_keys)}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="show_profile")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(2)
    
    await callback.message.edit_text(profile_text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "manage_keys")
async def manage_keys(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_keys = db.get_user_keys(user_id)
    
    if not user_keys:
        text = "🔑 У вас пока нет ключей VPN.\n\nНажмите кнопку ниже, чтобы приобрести ключ:"
        builder = InlineKeyboardBuilder()
        builder.button(text="🛒 Купить VPN", callback_data="buy_new_key")
        builder.button(text="🎁 Попробовать бесплатно", callback_data="get_trial")
        builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
        builder.adjust(1)
    else:
        now = datetime.now()
        text = "🔑 <b>Ваши ключи VPN:</b>\n\n"
        
        builder = InlineKeyboardBuilder()
        
        for i, key in enumerate(user_keys[:10], 1):  # Ограничиваем 10 ключами
            expiry_date = datetime.fromisoformat(key['expiry_date']) if isinstance(key['expiry_date'], str) else key['expiry_date']
            is_active = expiry_date > now
            
            status_icon = "✅" if is_active else "❌"
            expiry_str = expiry_date.strftime('%d.%m.%Y')
            
            text += f"{i}. {status_icon} <b>{key['host_name']}</b>\n"
            text += f"   📅 Срок: {expiry_str}\n"
            
            if is_active:
                days_left = (expiry_date - now).days
                text += f"   ⏳ Осталось: {days_left} д.\n"
            
            text += "\n"
            
            # Добавляем кнопки для каждого ключа
            builder.button(text=f"#{key['key_id']} - {key['host_name']}", callback_data=f"view_key_{key['key_id']}")
        
        if len(user_keys) > 10:
            text += f"\n... и еще {len(user_keys) - 10} ключей"
        
        builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
        builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("view_key_"))
async def view_key(callback: types.CallbackQuery):
    try:
        key_id = int(callback.data.split("_")[2])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        # Получаем хост для генерации connection string
        host_data = db.get_host(key_data['host_name'])
        if not host_data:
            await callback.answer("Хост не найден", show_alert=True)
            return
        
        # Получаем connection string
        try:
            api, inbound = xui_api.login_to_host(
                host_data['host_url'],
                host_data['host_username'],
                host_data['host_pass'],
                host_data['host_inbound_id']
            )
            connection_string = xui_api.get_connection_string(inbound, key_data['xui_client_uuid'], host_data['host_url'], key_data['host_name'])
        except:
            connection_string = "❌ Не удалось получить конфигурацию"
        
        expiry_date = datetime.fromisoformat(key_data['expiry_date']) if isinstance(key_data['expiry_date'], str) else key_data['expiry_date']
        now = datetime.now()
        is_active = expiry_date > now
        
        status_text = "✅ Активен" if is_active else "❌ Истек"
        time_left = expiry_date - now if is_active else timedelta(0)
        
        text = (
            f"🔑 <b>Ключ #{key_data['key_id']}</b>\n\n"
            f"🖥️ <b>Сервер:</b> {key_data['host_name']}\n"
            f"📧 <b>Email:</b> {key_data['key_email']}\n"
            f"📅 <b>Создан:</b> {key_data['created_date'][:10] if isinstance(key_data['created_date'], str) else key_data['created_date'].strftime('%d.%m.%Y')}\n"
            f"📅 <b>Действует до:</b> {expiry_date.strftime('%d.%m.%Y %H:%M')}\n"
            f"📊 <b>Статус:</b> {status_text}\n"
        )
        
        if is_active:
            text += f"⏳ <b>Осталось:</b> {time_left.days} дней\n\n"
        
        text += f"<code>{connection_string}</code>"
        
        builder = InlineKeyboardBuilder()
        
        if is_active:
            builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
            builder.button(text="➕ Продлить", callback_data=f"extend_{key_id}")
        
        builder.button(text="🗑️ Удалить", callback_data=f"delete_key_{key_id}")
        builder.button(text="⬅️ Назад", callback_data="manage_keys")
        builder.adjust(2, 1, 1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        
    except Exception as e:
        logger.error(f"Error viewing key: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("qr_"))
async def show_qr_code(callback: types.CallbackQuery):
    try:
        key_id = int(callback.data.split("_")[1])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        # Получаем connection string для QR кода
        host_data = db.get_host(key_data['host_name'])
        if not host_data:
            await callback.answer("Хост не найден", show_alert=True)
            return
        
        try:
            api, inbound = xui_api.login_to_host(
                host_data['host_url'],
                host_data['host_username'],
                host_data['host_pass'],
                host_data['host_inbound_id']
            )
            connection_string = xui_api.get_connection_string(inbound, key_data['xui_client_uuid'], host_data['host_url'], key_data['host_name'])
            
            # Генерируем QR код
            qr_image = create_qr_code(connection_string)
            
            text = (
                f"📱 <b>QR-код для ключа #{key_id}</b>\n\n"
                f"🖥️ Сервер: {key_data['host_name']}\n"
                f"📅 Действует до: {key_data['expiry_date'][:10] if isinstance(key_data['expiry_date'], str) else key_data['expiry_date'].strftime('%d.%m.%Y')}\n\n"
                "Отсканируйте QR-код в приложении V2Ray/VLESS."
            )
            
            builder = InlineKeyboardBuilder()
            builder.button(text="⬅️ Назад", callback_data=f"view_key_{key_id}")
            
            await callback.message.delete()
            await callback.message.answer_photo(
                photo=types.BufferedInputFile(qr_image.getvalue(), filename="qrcode.png"),
                caption=text,
                reply_markup=builder.as_markup()
            )
            
        except Exception as e:
            logger.error(f"Error generating QR: {e}")
            await callback.answer("Ошибка генерации QR-кода", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error in QR handler: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("delete_key_"))
async def delete_key(callback: types.CallbackQuery):
    try:
        key_id = int(callback.data.split("_")[2])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Да, удалить", callback_data=f"confirm_delete_key_{key_id}")
        builder.button(text="❌ Нет, отмена", callback_data=f"view_key_{key_id}")
        
        await callback.message.edit_text(
            f"🗑️ <b>Удаление ключа #{key_id}</b>\n\n"
            f"Вы уверены, что хотите удалить ключ для сервера {key_data['host_name']}?\n"
            f"Это действие нельзя отменить.",
            reply_markup=builder.as_markup()
        )
        
    except Exception as e:
        logger.error(f"Error in delete_key: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("confirm_delete_key_"))
async def confirm_delete_key(callback: types.CallbackQuery):
    try:
        key_id = int(callback.data.split("_")[3])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        db.delete_key(key_id)
        await callback.answer("✅ Ключ удален", show_alert=True)
        await manage_keys(callback)  # Возвращаемся к списку ключей
        
    except Exception as e:
        logger.error(f"Error confirming delete: {e}")
        await callback.answer("Ошибка", show_alert=True)

# ========== ТРИАЛЬНЫЙ ПЕРИОД ==========

@dp.callback_query(F.data == "get_trial")
async def get_trial(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if not TRIAL_ENABLED:
        await callback.answer("Пробный период отключен", show_alert=True)
        return
    
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
        result['expiry_date']
    )
    
    # Помечаем триал как использованный
    db.set_trial_used(user_id)
    
    # Показываем ключ
    expiry_date = result['expiry_date']
    expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
    
    success_text = (
        f"🎉 <b>Ваш пробный ключ готов!</b>\n\n"
        f"⏳ <b>Действует до:</b> {expiry_formatted}\n"
        f"🖥️ <b>Сервер:</b> {host['host_name']}\n"
        f"📅 <b>Длительность:</b> {TRIAL_DURATION_DAYS} дней\n\n"
        f"<code>{result['connection_string']}</code>"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
    builder.button(text="🛒 Купить полный доступ", callback_data="buy_new_key")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(2, 1)
    
    await callback.message.edit_text(success_text, reply_markup=builder.as_markup())

# ========== ПОКУПКА VPN ==========

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
    
    await callback.message.edit_text("🛒 <b>Покупка VPN ключа</b>\n\nВыберите сервер:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("select_host_"))
async def select_host(callback: types.CallbackQuery):
    host_name = callback.data.split("_")[2]
    plans = db.get_plans_for_host(host_name)
    
    if not plans:
        await callback.message.edit_text(f"❌ Нет тарифов для {host_name}")
        return
    
    builder = InlineKeyboardBuilder()
    for plan in plans:
        price_int = int(plan['price']) if plan['price'].is_integer() else plan['price']
        builder.button(
            text=f"{plan['plan_name']} - {price_int}₽",
            callback_data=f"select_plan_{plan['plan_id']}"
        )
    builder.button(text="⬅️ Назад", callback_data="buy_new_key")
    builder.adjust(1)
    
    await callback.message.edit_text(f"🛒 <b>Тарифы для {host_name}:</b>\n\nВыберите тарифный план:", reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("select_plan_"))
async def select_plan(callback: types.CallbackQuery):
    plan_id = int(callback.data.split("_")[2])
    plan = db.get_plan_by_id(plan_id)
    
    if not plan:
        await callback.answer("Ошибка: план не найден", show_alert=True)
        return
    
    # Создаем клавиатуру с методами оплаты
    builder = InlineKeyboardBuilder()
    
    # Проверяем доступные методы оплаты
    payment_methods_available = []
    
    if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
        payment_methods_available.append(("💳 Карта/СБП (ЮKassa)", f"pay_yookassa_{plan_id}"))
    
    if CRYPTOBOT_TOKEN:
        payment_methods_available.append(("🤖 CryptoBot (USDT)", f"pay_cryptobot_{plan_id}"))
    
    if HELEKET_MERCHANT_ID and HELEKET_API_KEY:
        payment_methods_available.append(("💎 Heleket (TON)", f"pay_heleket_{plan_id}"))
    
    if not payment_methods_available:
        await callback.message.edit_text("❌ Нет доступных методов оплаты")
        return
    
    # Добавляем кнопки методов оплаты
    for text, data in payment_methods_available:
        builder.button(text=text, callback_data=data)
    
    builder.button(text="⬅️ Назад", callback_data=f"select_host_{plan['host_name']}")
    builder.adjust(1)
    
    price_int = int(plan['price']) if plan['price'].is_integer() else plan['price']
    
    await callback.message.edit_text(
        f"🛒 <b>Оформление заказа</b>\n\n"
        f"📋 <b>План:</b> {plan['plan_name']}\n"
        f"💰 <b>Цена:</b> {price_int}₽\n"
        f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
        f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
        "Выберите способ оплаты:",
        reply_markup=builder.as_markup()
    )

# ========== ПЛАТЕЖИ ЮKASSA ==========

@dp.callback_query(F.data.startswith("pay_yookassa_"))
async def pay_yookassa(callback: types.CallbackQuery):
    try:
        plan_id = int(callback.data.split("_")[2])
        plan = db.get_plan_by_id(plan_id)
        user_id = callback.from_user.id
        user_data = db.get_user(user_id)
        
        if not plan:
            await callback.answer("Ошибка: план не найден", show_alert=True)
            return
        
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
            await callback.answer("Оплата через ЮKassa недоступна", show_alert=True)
            return
        
        # Применяем скидку для рефералов
        price = Decimal(str(plan['price']))
        discount_applied = False
        
        if user_data and user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            discount = Decimal(str(REFERRAL_DISCOUNT))
            if discount > 0:
                discount_amount = (price * discount / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                price = price - discount_amount
                discount_applied = True
        
        # Настраиваем ЮKassa
        Configuration.account_id = YOOKASSA_SHOP_ID
        Configuration.secret_key = YOOKASSA_SECRET_KEY
        
        # Создаем платеж
        payment_id = str(uuid.uuid4())
        
        payment = Payment.create({
            "amount": {"value": f"{float(price):.2f}", "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"},
            "capture": True,
            "description": f"VPN на {plan['months']} месяцев ({plan['host_name']})",
            "metadata": {
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price),
                "original_price": float(plan['price']),
                "discount_applied": discount_applied,
                "discount_percent": float(REFERRAL_DISCOUNT) if discount_applied else 0,
                "payment_id": payment_id,
                "payment_method": "yookassa"
            }
        })
        
        # Сохраняем транзакцию
        db.create_webhook_transaction(
            payment.id,
            user_id,
            float(price),
            {
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price),
                "original_price": float(plan['price']),
                "discount_applied": discount_applied,
                "discount_percent": float(REFERRAL_DISCOUNT) if discount_applied else 0,
                "payment_method": "yookassa"
            }
        )
        
        # Показываем ссылку на оплату
        builder = InlineKeyboardBuilder()
        builder.button(text="💳 Перейти к оплате", url=payment.confirmation.confirmation_url)
        builder.button(text="🔄 Проверить оплату", callback_data=f"check_payment_{payment.id}")
        builder.button(text="⬅️ Отмена", callback_data=f"select_plan_{plan_id}")
        builder.adjust(1)
        
        price_int = int(price) if price == price.to_integral() else float(price)
        original_price_int = int(plan['price']) if plan['price'].is_integer() else plan['price']
        
        message_text = "✅ <b>Счет создан!</b>\n\n"
        
        if discount_applied:
            message_text += f"💰 <b>Цена со скидкой:</b> {price_int}₽\n"
            message_text += f"🎁 <b>Скидка:</b> {REFERRAL_DISCOUNT}%\n"
            message_text += f"💵 <b>Изначальная цена:</b> <s>{original_price_int}₽</s>\n"
        else:
            message_text += f"💰 <b>Цена:</b> {price_int}₽\n"
        
        message_text += f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
        message_text += f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
        message_text += "Нажмите кнопку для оплаты. После оплаты нажмите 'Проверить оплату'."
        
        await callback.message.edit_text(message_text, reply_markup=builder.as_markup())
        
    except Exception as e:
        logger.error(f"YooKassa error: {e}")
        await callback.message.edit_text(f"❌ Ошибка создания счета: {str(e)[:200]}")

@dp.callback_query(F.data.startswith("check_payment_"))
async def check_payment(callback: types.CallbackQuery):
    try:
        payment_id = callback.data.split("_")[2]
        
        # Проверяем статус платежа
        payment = Payment.find_one(payment_id)
        
        if payment.status == "succeeded":
            # Платеж успешен, обрабатываем
            metadata = payment.metadata
            await process_successful_payment(metadata)
            await callback.answer("✅ Оплата подтверждена! Ключ создан.", show_alert=True)
        elif payment.status == "pending":
            await callback.answer("⏳ Платеж еще не прошел. Подождите немного.", show_alert=True)
        elif payment.status == "canceled":
            await callback.answer("❌ Платеж отменен.", show_alert=True)
        else:
            await callback.answer(f"Статус платежа: {payment.status}", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error checking payment: {e}")
        await callback.answer("Ошибка проверки платежа", show_alert=True)

# ========== ПЛАТЕЖИ CRYPTOBOT ==========

@dp.callback_query(F.data.startswith("pay_cryptobot_"))
async def pay_cryptobot(callback: types.CallbackQuery):
    try:
        plan_id = int(callback.data.split("_")[2])
        plan = db.get_plan_by_id(plan_id)
        user_id = callback.from_user.id
        user_data = db.get_user(user_id)
        
        if not plan or not CRYPTOBOT_TOKEN:
            await callback.answer("Ошибка: план не найден или CryptoBot недоступен", show_alert=True)
            return
        
        # Применяем скидку для рефералов
        price_rub = Decimal(str(plan['price']))
        discount_applied = False
        
        if user_data and user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            discount = Decimal(str(REFERRAL_DISCOUNT))
            if discount > 0:
                discount_amount = (price_rub * discount / 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                price_rub = price_rub - discount_amount
                discount_applied = True
        
        # Получаем курс USDT/RUB
        rate = await get_usdt_rub_rate()
        if not rate:
            await callback.message.edit_text("❌ Не удалось получить курс обмена")
            return
        
        # Конвертируем в USDT
        price_usdt = (price_rub / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        
        crypto = CryptoPay(CRYPTOBOT_TOKEN)
        
        # Создаем инвойс
        invoice = await crypto.create_invoice(
            currency_type="fiat",
            fiat="RUB",
            amount=float(price_rub),
            description=f"VPN на {plan['months']} месяцев ({plan['host_name']})",
            payload=json.dumps({
                "user_id": user_id,
                "plan_id": plan_id,
                "host_name": plan['host_name'],
                "action": "new",
                "months": plan['months'],
                "price": float(price_rub),
                "original_price": float(plan['price']),
                "discount_applied": discount_applied,
                "discount_percent": float(REFERRAL_DISCOUNT) if discount_applied else 0,
                "payment_method": "cryptobot"
            }),
            expires_in=3600
        )
        
        # Показываем ссылку
        builder = InlineKeyboardBuilder()
        builder.button(text="🤖 Оплатить в CryptoBot", url=invoice.pay_url)
        builder.button(text="🔄 Проверить оплату", callback_data=f"check_crypto_payment_{invoice.invoice_id}")
        builder.button(text="⬅️ Отмена", callback_data=f"select_plan_{plan_id}")
        builder.adjust(1)
        
        price_rub_int = int(price_rub) if price_rub == price_rub.to_integral() else float(price_rub)
        original_price_int = int(plan['price']) if plan['price'].is_integer() else plan['price']
        
        message_text = "🤖 <b>Счет CryptoBot создан!</b>\n\n"
        
        if discount_applied:
            message_text += f"💰 <b>Цена со скидкой:</b> {price_rub_int}₽\n"
            message_text += f"🎁 <b>Скидка:</b> {REFERRAL_DISCOUNT}%\n"
            message_text += f"💵 <b>Изначальная цена:</b> <s>{original_price_int}₽</s>\n"
        else:
            message_text += f"💰 <b>Цена:</b> {price_rub_int}₽\n"
        
        message_text += f"💲 <b>В USDT:</b> {price_usdt}\n"
        message_text += f"📈 <b>Курс:</b> 1 USDT = {rate:.2f} RUB\n"
        message_text += f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
        message_text += f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
        message_text += "Нажмите кнопку для оплаты. После оплаты нажмите 'Проверить оплату'."
        
        await callback.message.edit_text(message_text, reply_markup=builder.as_markup())
        
    except Exception as e:
        logger.error(f"CryptoBot error: {e}")
        await callback.message.edit_text(f"❌ Ошибка создания счета: {str(e)[:200]}")

@dp.callback_query(F.data.startswith("check_crypto_payment_"))
async def check_crypto_payment(callback: types.CallbackQuery):
    try:
        invoice_id = int(callback.data.split("_")[3])
        
        if not CRYPTOBOT_TOKEN:
            await callback.answer("CryptoBot недоступен", show_alert=True)
            return
        
        crypto = CryptoPay(CRYPTOBOT_TOKEN)
        
        # Получаем информацию о инвойсе
        invoices = await crypto.get_invoices(invoice_ids=invoice_id)
        
        if not invoices:
            await callback.answer("Счет не найден", show_alert=True)
            return
        
        invoice = invoices[0]
        
        if invoice.status == "paid":
            # Получаем payload из описания
            try:
                metadata = json.loads(invoice.payload)
                await process_successful_payment(metadata)
                await callback.answer("✅ Оплата подтверждена! Ключ создан.", show_alert=True)
            except:
                await callback.answer("✅ Оплата подтверждена, но не удалось обработать данные.", show_alert=True)
        elif invoice.status == "active":
            await callback.answer("⏳ Счет еще не оплачен.", show_alert=True)
        elif invoice.status == "expired":
            await callback.answer("❌ Счет истек.", show_alert=True)
        else:
            await callback.answer(f"Статус счета: {invoice.status}", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error checking crypto payment: {e}")
        await callback.answer("Ошибка проверки платежа", show_alert=True)

# ========== РЕФЕРАЛЬНАЯ СИСТЕМА ==========

@dp.callback_query(F.data == "show_referrals")
async def show_referrals(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    # Получаем рефералов
    referrals = db.get_referrals(user_id)
    
    # Генерируем реферальную ссылку
    referral_link = f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=ref_{user_id}"
    
    text = (
        f"🤝 <b>Реферальная программа</b>\n\n"
        f"💎 <b>Ваша ссылка:</b>\n<code>{referral_link}</code>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• Рефералов: {len(referrals)}\n"
        f"• Баланс: {user_data.get('referral_balance', 0):.2f}₽\n"
        f"• Минимальный вывод: {MINIMUM_WITHDRAWAL}₽\n\n"
        f"🎁 <b>Бонусы:</b>\n"
        f"• Вы получаете {REFERRAL_PERCENTAGE}% от покупок рефералов\n"
        f"• Реферал получает {REFERRAL_DISCOUNT}% скидку на первую покупку\n\n"
        f"💸 <b>Вывод средств:</b>\n"
        f"Доступен при достижении {MINIMUM_WITHDRAWAL}₽ на балансе"
    )
    
    builder = InlineKeyboardBuilder()
    
    if referrals:
        builder.button(text="👥 Список рефералов", callback_data="show_referrals_list")
    
    if user_data.get('referral_balance', 0) >= MINIMUM_WITHDRAWAL:
        builder.button(text="💰 Вывести средства", callback_data="withdraw_referral")
    
    builder.button(text="📋 Как работает", callback_data="referral_help")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "show_referrals_list")
async def show_referrals_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    referrals = db.get_referrals(user_id)
    
    if not referrals:
        text = "У вас пока нет рефералов."
    else:
        text = f"👥 <b>Ваши рефералы ({len(referrals)}):</b>\n\n"
        
        for i, ref in enumerate(referrals[:20], 1):  # Ограничиваем 20 рефералами
            created_at = ref['created_at']
            if isinstance(created_at, str):
                date_str = created_at[:10]
            else:
                date_str = created_at.strftime('%d.%m.%Y')
            
            text += f"{i}. {ref['full_name'] or ref['username']}\n"
            text += f"   📅 {date_str}\n"
            text += f"   💰 Потратил: {ref['total_spent']:.0f}₽\n\n"
        
        if len(referrals) > 20:
            text += f"\n... и еще {len(referrals) - 20} рефералов"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="show_referrals")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "referral_help")
async def referral_help(callback: types.CallbackQuery):
    text = (
        f"📋 <b>Как работает реферальная система</b>\n\n"
        f"1. <b>Ваша ссылка:</b>\n"
        f"Поделитесь своей реферальной ссылкой с друзьями\n\n"
        f"2. <b>Бонусы для друга:</b>\n"
        f"При первой покупке ваш друг получает {REFERRAL_DISCOUNT}% скидку\n\n"
        f"3. <b>Ваш бонус:</b>\n"
        f"Вы получаете {REFERRAL_PERCENTAGE}% от суммы каждой покупки ваших рефералов\n\n"
        f"4. <b>Вывод средств:</b>\n"
        f"Выводите заработанные средства при достижении {MINIMUM_WITHDRAWAL}₽ на балансе\n\n"
        f"<b>Пример:</b>\n"
        f"Друг покупает VPN за 1000₽:\n"
        f"• Он платит только {1000 * (1 - REFERRAL_DISCOUNT/100):.0f}₽\n"
        f"• Вы получаете {1000 * (REFERRAL_PERCENTAGE/100):.0f}₽ на баланс"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="show_referrals")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ========== ПОДДЕРЖКА ==========

@dp.callback_query(F.data == "show_help")
async def show_help(callback: types.CallbackQuery):
    support_user = SUPPORT_USER if SUPPORT_USER else "администратору"
    support_text = SUPPORT_TEXT if SUPPORT_TEXT else "Задайте свой вопрос напрямую"
    
    text = (
        f"🆘 <b>Поддержка</b>\n\n"
        f"{support_text}:\n\n"
        f"👤 Написать в поддержку: {support_user}\n\n"
        f"📋 <b>Частые вопросы:</b>\n"
        f"• <b>Как подключиться?</b>\n"
        f"Скачайте приложение для вашей ОС и вставьте конфигурацию\n\n"
        f"• <b>Не работает подключение?</b>\n"
        f"1. Проверьте срок действия ключа\n"
        f"2. Перезапустите VPN приложение\n"
        f"3. Попробуйте другой сервер\n\n"
        f"• <b>Как продлить ключ?</b>\n"
        f"Купите новый ключ на тот же сервер\n\n"
        f"<b>Приложения для подключения:</b>\n"
        f"• Android: {ANDROID_URL}\n"
        f"• iOS: {IOS_URL}\n"
        f"• Windows: {WINDOWS_URL}\n"
        f"• Linux: {LINUX_URL}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Написать в поддержку", url=f"https://t.me/{support_user.replace('@', '')}" if support_user.startswith('@') else f"tg://user?id={support_user}")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ========== О ПРОЕКТЕ ==========

@dp.callback_query(F.data == "show_about")
async def show_about(callback: types.CallbackQuery):
    about_text = ABOUT_TEXT if ABOUT_TEXT else "VPN сервис для безопасного и свободного интернета"
    
    text = (
        f"ℹ️ <b>О проекте</b>\n\n"
        f"{about_text}\n\n"
        f"<b>Наши преимущества:</b>\n"
        f"• Высокая скорость соединения\n"
        f"• Защита ваших данных\n"
        f"• Безлимитный трафик\n"
        f"• Круглосуточная поддержка\n"
        f"• Простая настройка\n\n"
        f"<b>Технологии:</b>\n"
        f"• Протокол VLESS + Reality\n"
        f"• Современное шифрование\n"
        f"• Глобальная сеть серверов\n\n"
    )
    
    # Добавляем ссылки если они есть
    if TERMS_URL:
        text += f"📄 <a href='{TERMS_URL}'>Пользовательское соглашение</a>\n"
    if PRIVACY_URL:
        text += f"🔒 <a href='{PRIVACY_URL}'>Политика конфиденциальности</a>\n"
    if CHANNEL_URL:
        text += f"📢 <a href='{CHANNEL_URL}'>Наш канал</a>\n"
    
    builder = InlineKeyboardBuilder()
    
    if TERMS_URL:
        builder.button(text="📄 Соглашение", url=TERMS_URL)
    if PRIVACY_URL:
        builder.button(text="🔒 Конфиденциальность", url=PRIVACY_URL)
    if CHANNEL_URL:
        builder.button(text="📢 Канал", url=CHANNEL_URL)
    
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    
    # Настраиваем расположение кнопок
    if builder.buttons:
        builder.adjust(2 if len(builder.buttons) > 2 else 1, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), disable_web_page_preview=True)

# ========== ОБРАБОТКА УСПЕШНЫХ ПЛАТЕЖЕЙ ==========

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
        discount_applied = metadata.get('discount_applied', False)
        
        # Получаем данные
        plan = db.get_plan_by_id(plan_id)
        user_data = db.get_user(user_id)
        
        if not plan or not user_data:
            logger.error(f"Invalid payment data: {metadata}")
            return
        
        # Создаем email для ключа
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
            result['expiry_date']
        )
        
        # Обновляем статистику пользователя
        db.update_user_stats(user_id, price, months)
        
        # Реферальная система
        referrer_id = user_data.get('referred_by')
        if referrer_id and price > 0 and ENABLE_REFERRALS:
            percentage = Decimal(str(REFERRAL_PERCENTAGE))
            reward = (Decimal(str(price)) * percentage / 100).quantize(Decimal("0.01"))
            
            if float(reward) > 0:
                db.add_referral_balance(referrer_id, float(reward))
                
                # Уведомляем реферера
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 <b>Вы получили реферальное вознаграждение!</b>\n\n"
                        f"👤 От: {user_data['full_name'] or user_data['username']}\n"
                        f"💰 Сумма: {float(reward):.2f}₽\n"
                        f"💎 Ваш баланс: {db.get_user(referrer_id).get('referral_balance', 0):.2f}₽"
                    )
                except:
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
        expiry_date = result['expiry_date']
        expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
        
        success_text = (
            f"🎉 <b>Ваш ключ #{key_number} создан!</b>\n\n"
            f"⏳ <b>Действует до:</b> {expiry_formatted}\n"
            f"🖥️ <b>Сервер:</b> {host_name}\n"
            f"📅 <b>Срок:</b> {months} месяцев\n"
            f"💰 <b>Сумма:</b> {price:.2f}₽\n\n"
        )
        
        if discount_applied:
            original_price = metadata.get('original_price', price)
            discount_percent = metadata.get('discount_percent', 0)
            success_text += f"🎁 <b>Скидка:</b> {discount_percent}% (экономия {float(original_price) - price:.2f}₽)\n\n"
        
        success_text += f"<code>{result['connection_string']}</code>"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
        builder.button(text="🔄 Продлить", callback_data=f"extend_{key_id}")
        builder.button(text="⬅️ В меню", callback_data="back_to_main_menu")
        builder.adjust(2, 1)
        
        await bot.send_message(user_id, success_text, reply_markup=builder.as_markup())
        
        # Уведомление админу
        if ADMIN_ID:
            admin_text = (
                f"🛒 <b>Новая покупка!</b>\n\n"
                f"👤 Пользователь: @{user_data['username'] or 'без username'} ({user_data['full_name']})\n"
                f"🆔 ID: {user_id}\n"
                f"🖥️ Сервер: {host_name}\n"
                f"📦 Тариф: {plan['plan_name']} ({months} месяцев)\n"
                f"💰 Сумма: {price:.2f}₽"
            )
            
            if discount_applied:
                original_price = metadata.get('original_price', price)
                admin_text += f"\n🎁 Со скидкой: {metadata.get('discount_percent', 0)}% (было {original_price:.2f}₽)"
            
            admin_text += f"\n💳 Способ: {payment_method}"
            
            if referrer_id:
                referrer_data = db.get_user(referrer_id)
                admin_text += f"\n🤝 Реферер: @{referrer_data['username'] or 'без username'} ({referrer_id})"
            
            await bot.send_message(ADMIN_ID, admin_text)
        
        logger.info(f"Payment processed successfully for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error processing payment: {e}", exc_info=True)

# ========== ОБРАБОТКА ВЕБХУКОВ ==========

async def handle_yookassa_webhook(request: web.Request):
    """Обработчик вебхуков ЮKassa"""
    try:
        # Проверяем IP (опционально)
        # trusted_ips = ['185.71.76.0/27', '185.71.77.0/27', '77.75.153.0/25', '77.75.154.128/25']
        
        data = await request.json()
        logger.info(f"YooKassa webhook received: {json.dumps(data, ensure_ascii=False)[:500]}")
        
        if data.get('event') == 'payment.succeeded':
            payment_id = data['object']['id']
            
            # Проверяем, не обрабатывали ли мы уже этот платеж
            webhook_tx = db.get_webhook_transaction(payment_id)
            if not webhook_tx:
                logger.warning(f"Unknown payment ID in webhook: {payment_id}")
                return web.Response(text='Unknown payment', status=400)
            
            metadata = data['object']['metadata']
            
            # Запускаем обработку в фоне
            asyncio.create_task(process_successful_payment(metadata))
            
            # Помечаем как обработанное
            db.mark_webhook_processed(payment_id)
        
        return web.Response(text='OK')
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON in YooKassa webhook")
        return web.Response(text='Invalid JSON', status=400)
    except Exception as e:
        logger.error(f"YooKassa webhook error: {e}", exc_info=True)
        return web.Response(text='ERROR', status=500)

async def handle_cryptobot_webhook(request: web.Request):
    """Обработчик вебхуков CryptoBot"""
    try:
        data = await request.json()
        logger.info(f"CryptoBot webhook received: {json.dumps(data, ensure_ascii=False)[:500]}")
        
        if data.get('update_type') == 'invoice_paid':
            invoice_id = data['payload']
            
            # Получаем информацию об инвойсе
            crypto = CryptoPay(CRYPTOBOT_TOKEN)
            invoices = await crypto.get_invoices(invoice_ids=int(invoice_id))
            
            if not invoices:
                logger.warning(f"Invoice not found: {invoice_id}")
                return web.Response(text='Invoice not found', status=400)
            
            invoice = invoices[0]
            
            try:
                metadata = json.loads(invoice.payload)
                payment_id = f"cryptobot_{invoice_id}"
                
                # Создаем запись о вебхуке
                db.create_webhook_transaction(
                    payment_id,
                    metadata['user_id'],
                    invoice.amount,
                    metadata
                )
                
                # Обрабатываем платеж
                asyncio.create_task(process_successful_payment(metadata))
                
            except json.JSONDecodeError:
                logger.error(f"Invalid payload in CryptoBot invoice: {invoice.payload}")
        
        return web.Response(text='OK')
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON in CryptoBot webhook")
        return web.Response(text='Invalid JSON', status=400)
    except Exception as e:
        logger.error(f"CryptoBot webhook error: {e}", exc_info=True)
        return web.Response(text='ERROR', status=500)

# ========== АДМИН ПАНЕЛЬ ==========

@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    user_count = db.get_user_count()
    active_users = db.get_active_users_count()
    total_keys = db.get_total_keys_count()
    active_keys = db.get_active_keys_count()
    total_spent = db.get_total_spent_sum()
    today_revenue = db.get_today_revenue()
    
    text = (
        "👑 <b>Админ панель</b>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• 👥 Пользователей: {user_count}\n"
        f"• 👤 Активных: {active_users}\n"
        f"• 🔑 Ключей: {total_keys}\n"
        f"• ✅ Активных ключей: {active_keys}\n"
        f"• 💰 Выручка всего: {total_spent:.2f}₽\n"
        f"• 📈 Сегодня: {today_revenue:.2f}₽\n\n"
        "Выберите раздел:"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Детальная статистика", callback_data="admin_stats")
    builder.button(text="👥 Управление пользователями", callback_data="admin_users")
    builder.button(text="🖥️ Управление хостами", callback_data="admin_hosts")
    builder.button(text="📦 Управление тарифами", callback_data="admin_plans")
    builder.button(text="📝 Транзакции", callback_data="admin_transactions")
    builder.button(text="⚙️ Настройки", callback_data="admin_settings")
    builder.button(text="⬅️ В меню", callback_data="back_to_main_menu")
    builder.adjust(2, 2, 2, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    user_count = db.get_user_count()
    active_users = db.get_active_users_count()
    total_keys = db.get_total_keys_count()
    active_keys = db.get_active_keys_count()
    total_spent = db.get_total_spent_sum()
    today_revenue = db.get_today_revenue()
    hosts_count = len(db.get_all_hosts())
    plans_count = len(db.get_all_plans())
    
    # Статистика по дням (за последние 7 дней)
    with db._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date(created_date) as day,
                COUNT(*) as transactions,
                SUM(amount_rub) as revenue
            FROM transactions 
            WHERE status = 'paid' 
            AND date(created_date) >= date('now', '-7 days')
            GROUP BY date(created_date)
            ORDER BY day DESC
        """)
        daily_stats = cursor.fetchall()
    
    text = (
        "📊 <b>Детальная статистика</b>\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"• Всего: {user_count}\n"
        f"• Активных: {active_users}\n\n"
        f"🔑 <b>Ключи:</b>\n"
        f"• Всего: {total_keys}\n"
        f"• Активных: {active_keys}\n\n"
        f"💰 <b>Финансы:</b>\n"
        f"• Выручка всего: {total_spent:.2f}₽\n"
        f"• Выручка сегодня: {today_revenue:.2f}₽\n\n"
        f"🖥️ <b>Инфраструктура:</b>\n"
        f"• Серверов: {hosts_count}\n"
        f"• Тарифов: {plans_count}\n\n"
    )
    
    if daily_stats:
        text += "📈 <b>Последние 7 дней:</b>\n"
        for day in daily_stats[:7]:  # Ограничиваем 7 днями
            day_str = day[0]
            transactions = day[1]
            revenue = day[2] or 0
            text += f"• {day_str}: {transactions} тр. на {revenue:.2f}₽\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="admin_stats")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    users = db.get_all_users()[:50]  # Ограничиваем 50 пользователями
    
    text = f"👥 <b>Пользователи</b> (последние {len(users)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for user in users[:20]:  # Показываем только первые 20 в списке
        user_id = user['telegram_id']
        username = user['username'] or user['full_name'] or f"ID: {user_id}"
        status = "🚫" if user.get('is_banned') else "✅"
        created_at = user['created_at']
        if isinstance(created_at, str):
            date_str = created_at[:10]
        else:
            date_str = created_at.strftime('%d.%m.%Y')
        
        text += f"{status} <b>{username}</b>\n"
        text += f"   🆔 {user_id} | 📅 {date_str}\n"
        text += f"   💰 {user['total_spent']:.0f}₽ | 🔑 {len(db.get_user_keys(user_id))}\n"
        
        if user.get('is_banned'):
            text += "   🚫 Заблокирован\n"
        
        text += "\n"
        
        # Добавляем кнопку для управления пользователем
        builder.button(text=f"👤 {user_id}", callback_data=f"admin_view_user_{user_id}")
    
    if len(users) > 20:
        text += f"\n... и еще {len(users) - 20} пользователей"
    
    builder.button(text="➕ Поиск пользователя", callback_data="admin_search_user")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(3, 3, 3, 3, 2, 1)  # Настраиваем расположение кнопок
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_hosts")
async def admin_hosts(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    hosts = db.get_all_hosts()
    
    text = f"🖥️ <b>Хосты</b> ({len(hosts)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for host in hosts:
        plans = db.get_plans_for_host(host['host_name'])
        text += f"🖥️ <b>{host['host_name']}</b>\n"
        text += f"🔗 {host['host_url']}\n"
        text += f"👤 {host['host_username']}\n"
        text += f"🆔 Inbound: {host['host_inbound_id']}\n"
        text += f"📦 Тарифов: {len(plans)}\n\n"
        
        builder.button(text=host['host_name'], callback_data=f"admin_view_host_{host['host_name']}")
    
    builder.button(text="➕ Добавить хост", callback_data="admin_add_host")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_add_host")
async def admin_add_host(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_host_data)
    
    await callback.message.edit_text(
        "🖥️ <b>Добавление нового хоста</b>\n\n"
        "Пришлите данные в формате:\n"
        "<code>Имя_хоста\nURL_X-UI\nЛогин\nПароль\nID_инбаунда</code>\n\n"
        "Пример:\n"
        "<code>Server-1\nhttps://server.com:54321\nadmin\npassword\n1</code>\n\n"
        "❌ Для отмены введите /cancel",
        parse_mode="HTML"
    )

@dp.message(Form.waiting_for_host_data)
async def process_host_data(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    
    if message.text.lower() == '/cancel':
        await state.clear()
        await message.answer("❌ Добавление хоста отменено.")
        return
    
    try:
        lines = message.text.strip().split('\n')
        if len(lines) != 5:
            raise ValueError("Неверный формат данных")
        
        host_name = lines[0].strip()
        host_url = lines[1].strip()
        host_username = lines[2].strip()
        host_pass = lines[3].strip()
        host_inbound_id = int(lines[4].strip())
        
        # Проверяем подключение к хосту
        await message.answer("🔄 Проверяю подключение к хосту...")
        
        try:
            api, inbound = xui_api.login_to_host(host_url, host_username, host_pass, host_inbound_id)
            
            # Сохраняем хост в БД
            db.add_host(host_name, host_url, host_username, host_pass, host_inbound_id)
            
            await message.answer(f"✅ Хост <b>{host_name}</b> успешно добавлен!")
            
            # Предлагаем добавить тарифы
            builder = InlineKeyboardBuilder()
            builder.button(text="📦 Добавить тарифы", callback_data=f"admin_add_plan_{host_name}")
            builder.button(text="⬅️ К хостам", callback_data="admin_hosts")
            
            await message.answer(f"Хотите добавить тарифы для хоста <b>{host_name}</b>?", reply_markup=builder.as_markup())
            
        except Exception as e:
            await message.answer(f"❌ Ошибка подключения к хосту: {str(e)}")
            
    except ValueError as e:
        await message.answer(f"❌ Ошибка: {str(e)}\n\nПроверьте формат данных и попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Error adding host: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data.startswith("admin_view_host_"))
async def admin_view_host(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[3]
    host = db.get_host(host_name)
    
    if not host:
        await callback.answer("Хост не найден", show_alert=True)
        return
    
    plans = db.get_plans_for_host(host_name)
    
    text = (
        f"🖥️ <b>Хост: {host_name}</b>\n\n"
        f"🔗 <b>URL:</b> {host['host_url']}\n"
        f"👤 <b>Логин:</b> {host['host_username']}\n"
        f"🔑 <b>Пароль:</b> {'*' * len(host['host_pass'])}\n"
        f"🆔 <b>Inbound ID:</b> {host['host_inbound_id']}\n\n"
        f"📦 <b>Тарифы ({len(plans)}):</b>\n"
    )
    
    if plans:
        for plan in plans:
            text += f"• {plan['plan_name']} - {plan['months']}м - {plan['price']}₽\n"
    else:
        text += "Нет тарифов\n"
    
    # Получаем количество ключей на этом хосте
    with db._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_keys WHERE host_name = ?", (host_name,))
        key_count = cursor.fetchone()[0]
    
    text += f"\n🔑 <b>Ключей на хосте:</b> {key_count}"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Добавить тариф", callback_data=f"admin_add_plan_{host_name}")
    builder.button(text="✏️ Редактировать", callback_data=f"admin_edit_host_{host_name}")
    builder.button(text="🗑️ Удалить", callback_data=f"admin_delete_host_{host_name}")
    builder.button(text="⬅️ Назад", callback_data="admin_hosts")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("admin_delete_host_"))
async def admin_delete_host(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[3]
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"admin_confirm_delete_host_{host_name}")
    builder.button(text="❌ Нет, отмена", callback_data=f"admin_view_host_{host_name}")
    
    await callback.message.edit_text(
        f"🗑️ <b>Удаление хоста</b>\n\n"
        f"Вы уверены, что хотите удалить хост <b>{host_name}</b>?\n"
        f"⚠️ Это также удалит все связанные тарифы!\n"
        f"⚠️ Ключи пользователей перестанут работать!",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("admin_confirm_delete_host_"))
async def admin_confirm_delete_host(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[4]
    db.delete_host(host_name)
    
    await callback.answer(f"✅ Хост {host_name} удален", show_alert=True)
    await admin_hosts(callback)

@dp.callback_query(F.data.startswith("admin_add_plan_"))
async def admin_add_plan(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[3]
    
    await state.set_state(Form.waiting_for_plan_data)
    await state.update_data(host_name=host_name)
    
    await callback.message.edit_text(
        f"📦 <b>Добавление тарифа для {host_name}</b>\n\n"
        "Пришлите данные в формате:\n"
        "<code>Название_тарифа\nКоличество_месяцев\nЦена_в_рублях</code>\n\n"
        "Пример:\n"
        "<code>Стандарт\n1\n300</code>\n\n"
        "❌ Для отмены введите /cancel",
        parse_mode="HTML"
    )

@dp.message(Form.waiting_for_plan_data)
async def process_plan_data(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    
    if message.text.lower() == '/cancel':
        await state.clear()
        await message.answer("❌ Добавление тарифа отменено.")
        return
    
    try:
        data = await state.get_data()
        host_name = data.get('host_name')
        
        lines = message.text.strip().split('\n')
        if len(lines) != 3:
            raise ValueError("Неверный формат данных")
        
        plan_name = lines[0].strip()
        months = int(lines[1].strip())
        price = float(lines[2].strip())
        
        # Сохраняем тариф в БД
        db.add_plan(host_name, plan_name, months, price)
        
        await message.answer(f"✅ Тариф <b>{plan_name}</b> успешно добавлен для хоста <b>{host_name}</b>!")
        
        # Предлагаем добавить еще тарифы
        builder = InlineKeyboardBuilder()
        builder.button(text="➕ Добавить еще тариф", callback_data=f"admin_add_plan_{host_name}")
        builder.button(text="⬅️ К хосту", callback_data=f"admin_view_host_{host_name}")
        
        await message.answer("Добавить еще один тариф?", reply_markup=builder.as_markup())
        
    except ValueError as e:
        await message.answer(f"❌ Ошибка: {str(e)}\n\nПроверьте формат данных и попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Error adding plan: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "admin_plans")
async def admin_plans(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    plans = db.get_all_plans()
    
    # Группируем планы по хостам
    plans_by_host = {}
    for plan in plans:
        host_name = plan['host_name']
        if host_name not in plans_by_host:
            plans_by_host[host_name] = []
        plans_by_host[host_name].append(plan)
    
    text = f"📦 <b>Тарифы</b> ({len(plans)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for host_name, host_plans in plans_by_host.items():
        text += f"🖥️ <b>{host_name}</b> ({len(host_plans)} тарифов):\n"
        for plan in host_plans[:5]:  # Ограничиваем 5 тарифами на хост
            text += f"• {plan['plan_name']} - {plan['months']}м - {plan['price']}₽\n"
        
        if len(host_plans) > 5:
            text += f"  ... и еще {len(host_plans) - 5}\n"
        
        text += "\n"
        
        builder.button(text=host_name, callback_data=f"admin_view_host_{host_name}")
    
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_transactions")
async def admin_transactions(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    transactions = db.get_all_transactions()
    
    text = f"📝 <b>Транзакции</b> (последние {len(transactions)})\n\n"
    
    total_today = 0
    for tx in transactions[:20]:  # Показываем только 20 транзакций
        created_date = tx['created_date']
        if isinstance(created_date, str):
            date_str = created_date[11:16]  # Время
        else:
            date_str = created_date.strftime('%H:%M')
        
        status_icon = "✅" if tx['status'] == 'paid' else "⏳" if tx['status'] == 'pending' else "❌"
        
        text += f"{status_icon} <b>{tx['username'] or 'Без имени'}</b>\n"
        text += f"   🕒 {date_str} | 💰 {tx['amount_rub']:.2f}₽\n"
        text += f"   💳 {tx['payment_method']}\n\n"
        
        # Суммируем сегодняшние платежи
        if tx['status'] == 'paid':
            tx_date = created_date[:10] if isinstance(created_date, str) else created_date.date()
            today = datetime.now().date()
            if str(tx_date) == str(today):
                total_today += tx['amount_rub']
    
    text += f"\n💰 <b>Сумма сегодня:</b> {total_today:.2f}₽"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="admin_transactions")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    settings = db.get_all_settings()
    
    text = "⚙️ <b>Настройки бота</b>\n\n"
    
    # Группируем настройки
    text += "🔐 <b>Основные:</b>\n"
    for key in ['telegram_bot_token', 'admin_telegram_id', 'telegram_bot_username']:
        if key in settings:
            value = settings[key]
            if key == 'telegram_bot_token' and value:
                value = f"{value[:10]}..." if len(value) > 10 else value
            text += f"• {key}: {value}\n"
    
    text += "\n🎁 <b>Триал:</b>\n"
    for key in ['trial_enabled', 'trial_duration_days']:
        if key in settings:
            text += f"• {key}: {settings[key]}\n"
    
    text += "\n🤝 <b>Рефералы:</b>\n"
    for key in ['enable_referrals', 'referral_percentage', 'referral_discount', 'minimum_withdrawal']:
        if key in settings:
            text += f"• {key}: {settings[key]}\n"
    
    text += "\n💳 <b>Платежные системы:</b>\n"
    payment_keys = ['yookassa_shop_id', 'cryptobot_token', 'heleket_merchant_id', 'ton_wallet_address']
    for key in payment_keys:
        if key in settings:
            value = settings[key]
            status = "✅ Настроено" if value else "❌ Не настроено"
            text += f"• {key}: {status}\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать настройки", callback_data="admin_edit_settings")
    builder.button(text="🔄 Обновить из .env", callback_data="admin_reload_settings")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_edit_settings")
async def admin_edit_settings(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав доступа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_settings)
    
    await callback.message.edit_text(
        "✏️ <b>Редактирование настроек</b>\n\n"
        "Пришлите настройку в формате:\n"
        "<code>ключ=значение</code>\n\n"
        "Пример:\n"
        "<code>trial_duration_days=7</code>\n\n"
        "Доступные ключи:\n"
        "• trial_enabled (true/false)\n"
        "• trial_duration_days (число)\n"
        "• enable_referrals (true/false)\n"
        "• referral_percentage (число)\n"
        "• referral_discount (число)\n"
        "• minimum_withdrawal (число)\n\n"
        "❌ Для отмены введите /cancel",
        parse_mode="HTML"
    )

@dp.message(Form.waiting_for_settings)
async def process_settings(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
    
    if message.text.lower() == '/cancel':
        await state.clear()
        await message.answer("❌ Редактирование настроек отменено.")
        return
    
    try:
        if '=' not in message.text:
            raise ValueError("Неверный формат. Используйте ключ=значение")
        
        key, value = message.text.strip().split('=', 1)
        key = key.strip()
        value = value.strip()
        
        # Проверяем валидность ключа
        valid_keys = {
            'trial_enabled': lambda v: v.lower() in ['true', 'false'],
            'trial_duration_days': lambda v: v.isdigit() and 1 <= int(v) <= 365,
            'enable_referrals': lambda v: v.lower() in ['true', 'false'],
            'referral_percentage': lambda v: v.replace('.', '').isdigit() and 0 <= float(v) <= 100,
            'referral_discount': lambda v: v.replace('.', '').isdigit() and 0 <= float(v) <= 100,
            'minimum_withdrawal': lambda v: v.replace('.', '').isdigit() and float(v) >= 0
        }
        
        if key not in valid_keys:
            raise ValueError(f"Неизвестный ключ: {key}")
        
        if not valid_keys[key](value):
            raise ValueError(f"Неверное значение для {key}")
        
        # Сохраняем настройку
        db.update_setting(key, value)
        
        await message.answer(f"✅ Настройка <b>{key}</b> обновлена на <b>{value}</b>")
        
        # Обновляем глобальные переменные
        global TRIAL_ENABLED, TRIAL_DURATION_DAYS, ENABLE_REFERRALS, REFERRAL_PERCENTAGE, REFERRAL_DISCOUNT, MINIMUM_WITHDRAWAL
        
        if key == 'trial_enabled':
            TRIAL_ENABLED = value.lower() == 'true'
        elif key == 'trial_duration_days':
            TRIAL_DURATION_DAYS = int(value)
        elif key == 'enable_referrals':
            ENABLE_REFERRALS = value.lower() == 'true'
        elif key == 'referral_percentage':
            REFERRAL_PERCENTAGE = float(value)
        elif key == 'referral_discount':
            REFERRAL_DISCOUNT = float(value)
        elif key == 'minimum_withdrawal':
            MINIMUM_WITHDRAWAL = float(value)
        
    except ValueError as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
    except Exception as e:
        logger.error(f"Error updating settings: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")
    
    await state.clear()
    await admin_settings(message)

# ========== ОБРАТНАЯ НАВИГАЦИЯ ==========

@dp.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text("Главное меню:", reply_markup=create_main_menu(user_id))

# ========== ВЕБ-СЕРВЕР ДЛЯ ВЕБХУКОВ ==========

async def start_webhook_server():
    """Запуск веб-сервера для вебхуков"""
    app = web.Application()
    
    # Регистрируем обработчики
    app.router.add_post('/yookassa-webhook', handle_yookassa_webhook)
    app.router.add_post('/cryptobot-webhook', handle_cryptobot_webhook)
    
    # Добавляем health check
    async def health_check(request):
        return web.Response(text='OK')
    
    app.router.add_get('/health', health_check)
    
    # Запускаем сервер
    runner = web.AppRunner(app)
    await runner.setup()
    
    try:
        site = web.TCPSite(runner, '0.0.0.0', WEBHOOK_PORT)
        await site.start()
        
        logger.info(f"✅ Webhook server started on port {WEBHOOK_PORT}")
        logger.info(f"✅ YooKassa webhook: {WEBHOOK_DOMAIN}/yookassa-webhook")
        logger.info(f"✅ CryptoBot webhook: {WEBHOOK_DOMAIN}/cryptobot-webhook")
        
        return runner
    except Exception as e:
        logger.error(f"❌ Failed to start webhook server: {e}")
        raise

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

async def main():
    """Запуск всего приложения"""
    if not BOT_TOKEN or BOT_TOKEN == "ваш_токен_бота":
        print("\n" + "="*60)
        print("❌ ОШИБКА: BOT_TOKEN не настроен!")
        print("Отредактируйте файл .env")
        print("="*60)
        sys.exit(1)
    
    if ADMIN_ID == 0 or ADMIN_ID == 123456789:
        print("⚠️  ВНИМАНИЕ: ADMIN_ID не настроен!")
        print("Установите ваш Telegram ID в файле .env")
    
    # Проверяем наличие необходимых модулей
    try:
        import py3xui
        import yookassa
        import aiosend
    except ImportError as e:
        print(f"❌ Отсутствует необходимый модуль: {e}")
        print("Установите зависимости: pip install -r requirements.txt")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("🚀 Запуск VLESS Telegram Bot...")
    print("="*60)
    
    try:
        # Запускаем веб-сервер для вебхуков
        webhook_runner = await start_webhook_server()
        
        # Получаем информацию о боте
        bot_info = await bot.get_me()
        print(f"\n✅ Бот запущен: @{bot_info.username}")
        print(f"👑 Админ ID: {ADMIN_ID}")
        print(f"🌐 Вебхуки: {WEBHOOK_DOMAIN}:{WEBHOOK_PORT}")
        print(f"🗄️  База данных: vless_bot.db")
        print("\n" + "="*60)
        
        if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
            print("💳 ЮKassa: ✅ Настроено")
        else:
            print("💳 ЮKassa: ❌ Не настроено")
        
        if CRYPTOBOT_TOKEN:
            print("🤖 CryptoBot: ✅ Настроено")
        else:
            print("🤖 CryptoBot: ❌ Не настроено")
        
        print("="*60)
        print("\n📋 Инструкция по настройке вебхуков:")
        print(f"1. ЮKassa: {WEBHOOK_DOMAIN}/yookassa-webhook")
        print(f"2. CryptoBot: {WEBHOOK_DOMAIN}/cryptobot-webhook")
        print("="*60)
        
        # Запускаем polling
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n❌ Критическая ошибка: {e}")
        
    finally:
        # Очистка ресурсов
        try:
            await bot.session.close()
            if 'webhook_runner' in locals():
                await webhook_runner.cleanup()
        except:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {e}")
        sys.exit(1)
