#!/usr/bin/env python3
"""
VLESS Telegram Bot - ПОЛНАЯ РАБОЧАЯ ВЕРСИЯ
Версия 8.0 - Полный функционал со всеми модулями
"""

import asyncio
import logging
import sys
import sqlite3
import json
import uuid
import qrcode
import hashlib
import random
import re
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urlparse, quote
from contextlib import contextmanager

import aiohttp
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import (
    InlineKeyboardMarkup, BufferedInputFile, Message,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
)

# === НАСТРОЙКИ ===
import os
from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "")

# Платежные системы
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")

# X-UI Настройки
DEFAULT_XUI_HOST = os.getenv("DEFAULT_XUI_HOST", "")
DEFAULT_XUI_USERNAME = os.getenv("DEFAULT_XUI_USERNAME", "admin")
DEFAULT_XUI_PASSWORD = os.getenv("DEFAULT_XUI_PASSWORD", "")
DEFAULT_XUI_INBOUND_ID = int(os.getenv("DEFAULT_XUI_INBOUND_ID", "1"))

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
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "true").lower() == "true"
TRIAL_DURATION_DAYS = int(os.getenv("TRIAL_DURATION_DAYS", "3"))
ENABLE_REFERRALS = os.getenv("ENABLE_REFERRALS", "true").lower() == "true"
REFERRAL_PERCENTAGE = float(os.getenv("REFERRAL_PERCENTAGE", "10"))
REFERRAL_DISCOUNT = float(os.getenv("REFERRAL_DISCOUNT", "10"))
MINIMUM_WITHDRAWAL = float(os.getenv("MINIMUM_WITHDRAWAL", "100"))

# === СОСТОЯНИЯ FSM ===
class Form(StatesGroup):
    # Админ состояния
    waiting_for_host_data = State()
    waiting_for_plan_data = State()
    waiting_for_settings = State()
    waiting_for_support_message = State()
    waiting_for_user_search = State()
    waiting_for_mailing = State()
    waiting_for_broadcast = State()
    waiting_for_add_host = State()
    waiting_for_edit_host = State()
    waiting_for_delete_host = State()
    waiting_for_add_plan = State()
    waiting_for_edit_plan = State()
    waiting_for_delete_plan = State()
    waiting_for_edit_setting = State()
    waiting_for_test_xui = State()
    
    # Пользовательские состояния
    waiting_for_withdrawal_details = State()
    waiting_for_support_message = State()

# === БАЗА ДАННЫХ ===
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
                    trial_used BOOLEAN DEFAULT 0,
                    total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    is_banned BOOLEAN DEFAULT 0,
                    language TEXT DEFAULT 'ru',
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
                    host_inbound_id INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Тарифы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    plan_name TEXT NOT NULL,
                    months INTEGER NOT NULL,
                    price REAL NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    is_active BOOLEAN DEFAULT 1,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (telegram_id)
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
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Crypto payments
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crypto_payments (
                    invoice_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    plan_id INTEGER,
                    key_id INTEGER,
                    amount REAL,
                    asset TEXT,
                    status TEXT DEFAULT 'pending',
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (telegram_id),
                    FOREIGN KEY (plan_id) REFERENCES plans (plan_id),
                    FOREIGN KEY (key_id) REFERENCES user_keys (key_id)
                )
            ''')
            
            # Referral withdrawals
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS referral_withdrawals (
                    withdrawal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    status TEXT DEFAULT 'pending',
                    details TEXT,
                    admin_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (telegram_id)
                )
            ''')
            
            # Support messages
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    message TEXT,
                    status TEXT DEFAULT 'open',
                    admin_response TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    responded_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (telegram_id)
                )
            ''')
            
            # Admin logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS admin_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    action TEXT,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    broadcast_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id INTEGER,
                    message TEXT,
                    total_users INTEGER,
                    sent_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            ''')
            
            # Добавляем индексы для производительности
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_keys_user_id ON user_keys(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_keys_expiry ON user_keys(expiry_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(created_date)')
            
            # Добавляем настройки по умолчанию
            default_settings = [
                ("bot_name", "VLESS VPN Bot"),
                ("bot_username", TELEGRAM_BOT_USERNAME),
                ("admin_id", str(ADMIN_ID)),
                ("trial_enabled", "true" if TRIAL_ENABLED else "false"),
                ("trial_duration_days", str(TRIAL_DURATION_DAYS)),
                ("enable_referrals", "true" if ENABLE_REFERRALS else "false"),
                ("referral_percentage", str(REFERRAL_PERCENTAGE)),
                ("referral_discount", str(REFERRAL_DISCOUNT)),
                ("minimum_withdrawal", str(MINIMUM_WITHDRAWAL)),
                ("cryptobot_token", CRYPTOBOT_TOKEN),
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
                ("default_xui_host", DEFAULT_XUI_HOST),
                ("default_xui_username", DEFAULT_XUI_USERNAME),
                ("default_xui_password", DEFAULT_XUI_PASSWORD),
                ("default_xui_inbound_id", str(DEFAULT_XUI_INBOUND_ID))
            ]
            
            for key, value in default_settings:
                cursor.execute('''
                    INSERT OR IGNORE INTO settings (key, value) 
                    VALUES (?, ?)
                ''', (key, value))
            
            # Добавляем дефолтный хост если его нет
            if DEFAULT_XUI_HOST and DEFAULT_XUI_USERNAME and DEFAULT_XUI_PASSWORD:
                cursor.execute('SELECT * FROM hosts WHERE host_name = ?', ("Default Server",))
                if not cursor.fetchone():
                    cursor.execute('''
                        INSERT INTO hosts (host_name, host_url, host_username, host_pass, host_inbound_id)
                        VALUES (?, ?, ?, ?, ?)
                    ''', ("Default Server", DEFAULT_XUI_HOST, DEFAULT_XUI_USERNAME, DEFAULT_XUI_PASSWORD, DEFAULT_XUI_INBOUND_ID))
                    
                    # Добавляем дефолтные планы
                    default_plans = [
                        ("Default Server", "1 Month", 1, 300),
                        ("Default Server", "3 Months", 3, 800),
                        ("Default Server", "6 Months", 6, 1500),
                        ("Default Server", "12 Months", 12, 2800)
                    ]
                    
                    for plan in default_plans:
                        cursor.execute('''
                            INSERT INTO plans (host_name, plan_name, months, price)
                            VALUES (?, ?, ?, ?)
                        ''', plan)
    
    # === ПОЛЬЗОВАТЕЛИ ===
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
    
    def withdraw_referral_balance(self, user_id: int, amount: float, details: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users 
                SET referral_balance = referral_balance - ? 
                WHERE telegram_id = ?
            ''', (amount, user_id))
            
            cursor.execute('''
                INSERT INTO referral_withdrawals (user_id, amount, details)
                VALUES (?, ?, ?)
            ''', (user_id, amount, details))
            return cursor.lastrowid
    
    def get_referral_withdrawals(self, status: str = None) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status:
                cursor.execute('''
                    SELECT rw.*, u.username, u.full_name 
                    FROM referral_withdrawals rw
                    JOIN users u ON rw.user_id = u.telegram_id
                    WHERE rw.status = ?
                    ORDER BY rw.created_at DESC
                ''', (status,))
            else:
                cursor.execute('''
                    SELECT rw.*, u.username, u.full_name 
                    FROM referral_withdrawals rw
                    JOIN users u ON rw.user_id = u.telegram_id
                    ORDER BY rw.created_at DESC
                    LIMIT 100
                ''')
            return [dict(row) for row in cursor.fetchall()]
    
    def update_withdrawal_status(self, withdrawal_id: int, status: str, admin_notes: str = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status == 'completed':
                cursor.execute('''
                    UPDATE referral_withdrawals 
                    SET status = ?, admin_notes = ?, processed_at = CURRENT_TIMESTAMP
                    WHERE withdrawal_id = ?
                ''', (status, admin_notes, withdrawal_id))
            else:
                cursor.execute('''
                    UPDATE referral_withdrawals 
                    SET status = ?, admin_notes = ?
                    WHERE withdrawal_id = ?
                ''', (status, admin_notes, withdrawal_id))
    
    def get_referrals(self, referrer_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM users 
                WHERE referred_by = ? 
                ORDER BY created_at DESC
            ''', (referrer_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def search_users(self, query: str) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if query.isdigit():
                cursor.execute(
                    "SELECT * FROM users WHERE telegram_id = ?",
                    (int(query),)
                )
                results = cursor.fetchall()
                if results:
                    return [dict(row) for row in results]
            
            search_term = f"%{query}%"
            cursor.execute(
                "SELECT * FROM users WHERE username LIKE ? OR full_name LIKE ? ORDER BY telegram_id DESC LIMIT 20",
                (search_term, search_term)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_users(self, limit: int = 1000) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_users_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]
    
    def get_active_users(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE is_banned = 0 ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_banned_users(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE is_banned = 1 ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_today_users(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE date(created_at) = date('now')")
            return cursor.fetchone()[0]
    
    def get_user_activity_stats(self, days: int = 7) -> Dict:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT user_id) as active_users,
                    COUNT(*) as total_keys_created
                FROM user_keys 
                WHERE created_date >= datetime('now', ?)
            ''', (f'-{days} days',))
            return dict(cursor.fetchone())
    
    # === КЛЮЧИ ===
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
            cursor.execute('''
                SELECT * FROM user_keys 
                WHERE user_id = ? 
                ORDER BY created_date DESC
            ''', (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_key_by_id(self, key_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM user_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_active_user_keys(self, user_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM user_keys 
                WHERE user_id = ? AND expiry_date > datetime('now')
                ORDER BY expiry_date DESC
            ''', (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    
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
    
    def get_active_keys_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys WHERE expiry_date > datetime('now') AND is_active = 1")
            return cursor.fetchone()[0]
    
    def get_expiring_keys(self, days: int = 7) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT uk.*, u.username, u.full_name 
                FROM user_keys uk
                JOIN users u ON uk.user_id = u.telegram_id
                WHERE uk.expiry_date BETWEEN datetime('now') AND datetime('now', ?)
                AND uk.is_active = 1
                ORDER BY uk.expiry_date ASC
            ''', (f'+{days} days',))
            return [dict(row) for row in cursor.fetchall()]
    
    # === ХОСТЫ ===
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
            cursor.execute("SELECT * FROM hosts WHERE is_active = 1 ORDER BY host_name")
            return [dict(row) for row in cursor.fetchall()]
    
    def get_host(self, host_name: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM hosts WHERE host_name = ?", (host_name,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_host_by_id(self, host_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM hosts WHERE host_id = ?", (host_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_host(self, host_name: str, **kwargs):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values())
            values.append(host_name)
            cursor.execute(f"UPDATE hosts SET {set_clause} WHERE host_name = ?", values)
    
    def delete_host(self, host_name: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE hosts SET is_active = 0 WHERE host_name = ?", (host_name,))
            cursor.execute("UPDATE plans SET is_active = 0 WHERE host_name = ?", (host_name,))
    
    def get_hosts_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM hosts WHERE is_active = 1")
            return cursor.fetchone()[0]
    
    # === ТАРИФЫ ===
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
            cursor.execute('''
                SELECT * FROM plans 
                WHERE host_name = ? AND is_active = 1 
                ORDER BY price
            ''', (host_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_plan_by_id(self, plan_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_all_plans(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM plans 
                WHERE is_active = 1 
                ORDER BY host_name, price
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
    def update_plan(self, plan_id: int, **kwargs):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            set_clause = ', '.join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values())
            values.append(plan_id)
            cursor.execute(f"UPDATE plans SET {set_clause} WHERE plan_id = ?", values)
    
    def delete_plan(self, plan_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE plans SET is_active = 0 WHERE plan_id = ?", (plan_id,))
    
    def get_plans_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM plans WHERE is_active = 1")
            return cursor.fetchone()[0]
    
    # === ТРАНЗАКЦИИ ===
    def log_transaction(self, username: str, user_id: int, status: str, 
                       amount_rub: float, payment_method: str, metadata: dict):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO transactions 
                (username, user_id, status, amount_rub, payment_method, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, user_id, status, amount_rub, payment_method, json.dumps(metadata)))
    
    def get_all_transactions(self, limit: int = 100) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT t.*, u.username, u.full_name 
                FROM transactions t
                LEFT JOIN users u ON t.user_id = u.telegram_id
                ORDER BY t.created_date DESC 
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_user_transactions(self, user_id: int, limit: int = 50) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM transactions 
                WHERE user_id = ? 
                ORDER BY created_date DESC 
                LIMIT ?
            ''', (user_id, limit))
            return [dict(row) for row in cursor.fetchall()]
    
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
    
    def get_week_revenue(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT SUM(amount_rub) 
                FROM transactions 
                WHERE created_date >= datetime('now', '-7 days') 
                AND status = 'paid'
            """)
            result = cursor.fetchone()[0]
            return result if result else 0.0
    
    def get_month_revenue(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT SUM(amount_rub) 
                FROM transactions 
                WHERE created_date >= datetime('now', '-30 days') 
                AND status = 'paid'
            """)
            result = cursor.fetchone()[0]
            return result if result else 0.0
    
    def get_total_revenue(self) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_spent) FROM users")
            result = cursor.fetchone()[0]
            return result if result else 0.0
    
    def get_revenue_stats(self, days: int = 30) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    date(created_date) as date,
                    COUNT(*) as transactions,
                    SUM(amount_rub) as revenue
                FROM transactions 
                WHERE created_date >= datetime('now', ?)
                AND status = 'paid'
                GROUP BY date(created_date)
                ORDER BY date DESC
            ''', (f'-{days} days',))
            return [dict(row) for row in cursor.fetchall()]
    
    # === CRYPTO PAYMENTS ===
    def create_crypto_payment(self, invoice_id: str, user_id: int, plan_id: int, 
                            amount: float, asset: str, metadata: dict, key_id: int = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO crypto_payments 
                (invoice_id, user_id, plan_id, key_id, amount, asset, metadata) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (invoice_id, user_id, plan_id, key_id, amount, asset, json.dumps(metadata)))
    
    def get_crypto_payment(self, invoice_id: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM crypto_payments WHERE invoice_id = ?", (invoice_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def update_crypto_payment_status(self, invoice_id: str, status: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE crypto_payments 
                SET status = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE invoice_id = ?
            ''', (status, invoice_id))
    
    def get_pending_payments(self) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT cp.*, u.username, u.full_name, p.plan_name, p.host_name
                FROM crypto_payments cp
                JOIN users u ON cp.user_id = u.telegram_id
                JOIN plans p ON cp.plan_id = p.plan_id
                WHERE cp.status = 'pending'
                ORDER BY cp.created_at DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]
    
    def get_payments_by_status(self, status: str, limit: int = 100) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT cp.*, u.username, u.full_name, p.plan_name, p.host_name
                FROM crypto_payments cp
                JOIN users u ON cp.user_id = u.telegram_id
                JOIN plans p ON cp.plan_id = p.plan_id
                WHERE cp.status = ?
                ORDER BY cp.created_at DESC
                LIMIT ?
            ''', (status, limit))
            return [dict(row) for row in cursor.fetchall()]
    
    # === НАСТРОЙКИ ===
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
            cursor.execute("SELECT key, value FROM settings ORDER BY key")
            return {row[0]: row[1] for row in cursor.fetchall()}
    
    def delete_setting(self, key: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM settings WHERE key = ?", (key,))
    
    # === SUPPORT MESSAGES ===
    def create_support_message(self, user_id: int, message: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO support_messages (user_id, message)
                VALUES (?, ?)
            ''', (user_id, message))
            return cursor.lastrowid
    
    def get_support_messages(self, status: str = None, limit: int = 100) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status:
                cursor.execute('''
                    SELECT sm.*, u.username, u.full_name 
                    FROM support_messages sm
                    JOIN users u ON sm.user_id = u.telegram_id
                    WHERE sm.status = ?
                    ORDER BY sm.created_at DESC
                    LIMIT ?
                ''', (status, limit))
            else:
                cursor.execute('''
                    SELECT sm.*, u.username, u.full_name 
                    FROM support_messages sm
                    JOIN users u ON sm.user_id = u.telegram_id
                    ORDER BY sm.created_at DESC
                    LIMIT ?
                ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def update_support_message(self, message_id: int, status: str, admin_response: str = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status == 'closed':
                cursor.execute('''
                    UPDATE support_messages 
                    SET status = ?, admin_response = ?, responded_at = CURRENT_TIMESTAMP
                    WHERE message_id = ?
                ''', (status, admin_response, message_id))
            else:
                cursor.execute('''
                    UPDATE support_messages 
                    SET status = ?, admin_response = ?
                    WHERE message_id = ?
                ''', (status, admin_response, message_id))
    
    # === BROADCASTS ===
    def create_broadcast(self, admin_id: int, message: str, total_users: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO broadcasts (admin_id, message, total_users)
                VALUES (?, ?, ?)
            ''', (admin_id, message, total_users))
            return cursor.lastrowid
    
    def update_broadcast_stats(self, broadcast_id: int, sent: int = 0, failed: int = 0, 
                              status: str = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status:
                if status == 'completed':
                    cursor.execute('''
                        UPDATE broadcasts 
                        SET sent_count = sent_count + ?, 
                            failed_count = failed_count + ?,
                            status = ?,
                            completed_at = CURRENT_TIMESTAMP
                        WHERE broadcast_id = ?
                    ''', (sent, failed, status, broadcast_id))
                else:
                    cursor.execute('''
                        UPDATE broadcasts 
                        SET sent_count = sent_count + ?, 
                            failed_count = failed_count + ?,
                            status = ?
                        WHERE broadcast_id = ?
                    ''', (sent, failed, status, broadcast_id))
            else:
                cursor.execute('''
                    UPDATE broadcasts 
                    SET sent_count = sent_count + ?, 
                        failed_count = failed_count + ?
                    WHERE broadcast_id = ?
                ''', (sent, failed, broadcast_id))
    
    def get_broadcasts(self, limit: int = 50) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.*, u.username as admin_username
                FROM broadcasts b
                LEFT JOIN users u ON b.admin_id = u.telegram_id
                ORDER BY b.created_at DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    # === ADMIN LOGS ===
    def log_admin_action(self, admin_id: int, action: str, details: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO admin_logs (admin_id, action, details)
                VALUES (?, ?, ?)
            ''', (admin_id, action, details))
    
    def get_admin_logs(self, limit: int = 100) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT al.*, u.username, u.full_name 
                FROM admin_logs al
                LEFT JOIN users u ON al.admin_id = u.telegram_id
                ORDER BY al.created_at DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    # === СТАТИСТИКА ===
    def get_user_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]
    
    def get_active_users_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = 0")
            return cursor.fetchone()[0]
    
    def get_banned_users_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            return cursor.fetchone()[0]
    
    def get_total_keys_count(self) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM user_keys")
            return cursor.fetchone()[0]
    
    def get_stats_summary(self) -> Dict:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    (SELECT COUNT(*) FROM users) as total_users,
                    (SELECT COUNT(*) FROM users WHERE is_banned = 0) as active_users,
                    (SELECT COUNT(*) FROM users WHERE is_banned = 1) as banned_users,
                    (SELECT COUNT(*) FROM user_keys) as total_keys,
                    (SELECT COUNT(*) FROM user_keys WHERE expiry_date > datetime('now')) as active_keys,
                    (SELECT SUM(total_spent) FROM users) as total_revenue,
                    (SELECT SUM(amount_rub) FROM transactions WHERE date(created_date) = date('now') AND status = 'paid') as today_revenue,
                    (SELECT COUNT(*) FROM users WHERE date(created_at) = date('now')) as today_users,
                    (SELECT COUNT(*) FROM crypto_payments WHERE status = 'pending') as pending_payments,
                    (SELECT COUNT(*) FROM referral_withdrawals WHERE status = 'pending') as pending_withdrawals
            ''')
            return dict(cursor.fetchone())

# === X-UI API ===
class XUIAPI:
    def __init__(self):
        self.sessions = {}
    
    async def _make_request(self, url: str, method: str = "GET", data: Dict = None, 
                          username: str = None, password: str = None) -> Tuple[bool, Any]:
        """Универсальный метод для запросов к X-UI"""
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                auth = aiohttp.BasicAuth(username, password) if username and password else None
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                }
                
                if method.upper() == "GET":
                    async with session.get(url, auth=auth, headers=headers, ssl=False) as response:
                        if response.status == 200:
                            try:
                                return True, await response.json()
                            except:
                                return True, await response.text()
                        else:
                            return False, f"HTTP {response.status}: {await response.text()}"
                
                elif method.upper() == "POST":
                    async with session.post(url, json=data, auth=auth, headers=headers, ssl=False) as response:
                        if response.status == 200:
                            try:
                                return True, await response.json()
                            except:
                                return True, await response.text()
                        else:
                            return False, f"HTTP {response.status}: {await response.text()}"
                
                elif method.upper() == "PUT":
                    async with session.put(url, json=data, auth=auth, headers=headers, ssl=False) as response:
                        if response.status == 200:
                            try:
                                return True, await response.json()
                            except:
                                return True, await response.text()
                        else:
                            return False, f"HTTP {response.status}: {await response.text()}"
                
                else:
                    return False, f"Unsupported method: {method}"
                    
        except asyncio.TimeoutError:
            return False, "Request timeout"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    async def test_connection(self, host_url: str, username: str, password: str) -> Tuple[bool, str]:
        """Тестирование подключения к X-UI"""
        try:
            # Проверяем доступность панели
            login_url = f"{host_url}/login"
            success, result = await self._make_request(login_url, "GET")
            
            if not success:
                return False, f"Panel not accessible: {result}"
            
            # Пробуем получить список инбаундов
            inbounds_url = f"{host_url}/panel/api/inbounds"
            success, result = await self._make_request(inbounds_url, "GET", username=username, password=password)
            
            if success:
                # Проверяем наличие success поля в ответе
                if isinstance(result, dict) and result.get('success'):
                    return True, "Connection successful"
                else:
                    return False, "Invalid response format from panel"
            else:
                return False, f"Auth failed: {result}"
                
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    async def get_inbounds(self, host_data: Dict) -> Dict:
        """Получение списка инбаундов"""
        try:
            host_url = host_data['host_url']
            username = host_data['host_username']
            password = host_data['host_pass']
            
            inbounds_url = f"{host_url}/panel/api/inbounds"
            success, result = await self._make_request(inbounds_url, "GET", username=username, password=password)
            
            if success:
                if isinstance(result, dict) and result.get('success'):
                    return {"success": True, "inbounds": result.get('obj', [])}
                else:
                    return {"error": "Invalid response format"}
            else:
                return {"error": f"Failed to get inbounds: {result}"}
                
        except Exception as e:
            logging.error(f"X-UI get inbounds error: {e}")
            return {"error": str(e)}
    
    async def create_client(self, host_data: Dict, email: str, days: int, flow: str = "xtls-rprx-vision") -> Dict:
        """Создание нового клиента в X-UI"""
        try:
            host_url = host_data['host_url']
            username = host_data['host_username']
            password = host_data['host_pass']
            inbound_id = host_data['host_inbound_id']
            
            # Получаем текущий инбаунд
            inbound_url = f"{host_url}/panel/api/inbounds/get/{inbound_id}"
            success, inbound_result = await self._make_request(inbound_url, "GET", username=username, password=password)
            
            if not success:
                return {"error": f"Failed to get inbound: {inbound_result}"}
            
            # Парсим настройки инбаунда
            if not isinstance(inbound_result, dict) or not inbound_result.get('success'):
                return {"error": "Invalid inbound response"}
            
            inbound_obj = inbound_result.get('obj', {})
            inbound_settings = inbound_obj.get('settings', '{}')
            
            try:
                settings_dict = json.loads(inbound_settings) if isinstance(inbound_settings, str) else inbound_settings
            except:
                settings_dict = {}
            
            clients = settings_dict.get('clients', [])
            
            # Генерируем UUID для клиента
            client_uuid = str(uuid.uuid4())
            
            # Рассчитываем время истечения (в миллисекундах)
            expiry_time = int((datetime.now() + timedelta(days=days)).timestamp() * 1000)
            
            # Создаем нового клиента
            new_client = {
                "id": client_uuid,
                "email": email,
                "enable": True,
                "flow": flow,
                "totalGB": 0,
                "expiryTime": expiry_time,
                "limitIp": 0,
                "fingerprint": "chrome",
                "tgId": "",
                "subId": ""
            }
            
            # Добавляем клиента в список
            clients.append(new_client)
            
            # Обновляем настройки инбаунда
            settings_dict['clients'] = clients
            
            # Подготавливаем данные для обновления
            update_data = {
                "settings": json.dumps(settings_dict)
            }
            
            # Обновляем инбаунд
            update_url = f"{host_url}/panel/api/inbounds/update/{inbound_id}"
            success, update_result = await self._make_request(update_url, "POST", data=update_data, 
                                                            username=username, password=password)
            
            if success:
                # Генерируем connection string
                connection_string = await self._generate_connection_string(
                    host_data, client_uuid, email, inbound_obj
                )
                
                return {
                    "success": True,
                    "client_uuid": client_uuid,
                    "email": email,
                    "expiry_date": datetime.fromtimestamp(expiry_time / 1000),
                    "connection_string": connection_string,
                    "host_name": host_data['host_name']
                }
            else:
                return {"error": f"Failed to update inbound: {update_result}"}
                
        except Exception as e:
            logging.error(f"X-UI create client error: {e}")
            return {"error": str(e)}
    
    async def update_client_expiry(self, host_data: Dict, client_id: str, days_to_add: int) -> Dict:
        """Обновление срока действия клиента"""
        try:
            host_url = host_data['host_url']
            username = host_data['host_username']
            password = host_data['host_pass']
            inbound_id = host_data['host_inbound_id']
            
            # Получаем текущий инбаунд
            inbound_url = f"{host_url}/panel/api/inbounds/get/{inbound_id}"
            success, inbound_result = await self._make_request(inbound_url, "GET", username=username, password=password)
            
            if not success:
                return {"error": f"Failed to get inbound: {inbound_result}"}
            
            inbound_obj = inbound_result.get('obj', {})
            inbound_settings = inbound_obj.get('settings', '{}')
            
            try:
                settings_dict = json.loads(inbound_settings) if isinstance(inbound_settings, str) else inbound_settings
            except:
                settings_dict = {}
            
            clients = settings_dict.get('clients', [])
            
            # Ищем клиента по ID
            client_found = None
            client_index = -1
            
            for i, client in enumerate(clients):
                if client.get('id') == client_id:
                    client_found = client
                    client_index = i
                    break
            
            if not client_found:
                return {"error": "Client not found"}
            
            # Обновляем срок действия
            current_expiry = client_found.get('expiryTime', 0)
            if current_expiry > 0:
                new_expiry = current_expiry + (days_to_add * 24 * 60 * 60 * 1000)
            else:
                new_expiry = int((datetime.now() + timedelta(days=days_to_add)).timestamp() * 1000)
            
            clients[client_index]['expiryTime'] = new_expiry
            
            # Обновляем настройки
            settings_dict['clients'] = clients
            
            update_data = {
                "settings": json.dumps(settings_dict)
            }
            
            update_url = f"{host_url}/panel/api/inbounds/update/{inbound_id}"
            success, update_result = await self._make_request(update_url, "POST", data=update_data, 
                                                            username=username, password=password)
            
            if success:
                return {
                    "success": True,
                    "client_uuid": client_id,
                    "expiry_date": datetime.fromtimestamp(new_expiry / 1000)
                }
            else:
                return {"error": f"Failed to update client: {update_result}"}
                
        except Exception as e:
            logging.error(f"X-UI update client error: {e}")
            return {"error": str(e)}
    
    async def delete_client(self, host_data: Dict, client_id: str) -> Dict:
        """Удаление клиента из X-UI"""
        try:
            host_url = host_data['host_url']
            username = host_data['host_username']
            password = host_data['host_pass']
            inbound_id = host_data['host_inbound_id']
            
            # Получаем инбаунд
            inbound_url = f"{host_url}/panel/api/inbounds/get/{inbound_id}"
            success, inbound_result = await self._make_request(inbound_url, "GET", username=username, password=password)
            
            if not success:
                return {"error": f"Failed to get inbound: {inbound_result}"}
            
            inbound_obj = inbound_result.get('obj', {})
            inbound_settings = inbound_obj.get('settings', '{}')
            
            try:
                settings_dict = json.loads(inbound_settings) if isinstance(inbound_settings, str) else inbound_settings
            except:
                settings_dict = {}
            
            clients = settings_dict.get('clients', [])
            
            # Удаляем клиента по ID
            filtered_clients = [c for c in clients if c.get('id') != client_id]
            
            if len(filtered_clients) == len(clients):
                return {"error": "Client not found"}
            
            # Обновляем настройки
            settings_dict['clients'] = filtered_clients
            
            update_data = {
                "settings": json.dumps(settings_dict)
            }
            
            update_url = f"{host_url}/panel/api/inbounds/update/{inbound_id}"
            success, update_result = await self._make_request(update_url, "POST", data=update_data, 
                                                            username=username, password=password)
            
            if success:
                return {"success": True, "message": "Client deleted successfully"}
            else:
                return {"error": f"Failed to delete client: {update_result}"}
                
        except Exception as e:
            logging.error(f"X-UI delete client error: {e}")
            return {"error": str(e)}
    
    async def _generate_connection_string(self, host_data: Dict, client_uuid: str, 
                                        email: str, inbound_data: Dict) -> str:
        """Генерация VLESS connection string"""
        try:
            host_url = host_data['host_url']
            host_name = host_data['host_name']
            
            # Парсим URL хоста
            parsed_url = urlparse(host_url)
            hostname = parsed_url.hostname
            
            if not hostname:
                hostname = host_url.replace("https://", "").replace("http://", "").split(":")[0]
            
            # Получаем порт из инбаунда
            port = inbound_data.get('port', 443)
            
            # Получаем настройки stream
            stream_settings = inbound_data.get('streamSettings', {})
            
            # Определяем тип безопасности
            security = stream_settings.get('security', 'tls')
            
            if security == 'reality':
                reality_settings = stream_settings.get('realitySettings', {})
                public_key = reality_settings.get('publicKey', '')
                server_names = reality_settings.get('serverNames', [])
                short_ids = reality_settings.get('shortIds', [])
                fingerprint = reality_settings.get('fingerprint', 'chrome')
                
                if public_key and server_names and short_ids:
                    sni = server_names[0] if server_names else hostname
                    short_id = short_ids[0] if short_ids else ''
                    
                    conn_str = (
                        f"vless://{client_uuid}@{hostname}:{port}"
                        f"?type=tcp&security=reality&pbk={public_key}&fp={fingerprint}&sni={sni}"
                        f"&sid={short_id}&spx=%2F&flow=xtls-rprx-vision"
                    )
                else:
                    # Если reality настройки неполные, используем tls
                    security = 'tls'
            
            if security == 'tls':
                tls_settings = stream_settings.get('tlsSettings', {})
                server_name = tls_settings.get('serverName', hostname)
                fingerprint = 'chrome'
                
                conn_str = (
                    f"vless://{client_uuid}@{hostname}:{port}"
                    f"?type=tcp&security=tls&fp={fingerprint}&sni={server_name}&flow=xtls-rprx-vision"
                )
            
            # Добавляем remark (название)
            remark = email if email else host_name
            conn_str += f"#{quote(remark)}"
            
            return conn_str
            
        except Exception as e:
            logging.error(f"Error generating connection string: {e}")
            # Возвращаем базовую строку
            return f"vless://{client_uuid}@{hostname}:443?type=tcp&security=tls&flow=xtls-rprx-vision#{quote(host_name)}"
    
    async def get_inbound_stats(self, host_data: Dict, inbound_id: int = None) -> Dict:
        """Получение статистики инбаунда"""
        try:
            host_url = host_data['host_url']
            username = host_data['host_username']
            password = host_data['host_pass']
            
            if not inbound_id:
                inbound_id = host_data.get('host_inbound_id', 1)
            
            stats_url = f"{host_url}/panel/api/inbounds/getClientTraffics/{inbound_id}"
            success, result = await self._make_request(stats_url, "GET", username=username, password=password)
            
            if success:
                if isinstance(result, dict) and result.get('success'):
                    return {"success": True, "stats": result.get('obj', [])}
                else:
                    return {"success": True, "stats": result}
            else:
                return {"error": f"Failed to get stats: {result}"}
                
        except Exception as e:
            logging.error(f"X-UI stats error: {e}")
            return {"error": str(e)}

# === CRYPTOBOT API ===
class CryptoBotAPI:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://pay.crypt.bot/api"
        self.session = None
    
    async def get_session(self):
        """Создание сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers={
                'Crypto-Pay-API-Token': self.api_token,
                'Content-Type': 'application/json'
            })
        return self.session
    
    async def create_invoice(self, amount: float, asset: str = "USDT", 
                           description: str = "VPN Service", 
                           hidden_message: str = "Thank you for your purchase!") -> Dict:
        """Создание инвойса в CryptoBot"""
        try:
            session = await self.get_session()
            
            payload = {
                "asset": asset,
                "amount": str(amount),
                "description": description,
                "hidden_message": hidden_message,
                "expires_in": 3600,  # 1 час
                "paid_btn_name": "callback",
                "paid_btn_url": "https://t.me/vless_vpn_bot",
                "allow_comments": False
            }
            
            async with session.post(f"{self.base_url}/createInvoice", json=payload) as response:
                data = await response.json()
                
                if data.get('ok'):
                    return {"success": True, "invoice": data['result']}
                else:
                    error = data.get('error', {})
                    return {"error": f"CryptoBot error: {error.get('name', 'Unknown error')}"}
                    
        except Exception as e:
            logging.error(f"CryptoBot create invoice error: {e}")
            return {"error": str(e)}
    
    async def get_invoices(self, invoice_ids: List[str] = None, status: str = None) -> Dict:
        """Получение информации об инвойсах"""
        try:
            session = await self.get_session()
            
            params = {}
            if invoice_ids:
                params['invoice_ids'] = ','.join(invoice_ids)
            if status:
                params['status'] = status
            
            async with session.get(f"{self.base_url}/getInvoices", params=params) as response:
                data = await response.json()
                
                if data.get('ok'):
                    return {"success": True, "invoices": data['result']['items']}
                else:
                    error = data.get('error', {})
                    return {"error": f"CryptoBot error: {error.get('name', 'Unknown error')}"}
                    
        except Exception as e:
            logging.error(f"CryptoBot get invoices error: {e}")
            return {"error": str(e)}
    
    async def get_exchange_rates(self) -> Dict:
        """Получение курсов валют"""
        try:
            session = await self.get_session()
            
            async with session.get(f"{self.base_url}/getExchangeRates") as response:
                data = await response.json()
                
                if data.get('ok'):
                    return {"success": True, "rates": data['result']}
                else:
                    error = data.get('error', {})
                    return {"error": f"CryptoBot error: {error.get('name', 'Unknown error')}"}
                    
        except Exception as e:
            logging.error(f"CryptoBot get rates error: {e}")
            return {"error": str(e)}
    
    async def get_balance(self) -> Dict:
        """Получение баланса"""
        try:
            session = await self.get_session()
            
            async with session.get(f"{self.base_url}/getBalance") as response:
                data = await response.json()
                
                if data.get('ok'):
                    return {"success": True, "balance": data['result']}
                else:
                    error = data.get('error', {})
                    return {"error": f"CryptoBot error: {error.get('name', 'Unknown error')}"}
                    
        except Exception as e:
            logging.error(f"CryptoBot get balance error: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

# === ИНИЦИАЛИЗАЦИЯ ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Инициализация компонентов
db = Database()
xui_api = XUIAPI()
crypto_bot = CryptoBotAPI(CRYPTOBOT_TOKEN) if CRYPTOBOT_TOKEN else None

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# === УТИЛИТЫ ===
def create_qr_code(connection_string: str) -> BytesIO:
    """Создание QR-кода"""
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

def format_date(date_str_or_dt):
    """Форматирование даты"""
    if isinstance(date_str_or_dt, str):
        try:
            dt = datetime.fromisoformat(date_str_or_dt.replace('Z', '+00:00'))
        except:
            dt = datetime.now()
    else:
        dt = date_str_or_dt
    
    return dt.strftime('%d.%m.%Y %H:%M')

def format_date_short(date_str_or_dt):
    """Короткое форматирование даты"""
    if isinstance(date_str_or_dt, str):
        try:
            dt = datetime.fromisoformat(date_str_or_dt.replace('Z', '+00:00'))
        except:
            dt = datetime.now()
    else:
        dt = date_str_or_dt
    
    return dt.strftime('%d.%m.%Y')

def calculate_days_left(expiry_date):
    """Расчет оставшихся дней"""
    if isinstance(expiry_date, str):
        try:
            expiry = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
        except:
            expiry = datetime.now()
    else:
        expiry = expiry_date
    
    now = datetime.now()
    if expiry < now:
        return 0
    
    delta = expiry - now
    return delta.days

def format_price(price: float) -> str:
    """Форматирование цены"""
    if price.is_integer():
        return f"{int(price)}"
    else:
        return f"{price:.2f}"

def escape_markdown(text: str) -> str:
    """Экранирование символов Markdown"""
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

def create_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для админа"""
    keyboard = [
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="👥 Пользователи")],
        [KeyboardButton(text="🖥️ Хосты"), KeyboardButton(text="📦 Тарифы")],
        [KeyboardButton(text="💳 Транзакции"), KeyboardButton(text="📝 Заявки")],
        [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="📢 Рассылка")],
        [KeyboardButton(text="📋 Логи"), KeyboardButton(text="🚪 Выйти из админки")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# === КЛАВИАТУРЫ ===
def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Создание главного меню"""
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

def create_back_button(target: str = "back_to_main_menu") -> InlineKeyboardMarkup:
    """Создание кнопки Назад"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=target)
    return builder.as_markup()

def create_hosts_menu(hosts: List[Dict], action_prefix: str = "select_host_") -> InlineKeyboardMarkup:
    """Создание меню выбора хоста"""
    builder = InlineKeyboardBuilder()
    
    for host in hosts:
        builder.button(text=host['host_name'], callback_data=f"{action_prefix}{host['host_name']}")
    
    builder.button(text="⬅️ Назад", callback_data="buy_new_key")
    builder.adjust(1)
    return builder.as_markup()

def create_plans_menu(plans: List[Dict]) -> InlineKeyboardMarkup:
    """Создание меню выбора тарифа"""
    builder = InlineKeyboardBuilder()
    
    for plan in plans:
        price_text = f"{int(plan['price'])}₽" if plan['price'].is_integer() else f"{plan['price']:.2f}₽"
        builder.button(
            text=f"{plan['plan_name']} - {price_text}",
            callback_data=f"select_plan_{plan['plan_id']}"
        )
    
    builder.button(text="⬅️ Назад", callback_data="buy_new_key")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_main_menu() -> InlineKeyboardMarkup:
    """Главное меню админ панели"""
    builder = InlineKeyboardBuilder()
    
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🖥️ Хосты", callback_data="admin_hosts")
    builder.button(text="📦 Тарифы", callback_data="admin_plans")
    builder.button(text="💳 Транзакции", callback_data="admin_transactions")
    builder.button(text="📝 Заявки на вывод", callback_data="admin_withdrawals")
    builder.button(text="💬 Поддержка", callback_data="admin_support")
    builder.button(text="🤖 Платежи CryptoBot", callback_data="admin_crypto_payments")
    builder.button(text="📢 Рассылка", callback_data="admin_broadcast")
    builder.button(text="⚙️ Настройки", callback_data="admin_settings")
    builder.button(text="📋 Логи", callback_data="admin_logs")
    builder.button(text="🚪 Выйти из админки", callback_data="back_to_main_menu")
    
    builder.adjust(2, 2, 2, 2, 2, 1)
    return builder.as_markup()

# === ОСНОВНЫЕ ОБРАБОТЧИКИ ===
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
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
    """Обработчик команды /menu"""
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    
    if user_data and user_data.get('is_banned'):
        await message.answer("❌ Вы заблокированы")
        return
    
    await message.answer("Главное меню:", reply_markup=create_main_menu(user_id))

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Обработчик команды /admin"""
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав доступа к админ панели.")
        return
    
    await message.answer(
        "👑 <b>Административная панель</b>\n\n"
        "Выберите раздел:",
        reply_markup=create_admin_main_menu()
    )

# === ОБРАБОТЧИКИ CALLBACK ===
@dp.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu_handler(callback: types.CallbackQuery):
    """Обработчик кнопки Назад в главное меню"""
    await callback.message.edit_text(
        "Главное меню:", 
        reply_markup=create_main_menu(callback.from_user.id)
    )
    await callback.answer()

@dp.callback_query(F.data == "show_profile")
async def show_profile_handler(callback: types.CallbackQuery):
    """Показать профиль пользователя"""
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
    
    created_date = datetime.fromisoformat(user_data['created_at']) if isinstance(user_data.get('created_at'), str) else user_data.get('created_at', datetime.now())
    
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
    await callback.answer()

@dp.callback_query(F.data == "manage_keys")
async def manage_keys_handler(callback: types.CallbackQuery):
    """Управление ключами пользователя"""
    user_id = callback.from_user.id
    user_keys = db.get_user_keys(user_id)
    
    if not user_keys:
        text = "🔑 У вас пока нет ключей VPN.\n\nНажмите кнопку ниже, чтобы приобрести ключ:"
        builder = InlineKeyboardBuilder()
        builder.button(text="🛒 Купить VPN", callback_data="buy_new_key")
        if TRIAL_ENABLED and not db.get_user(user_id).get('trial_used'):
            builder.button(text="🎁 Попробовать бесплатно", callback_data="get_trial")
        builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
        builder.adjust(1)
    else:
        now = datetime.now()
        text = "🔑 <b>Ваши ключи VPN:</b>\n\n"
        
        builder = InlineKeyboardBuilder()
        
        for i, key in enumerate(user_keys[:10], 1):
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
            
            builder.button(text=f"#{key['key_id']} - {key['host_name']}", callback_data=f"view_key_{key['key_id']}")
        
        if len(user_keys) > 10:
            text += f"\n... и еще {len(user_keys) - 10} ключей"
        
        builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
        builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("view_key_"))
async def view_key_handler(callback: types.CallbackQuery):
    """Просмотр конкретного ключа"""
    try:
        key_id = int(callback.data.split("_")[2])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        host_data = db.get_host(key_data['host_name'])
        if not host_data:
            await callback.answer("Хост не найден", show_alert=True)
            return
        
        # Получаем актуальную информацию от X-UI
        result = await xui_api.create_client(
            host_data,
            key_data['key_email'],
            1  # Тест на 1 день для генерации строки
        )
        
        if result.get('error'):
            # Если ошибка, генерируем базовую строку
            host_url = host_data['host_url'].replace('https://', '').replace('http://', '').split(':')[0]
            connection_string = f"vless://{key_data['xui_client_uuid']}@{host_url}:443?type=tcp&security=tls&flow=xtls-rprx-vision#{quote(key_data['host_name'])}"
        else:
            connection_string = result['connection_string'].replace(key_data['xui_client_uuid'], key_data['xui_client_uuid'])
        
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
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error viewing key: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("qr_"))
async def qr_handler(callback: types.CallbackQuery):
    """Генерация QR-кода"""
    try:
        key_id = int(callback.data.split("_")[1])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        host_data = db.get_host(key_data['host_name'])
        if not host_data:
            await callback.answer("Хост не найден", show_alert=True)
            return
        
        # Генерируем connection string
        result = await xui_api.create_client(
            host_data,
            key_data['key_email'],
            1
        )
        
        if result.get('error'):
            host_url = host_data['host_url'].replace('https://', '').replace('http://', '').split(':')[0]
            connection_string = f"vless://{key_data['xui_client_uuid']}@{host_url}:443?type=tcp&security=tls&flow=xtls-rprx-vision#{quote(key_data['host_name'])}"
        else:
            connection_string = result['connection_string'].replace(key_data['xui_client_uuid'], key_data['xui_client_uuid'])
        
        qr_image = create_qr_code(connection_string)
        
        text = (
            f"📱 <b>QR-код для ключа #{key_id}</b>\n\n"
            f"🖥️ Сервер: {key_data['host_name']}\n"
            f"📅 Действует до: {key_data['expiry_date'][:10] if isinstance(key_data['expiry_date'], str) else key_data['expiry_date'].strftime('%d.%m.%Y')}\n\n"
            "Отсканируйте QR-код в приложении V2Ray/VLESS."
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data=f"view_key_{key_id}")
        
        await callback.message.answer_photo(
            photo=BufferedInputFile(qr_image.getvalue(), filename="qrcode.png"),
            caption=text,
            reply_markup=builder.as_markup()
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error generating QR: {e}")
        await callback.answer("Ошибка генерации QR-кода", show_alert=True)

@dp.callback_query(F.data.startswith("delete_key_"))
async def delete_key_handler(callback: types.CallbackQuery):
    """Удаление ключа"""
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
            f"⚠️ Это действие нельзя отменить.",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in delete_key: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("confirm_delete_key_"))
async def confirm_delete_key_handler(callback: types.CallbackQuery):
    """Подтверждение удаления ключа"""
    try:
        key_id = int(callback.data.split("_")[3])
        key_data = db.get_key_by_id(key_id)
        
        if not key_data or key_data['user_id'] != callback.from_user.id:
            await callback.answer("Ключ не найден", show_alert=True)
            return
        
        # Удаляем клиента из X-UI
        host_data = db.get_host(key_data['host_name'])
        if host_data:
            await xui_api.delete_client(host_data, key_data['xui_client_uuid'])
        
        # Удаляем из базы данных
        db.delete_key(key_id)
        
        await callback.answer("✅ Ключ удален", show_alert=True)
        await manage_keys_handler(callback)
        
    except Exception as e:
        logger.error(f"Error confirming delete: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data == "get_trial")
async def get_trial_handler(callback: types.CallbackQuery):
    """Получение пробного ключа"""
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
    
    host = hosts[0]
    email = f"user{user_id}-trial@{host['host_name'].replace(' ', '').lower()}.bot"
    
    await callback.message.edit_text("🔄 Создаю пробный ключ...")
    
    # Создаем ключ через X-UI
    result = await xui_api.create_client(
        host,
        email,
        TRIAL_DURATION_DAYS
    )
    
    if result.get('error'):
        await callback.message.edit_text(f"❌ Ошибка создания ключа: {result['error']}")
        return
    
    # Сохраняем ключ в базу данных
    key_id = db.add_key(
        user_id,
        host['host_name'],
        result['client_uuid'],
        email,
        result['expiry_date']
    )
    
    # Помечаем пробный период как использованный
    db.set_trial_used(user_id)
    
    # Начисляем реферальный бонус если есть реферер
    if user_data and user_data.get('referred_by'):
        referrer_id = user_data['referred_by']
        bonus_amount = 50  # Бонус за реферала
        db.add_referral_balance(referrer_id, bonus_amount)
    
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
    await callback.answer()

@dp.callback_query(F.data == "buy_new_key")
async def buy_new_key_handler(callback: types.CallbackQuery):
    """Покупка нового ключа"""
    hosts = db.get_all_hosts()
    
    if not hosts:
        await callback.message.edit_text("❌ Нет доступных серверов")
        return
    
    text = "🛒 <b>Покупка VPN ключа</b>\n\nВыберите сервер:"
    
    await callback.message.edit_text(text, reply_markup=create_hosts_menu(hosts))
    await callback.answer()

@dp.callback_query(F.data.startswith("select_host_"))
async def select_host_handler(callback: types.CallbackQuery):
    """Выбор хоста"""
    host_name = callback.data.split("_")[2]
    plans = db.get_plans_for_host(host_name)
    
    if not plans:
        await callback.message.edit_text(f"❌ Нет тарифов для {host_name}")
        return
    
    text = f"🛒 <b>Тарифы для {host_name}:</b>\n\nВыберите тарифный план:"
    
    await callback.message.edit_text(text, reply_markup=create_plans_menu(plans))
    await callback.answer()

@dp.callback_query(F.data.startswith("select_plan_"))
async def select_plan_handler(callback: types.CallbackQuery):
    """Выбор тарифного плана"""
    plan_id = int(callback.data.split("_")[2])
    plan = db.get_plan_by_id(plan_id)
    
    if not plan:
        await callback.answer("Ошибка: план не найден", show_alert=True)
        return
    
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    # Применяем реферальную скидку если есть
    price = float(plan['price'])
    discount = 0
    
    if ENABLE_REFERRALS and user_data and user_data.get('referred_by'):
        # Реферал получает скидку на первую покупку
        user_keys = db.get_user_keys(user_id)
        if len(user_keys) == 0:  # Первая покупка
            discount = price * (REFERRAL_DISCOUNT / 100)
            price -= discount
    
    builder = InlineKeyboardBuilder()
    
    # Добавляем метод оплаты CryptoBot
    if crypto_bot:
        builder.button(text="🤖 CryptoBot (USDT)", callback_data=f"pay_cryptobot_{plan_id}")
    
    builder.button(text="⬅️ Назад", callback_data=f"select_host_{plan['host_name']}")
    builder.adjust(1)
    
    if discount > 0:
        original_price = price + discount
        price_text = f"<s>{original_price:.2f}₽</s> <b>{price:.2f}₽</b> (-{REFERRAL_DISCOUNT}%)"
    else:
        price_text = f"<b>{price:.2f}₽</b>"
    
    await callback.message.edit_text(
        f"🛒 <b>Оформление заказа</b>\n\n"
        f"📋 <b>План:</b> {plan['plan_name']}\n"
        f"💰 <b>Цена:</b> {price_text}\n"
        f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
        f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
        "Выберите способ оплаты:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# === ОПЛАТА CRYPTOBOT ===
@dp.callback_query(F.data.startswith("pay_cryptobot_"))
async def pay_cryptobot_handler(callback: types.CallbackQuery):
    """Обработка оплаты через CryptoBot"""
    try:
        plan_id = int(callback.data.split("_")[2])
        plan = db.get_plan_by_id(plan_id)
        user_id = callback.from_user.id
        user_data = db.get_user(user_id)
        
        if not plan:
            await callback.answer("Ошибка: план не найден", show_alert=True)
            return
        
        if not crypto_bot:
            await callback.answer("CryptoBot недоступен", show_alert=True)
            return
        
        # Рассчитываем цену с учетом скидки
        price = float(plan['price'])
        discount = 0
        
        if ENABLE_REFERRALS and user_data and user_data.get('referred_by'):
            user_keys = db.get_user_keys(user_id)
            if len(user_keys) == 0:  # Первая покупка
                discount = price * (REFERRAL_DISCOUNT / 100)
                price -= discount
        
        # Получаем курс USDT к RUB
        rates_result = await crypto_bot.get_exchange_rates()
        if not rates_result.get('success'):
            await callback.message.edit_text(f"❌ Ошибка получения курса: {rates_result.get('error', 'Unknown error')}")
            return
        
        # Ищем курс USDT/RUB
        usdt_rate = None
        for rate in rates_result['rates']:
            if rate['source'] == 'USDT' and rate['target'] == 'RUB':
                usdt_rate = float(rate['rate'])
                break
        
        if not usdt_rate:
            await callback.message.edit_text("❌ Не удалось получить курс USDT/RUB")
            return
        
        # Рассчитываем сумму в USDT
        amount_usdt = price / usdt_rate
        
        # Создаем инвойс в CryptoBot
        result = await crypto_bot.create_invoice(
            amount=amount_usdt,
            asset="USDT",
            description=f"VPN Plan: {plan['plan_name']} for {plan['months']} months",
            hidden_message="Thank you for purchasing VPN service!"
        )
        
        if not result.get('success'):
            await callback.message.edit_text(f"❌ Ошибка создания счета: {result.get('error', 'Unknown error')}")
            return
        
        invoice = result['invoice']
        
        # Сохраняем платеж в базу данных
        metadata = {
            "plan_id": plan_id,
            "host_name": plan['host_name'],
            "plan_name": plan['plan_name'],
            "months": plan['months'],
            "price_rub": price,
            "price_usdt": amount_usdt,
            "discount": discount,
            "exchange_rate": usdt_rate
        }
        
        db.create_crypto_payment(
            invoice_id=invoice['invoice_id'],
            user_id=user_id,
            plan_id=plan_id,
            amount=amount_usdt,
            asset="USDT",
            metadata=metadata
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🤖 Оплатить в CryptoBot", url=invoice['pay_url'])
        builder.button(text="🔄 Проверить оплату", callback_data=f"check_crypto_payment_{invoice['invoice_id']}")
        builder.button(text="⬅️ Отмена", callback_data=f"select_plan_{plan_id}")
        builder.adjust(1)
        
        await callback.message.edit_text(
            f"🤖 <b>Счет CryptoBot создан!</b>\n\n"
            f"💰 <b>Сумма:</b> {amount_usdt:.2f} USDT (~{price:.0f}₽)\n"
            f"📅 <b>Срок:</b> {plan['months']} месяцев\n"
            f"🖥️ <b>Сервер:</b> {plan['host_name']}\n\n"
            "Нажмите кнопку для оплаты. После оплаты нажмите 'Проверить оплату'.\n\n"
            f"<i>Счет действителен 1 час</i>",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"CryptoBot payment error: {e}")
        await callback.message.edit_text(f"❌ Ошибка создания счета: {str(e)[:200]}")

@dp.callback_query(F.data.startswith("check_crypto_payment_"))
async def check_crypto_payment_handler(callback: types.CallbackQuery):
    """Проверка оплаты CryptoBot"""
    invoice_id = callback.data.split("_")[3]
    
    await callback.answer("⏳ Проверяю статус платежа...", show_alert=False)
    
    try:
        # Получаем информацию о платеже из базы данных
        payment_data = db.get_crypto_payment(invoice_id)
        
        if not payment_data:
            await callback.answer("Платеж не найден", show_alert=True)
            return
        
        # Проверяем статус в CryptoBot
        result = await crypto_bot.get_invoices(invoice_ids=[invoice_id])
        
        if not result.get('success'):
            await callback.answer(f"Ошибка проверки: {result.get('error')}", show_alert=True)
            return
        
        invoices = result.get('invoices', [])
        
        if not invoices:
            await callback.answer("Инвойс не найден", show_alert=True)
            return
        
        invoice = invoices[0]
        status = invoice['status']
        
        if status == 'paid':
            # Обновляем статус в базе данных
            db.update_crypto_payment_status(invoice_id, 'paid')
            
            # Создаем ключ VPN
            user_id = payment_data['user_id']
            plan_id = payment_data['plan_id']
            plan = db.get_plan_by_id(plan_id)
            
            if not plan:
                await callback.answer("Ошибка: план не найден", show_alert=True)
                return
            
            host_data = db.get_host(plan['host_name'])
            if not host_data:
                await callback.answer("Ошибка: сервер не найден", show_alert=True)
                return
            
            # Генерируем email для ключа
            user_data = db.get_user(user_id)
            email = f"user{user_id}-{plan_id}@{host_data['host_name'].replace(' ', '').lower()}.vpn"
            
            # Создаем ключ в X-UI
            result = await xui_api.create_client(
                host_data,
                email,
                plan['months'] * 30  # Переводим месяцы в дни
            )
            
            if result.get('error'):
                await callback.answer(f"Ошибка создания ключа: {result['error']}", show_alert=True)
                return
            
            # Сохраняем ключ в базу данных
            key_id = db.add_key(
                user_id,
                host_data['host_name'],
                result['client_uuid'],
                email,
                result['expiry_date']
            )
            
            # Обновляем статистику пользователя
            metadata = json.loads(payment_data['metadata'])
            price_rub = metadata.get('price_rub', 0)
            
            db.update_user_stats(user_id, price_rub, plan['months'])
            
            # Логируем транзакцию
            db.log_transaction(
                username=user_data.get('username') or user_data.get('full_name'),
                user_id=user_id,
                status='paid',
                amount_rub=price_rub,
                payment_method='CryptoBot',
                metadata={
                    'plan_id': plan_id,
                    'key_id': key_id,
                    'invoice_id': invoice_id
                }
            )
            
            # Начисляем реферальный бонус
            if ENABLE_REFERRALS and user_data and user_data.get('referred_by'):
                referrer_id = user_data['referred_by']
                bonus_amount = price_rub * (REFERRAL_PERCENTAGE / 100)
                db.add_referral_balance(referrer_id, bonus_amount)
            
            # Обновляем payment с key_id
            db.create_crypto_payment(
                invoice_id=invoice_id,
                user_id=user_id,
                plan_id=plan_id,
                amount=payment_data['amount'],
                asset=payment_data['asset'],
                metadata=metadata,
                key_id=key_id
            )
            
            # Отправляем ключ пользователю
            expiry_date = result['expiry_date']
            expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
            
            success_text = (
                f"🎉 <b>Оплата подтверждена!</b>\n\n"
                f"✅ <b>Ваш VPN ключ создан:</b>\n"
                f"🖥️ <b>Сервер:</b> {host_data['host_name']}\n"
                f"📅 <b>Действует до:</b> {expiry_formatted}\n"
                f"📅 <b>Срок:</b> {plan['months']} месяцев\n\n"
                f"<code>{result['connection_string']}</code>"
            )
            
            builder = InlineKeyboardBuilder()
            builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
            builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
            builder.button(text="⬅️ В меню", callback_data="back_to_main_menu")
            builder.adjust(2, 1)
            
            await callback.message.edit_text(success_text, reply_markup=builder.as_markup())
            
        elif status == 'active':
            await callback.answer("⏳ Ожидание оплаты...", show_alert=True)
        else:
            await callback.answer(f"Статус платежа: {status}", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error checking payment: {e}")
        await callback.answer("Ошибка проверки платежа", show_alert=True)

# === РЕФЕРАЛЬНАЯ СИСТЕМА ===
@dp.callback_query(F.data == "show_referrals")
async def show_referrals_handler(callback: types.CallbackQuery):
    """Показать реферальную программу"""
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    referrals = db.get_referrals(user_id)
    bot_username = TELEGRAM_BOT_USERNAME or (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
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
    await callback.answer()

@dp.callback_query(F.data == "withdraw_referral")
async def withdraw_referral_handler(callback: types.CallbackQuery, state: FSMContext):
    """Вывод реферальных средств"""
    user_id = callback.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    balance = user_data.get('referral_balance', 0)
    
    if balance < MINIMUM_WITHDRAWAL:
        await callback.answer(
            f"Минимальная сумма для вывода: {MINIMUM_WITHDRAWAL}₽\n"
            f"Ваш баланс: {balance:.2f}₽",
            show_alert=True
        )
        return
    
    await state.set_state(Form.waiting_for_withdrawal_details)
    await state.update_data(user_id=user_id, balance=balance)
    
    await callback.message.edit_text(
        f"💰 <b>Вывод реферальных средств</b>\n\n"
        f"💎 <b>Доступно для вывода:</b> {balance:.2f}₽\n"
        f"💳 <b>Минимальная сумма:</b> {MINIMUM_WITHDRAWAL}₽\n\n"
        f"📝 <b>Укажите:</b>\n"
        f"1. Сумму вывода (не более {balance:.2f}₽)\n"
        f"2. Реквизиты для перевода\n\n"
        f"<i>Пример:</i>\n"
        f"<code>500\nКарта 2200 1234 5678 9010</code>\n\n"
        f"Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="show_referrals")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_withdrawal_details)
async def process_withdrawal_details(message: Message, state: FSMContext):
    """Обработка данных для вывода"""
    try:
        data = await state.get_data()
        user_id = data['user_id']
        balance = data['balance']
        
        # Парсим сообщение
        lines = message.text.strip().split('\n')
        if len(lines) < 2:
            await message.answer(
                "❌ Неверный формат. Нужно указать сумму и реквизиты.\n\n"
                "<i>Пример:</i>\n"
                "<code>500\nКарта 2200 1234 5678 9010</code>",
                parse_mode="HTML"
            )
            return
        
        amount_str = lines[0].strip()
        details = '\n'.join(lines[1:]).strip()
        
        # Проверяем сумму
        try:
            amount = float(amount_str)
        except:
            await message.answer("❌ Неверный формат суммы. Укажите число.")
            return
        
        if amount < MINIMUM_WITHDRAWAL:
            await message.answer(f"❌ Минимальная сумма для вывода: {MINIMUM_WITHDRAWAL}₽")
            return
        
        if amount > balance:
            await message.answer(f"❌ Недостаточно средств. Ваш баланс: {balance:.2f}₽")
            return
        
        # Создаем заявку на вывод
        withdrawal_id = db.withdraw_referral_balance(user_id, amount, details)
        
        user_data = db.get_user(user_id)
        username = user_data.get('username') or user_data.get('full_name')
        
        # Отправляем уведомление админу
        admin_text = (
            f"📝 <b>Новая заявка на вывод #{withdrawal_id}</b>\n\n"
            f"👤 <b>Пользователь:</b> {username} (ID: {user_id})\n"
            f"💰 <b>Сумма:</b> {amount:.2f}₽\n"
            f"📋 <b>Реквизиты:</b>\n<code>{details}</code>\n\n"
            f"⏰ <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Одобрить", callback_data=f"admin_approve_withdrawal_{withdrawal_id}")
        builder.button(text="❌ Отклонить", callback_data=f"admin_reject_withdrawal_{withdrawal_id}")
        builder.adjust(2)
        
        try:
            await bot.send_message(ADMIN_ID, admin_text, reply_markup=builder.as_markup())
        except:
            pass
        
        await message.answer(
            f"✅ <b>Заявка на вывод создана!</b>\n\n"
            f"💰 <b>Сумма:</b> {amount:.2f}₽\n"
            f"📋 <b>Реквизиты:</b>\n<code>{details}</code>\n\n"
            f"📝 <b>Номер заявки:</b> #{withdrawal_id}\n\n"
            f"Заявка отправлена на рассмотрение администратору.\n"
            f"Обычно обработка занимает до 24 часов.",
            reply_markup=create_back_button("show_referrals")
        )
        
        await state.clear()
        
    except Exception as e:
        logger.error(f"Error processing withdrawal: {e}")
        await message.answer("❌ Ошибка при создании заявки. Попробуйте позже.")
    finally:
        await state.clear()

# === ПОДДЕРЖКА И О ПРОЕКТЕ ===
@dp.callback_query(F.data == "show_help")
async def show_help_handler(callback: types.CallbackQuery, state: FSMContext):
    """Показать помощь"""
    support_user = SUPPORT_USER if SUPPORT_USER else "администратору"
    support_text = SUPPORT_TEXT if SUPPORT_TEXT else "Напишите нам в поддержку"
    
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
        f"• Linux: {LINUX_URL}\n\n"
        f"Нажмите кнопку ниже чтобы написать в поддержку:"
    )
    
    builder = InlineKeyboardBuilder()
    
    if SUPPORT_USER.startswith('@'):
        builder.button(text="💬 Написать в поддержку", url=f"https://t.me/{SUPPORT_USER.replace('@', '')}")
    elif SUPPORT_USER.isdigit():
        builder.button(text="💬 Написать в поддержку", url=f"tg://user?id={SUPPORT_USER}")
    
    builder.button(text="📝 Отправить сообщение", callback_data="send_support_message")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "send_support_message")
async def send_support_message_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отправка сообщения в поддержку"""
    await state.set_state(Form.waiting_for_support_message)
    
    await callback.message.edit_text(
        "📝 <b>Напишите ваше сообщение в поддержку</b>\n\n"
        "Опишите вашу проблему как можно подробнее:\n"
        "• Что именно не работает?\n"
        "• Какие действия вы предпринимали?\n"
        "• Какие ошибки вы видите?\n\n"
        "Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="show_help")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_support_message)
async def process_support_message(message: Message, state: FSMContext):
    """Обработка сообщения в поддержку"""
    try:
        user_id = message.from_user.id
        user_data = db.get_user(user_id)
        username = user_data.get('username') or user_data.get('full_name') or f"ID: {user_id}"
        
        # Сохраняем сообщение в базе данных
        message_id = db.create_support_message(user_id, message.text)
        
        # Отправляем уведомление админу
        admin_text = (
            f"📩 <b>Новое сообщение в поддержку #{message_id}</b>\n\n"
            f"👤 <b>От:</b> {username} (ID: {user_id})\n"
            f"📅 <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"📝 <b>Сообщение:</b>\n{message.text}\n\n"
            f"Для ответа используйте команду /answer_{message_id}"
        )
        
        try:
            await bot.send_message(ADMIN_ID, admin_text)
        except:
            pass
        
        await message.answer(
            f"✅ <b>Сообщение отправлено!</b>\n\n"
            f"📝 <b>Номер сообщения:</b> #{message_id}\n"
            f"⏰ <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
            f"Администратор ответит вам в ближайшее время.",
            reply_markup=create_back_button("show_help")
        )
        
    except Exception as e:
        logger.error(f"Error processing support message: {e}")
        await message.answer("❌ Ошибка при отправке сообщения. Попробуйте позже.")
    finally:
        await state.clear()

@dp.callback_query(F.data == "show_about")
async def show_about_handler(callback: types.CallbackQuery):
    """Показать информацию о проекте"""
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
    
    if builder.buttons:
        builder.adjust(2 if len(builder.buttons) > 2 else 1, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), disable_web_page_preview=True)
    await callback.answer()

# === АДМИН ПАНЕЛЬ ===
@dp.callback_query(F.data == "admin_panel")
async def admin_panel_handler(callback: types.CallbackQuery):
    """Админ панель"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 <b>Административная панель</b>\n\n"
        "Выберите раздел:",
        reply_markup=create_admin_main_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: types.CallbackQuery):
    """Статистика админ панели"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        stats = db.get_stats_summary()
        
        today_revenue = stats.get('today_revenue', 0) or 0
        total_revenue = stats.get('total_revenue', 0) or 0
        active_keys = stats.get('active_keys', 0) or 0
        
        text = (
            f"📊 <b>Статистика системы</b>\n\n"
            f"👥 <b>Пользователи:</b>\n"
            f"• Всего: {stats.get('total_users', 0)}\n"
            f"• Активных: {stats.get('active_users', 0)}\n"
            f"• Заблокированных: {stats.get('banned_users', 0)}\n"
            f"• Новых сегодня: {stats.get('today_users', 0)}\n\n"
            f"🔑 <b>Ключи:</b>\n"
            f"• Всего: {stats.get('total_keys', 0)}\n"
            f"• Активных: {active_keys}\n\n"
            f"💰 <b>Финансы:</b>\n"
            f"• Выручка всего: {total_revenue:.2f}₽\n"
            f"• Выручка сегодня: {today_revenue:.2f}₽\n\n"
            f"🔄 <b>Ожидают обработки:</b>\n"
            f"• Платежей: {stats.get('pending_payments', 0)}\n"
            f"• Заявок на вывод: {stats.get('pending_withdrawals', 0)}\n\n"
            f"🖥️ <b>Инфраструктура:</b>\n"
            f"• Серверов: {db.get_hosts_count()}\n"
            f"• Тарифов: {db.get_plans_count()}"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Обновить", callback_data="admin_stats")
        builder.button(text="📈 Подробная статистика", callback_data="admin_detailed_stats")
        builder.button(text="⬅️ Назад", callback_data="admin_panel")
        builder.adjust(1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        await callback.answer("Ошибка получения статистики", show_alert=True)

@dp.callback_query(F.data == "admin_detailed_stats")
async def admin_detailed_stats_handler(callback: types.CallbackQuery):
    """Подробная статистика"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        week_revenue = db.get_week_revenue()
        month_revenue = db.get_month_revenue()
        expiring_keys = db.get_expiring_keys(3)
        activity_stats = db.get_user_activity_stats(7)
        
        text = (
            f"📈 <b>Подробная статистика</b>\n\n"
            f"💰 <b>Финансы:</b>\n"
            f"• За неделю: {week_revenue:.2f}₽\n"
            f"• За месяц: {month_revenue:.2f}₽\n\n"
            f"📊 <b>Активность (7 дней):</b>\n"
            f"• Активных пользователей: {activity_stats.get('active_users', 0)}\n"
            f"• Создано ключей: {activity_stats.get('total_keys_created', 0)}\n\n"
            f"⚠️ <b>Истекающие ключи (3 дня):</b> {len(expiring_keys)}\n"
        )
        
        if expiring_keys:
            text += "\nСписок истекающих ключей:\n"
            for key in expiring_keys[:5]:
                username = key.get('username') or key.get('full_name') or f"ID: {key['user_id']}"
                expiry_date = datetime.fromisoformat(key['expiry_date']) if isinstance(key['expiry_date'], str) else key['expiry_date']
                days_left = calculate_days_left(expiry_date)
                text += f"• {username} - {key['host_name']} (осталось {days_left} д.)\n"
            
            if len(expiring_keys) > 5:
                text += f"... и еще {len(expiring_keys) - 5} ключей\n"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Обновить", callback_data="admin_detailed_stats")
        builder.button(text="⬅️ Назад", callback_data="admin_stats")
        builder.adjust(1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error getting detailed stats: {e}")
        await callback.answer("Ошибка получения статистики", show_alert=True)

@dp.callback_query(F.data == "admin_users")
async def admin_users_handler(callback: types.CallbackQuery):
    """Управление пользователями"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    users = db.get_all_users(20)
    
    text = f"👥 <b>Пользователи</b> (последние {len(users)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for user in users:
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
        
        builder.button(text=f"👤 {user_id}", callback_data=f"admin_view_user_{user_id}")
    
    builder.button(text="🔍 Поиск пользователя", callback_data="admin_search_user")
    builder.button(text="📋 Все пользователи", callback_data="admin_all_users")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(3, 3, 3, 3, 2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_search_user")
async def admin_search_user_handler(callback: types.CallbackQuery, state: FSMContext):
    """Поиск пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_user_search)
    await callback.message.edit_text(
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Введите ID пользователя, username или имя:\n\n"
        "Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_users")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_user_search)
async def process_user_search(message: Message, state: FSMContext):
    """Обработка поиска пользователя"""
    if message.from_user.id != ADMIN_ID:
        return
    
    query = message.text.strip()
    
    if not query:
        await message.answer("❌ Введите поисковый запрос.")
        return
    
    users = db.search_users(query)
    
    if not users:
        await message.answer(f"❌ Пользователи по запросу '{query}' не найдены.")
        return
    
    text = f"🔍 <b>Результаты поиска</b> (найдено: {len(users)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for user in users[:15]:
        user_id = user['telegram_id']
        username = user['username'] or user['full_name'] or f"ID: {user_id}"
        status = "🚫" if user.get('is_banned') else "✅"
        
        text += f"{status} <b>{username}</b>\n"
        text += f"   🆔 {user_id} | 💰 {user['total_spent']:.0f}₽\n\n"
        
        builder.button(text=f"👤 {user_id}", callback_data=f"admin_view_user_{user_id}")
    
    if len(users) > 15:
        text += f"\n... и еще {len(users) - 15} пользователей"
    
    builder.button(text="⬅️ Назад", callback_data="admin_users")
    builder.adjust(3, 3, 3, 3, 1)
    
    await message.answer(text, reply_markup=builder.as_markup())
    await state.clear()

@dp.callback_query(F.data.startswith("admin_view_user_"))
async def admin_view_user_handler(callback: types.CallbackQuery):
    """Просмотр пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        user_id = int(callback.data.split("_")[3])
        user_data = db.get_user(user_id)
        
        if not user_data:
            await callback.answer("❌ Пользователь не найден", show_alert=True)
            return
        
        user_keys = db.get_user_keys(user_id)
        referrals = db.get_referrals(user_id)
        
        created_at = user_data['created_at']
        if isinstance(created_at, str):
            created_date = datetime.fromisoformat(created_at[:19]) if 'T' in created_at else datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        else:
            created_date = created_at
        
        text = (
            f"👤 <b>Пользователь #{user_id}</b>\n\n"
            f"📝 <b>Информация:</b>\n"
            f"• Имя: {user_data['full_name'] or 'Не указано'}\n"
            f"• Username: @{user_data['username'] or 'нет'}\n"
            f"• Зарегистрирован: {created_date.strftime('%d.%m.%Y %H:%M')}\n"
            f"• Пробный период: {'✅ Использован' if user_data.get('trial_used') else '🆓 Доступен'}\n"
            f"• Статус: {'🚫 Заблокирован' if user_data.get('is_banned') else '✅ Активен'}\n\n"
            f"💰 <b>Финансы:</b>\n"
            f"• Потрачено: {user_data['total_spent']:.2f}₽\n"
            f"• Месяцев куплено: {user_data['total_months']}\n"
            f"• Реферальный баланс: {user_data.get('referral_balance', 0):.2f}₽\n"
            f"• Реферер: {'Не указан' if not user_data.get('referred_by') else f'ID: {user_data['referred_by']}'}\n"
            f"• Рефералов: {len(referrals)}\n\n"
            f"🔑 <b>Ключи ({len(user_keys)}):</b>\n"
        )
        
        now = datetime.now()
        active_keys = 0
        for key in user_keys[:5]:
            expiry_date = datetime.fromisoformat(key['expiry_date']) if isinstance(key['expiry_date'], str) else key['expiry_date']
            is_active = expiry_date > now
            status = "✅" if is_active else "❌"
            if is_active:
                active_keys += 1
            text += f"{status} {key['host_name']} до {expiry_date.strftime('%d.%m.%Y')}\n"
        
        if len(user_keys) > 5:
            text += f"... и еще {len(user_keys) - 5} ключей\n"
        
        text += f"\n📊 <b>Активных ключей:</b> {active_keys}"
        
        builder = InlineKeyboardBuilder()
        
        if user_data.get('is_banned'):
            builder.button(text="✅ Разблокировать", callback_data=f"admin_unban_{user_id}")
        else:
            builder.button(text="🚫 Заблокировать", callback_data=f"admin_ban_{user_id}")
        
        builder.button(text="🗑️ Удалить ключи", callback_data=f"admin_delete_user_keys_{user_id}")
        builder.button(text="💸 Вывод средств", callback_data=f"admin_withdraw_user_{user_id}")
        builder.button(text="⬅️ Назад", callback_data="admin_users")
        builder.adjust(1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error viewing user: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_ban_"))
async def admin_ban_handler(callback: types.CallbackQuery):
    """Блокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        user_id = int(callback.data.split("_")[2])
        db.ban_user(user_id)
        db.log_admin_action(callback.from_user.id, "ban_user", f"Заблокировал пользователя {user_id}")
        
        await callback.answer(f"✅ Пользователь {user_id} заблокирован", show_alert=True)
        await admin_view_user_handler(callback)
        
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_unban_"))
async def admin_unban_handler(callback: types.CallbackQuery):
    """Разблокировка пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        user_id = int(callback.data.split("_")[2])
        db.unban_user(user_id)
        db.log_admin_action(callback.from_user.id, "unban_user", f"Разблокировал пользователя {user_id}")
        
        await callback.answer(f"✅ Пользователь {user_id} разблокирован", show_alert=True)
        await admin_view_user_handler(callback)
        
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_delete_user_keys_"))
async def admin_delete_user_keys_handler(callback: types.CallbackQuery):
    """Удаление ключей пользователя"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        user_id = int(callback.data.split("_")[4])
        user_data = db.get_user(user_id)
        
        if not user_data:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Да, удалить все", callback_data=f"admin_confirm_delete_keys_{user_id}")
        builder.button(text="❌ Нет, отмена", callback_data=f"admin_view_user_{user_id}")
        
        await callback.message.edit_text(
            f"🗑️ <b>Удаление всех ключей пользователя</b>\n\n"
            f"Вы уверены, что хотите удалить ВСЕ ключи пользователя {user_data['full_name'] or user_data['username']}?\n"
            f"⚠️ Это действие нельзя отменить!\n"
            f"⚠️ Все VPN подключения пользователя перестанут работать!",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in delete user keys: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_confirm_delete_keys_"))
async def admin_confirm_delete_keys_handler(callback: types.CallbackQuery):
    """Подтверждение удаления ключей"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        user_id = int(callback.data.split("_")[4])
        user_data = db.get_user(user_id)
        
        if not user_data:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        
        user_keys = db.get_user_keys(user_id)
        deleted_count = 0
        
        for key in user_keys:
            # Удаляем из X-UI
            host_data = db.get_host(key['host_name'])
            if host_data:
                await xui_api.delete_client(host_data, key['xui_client_uuid'])
            
            # Удаляем из базы
            db.delete_key(key['key_id'])
            deleted_count += 1
        
        db.log_admin_action(callback.from_user.id, "delete_user_keys", 
                          f"Удалил {deleted_count} ключей пользователя {user_id}")
        
        await callback.answer(f"✅ Удалено {deleted_count} ключей", show_alert=True)
        await admin_view_user_handler(callback)
        
    except Exception as e:
        logger.error(f"Error confirming delete keys: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data == "admin_hosts")
async def admin_hosts_handler(callback: types.CallbackQuery):
    """Управление хостами"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
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
    builder.button(text="🔄 Тестировать подключения", callback_data="admin_test_hosts")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_view_host_"))
async def admin_view_host_handler(callback: types.CallbackQuery):
    """Просмотр хоста"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
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
        for plan in plans[:5]:
            text += f"• {plan['plan_name']} - {plan['months']}м - {plan['price']}₽\n"
        if len(plans) > 5:
            text += f"  ... и еще {len(plans) - 5}\n"
    else:
        text += "Нет тарифов\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Добавить тариф", callback_data=f"admin_add_plan_{host_name}")
    builder.button(text="✏️ Редактировать", callback_data=f"admin_edit_host_{host_name}")
    builder.button(text="🔄 Тест подключения", callback_data=f"admin_test_host_{host_name}")
    builder.button(text="🗑️ Удалить", callback_data=f"admin_delete_host_{host_name}")
    builder.button(text="⬅️ Назад", callback_data="admin_hosts")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_add_host")
async def admin_add_host_handler(callback: types.CallbackQuery, state: FSMContext):
    """Добавление хоста"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_add_host)
    await callback.message.edit_text(
        "➕ <b>Добавление нового хоста</b>\n\n"
        "Введите данные в следующем формате:\n\n"
        "<code>Название сервера\nURL панели\nЛогин\nПароль\nID инбаунда</code>\n\n"
        "<i>Пример:</i>\n"
        "<code>Сервер 1\nhttps://xui.example.com:54321\nadmin\npassword123\n1</code>\n\n"
        "Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_hosts")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_add_host)
async def process_add_host(message: Message, state: FSMContext):
    """Обработка добавления хоста"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        lines = message.text.strip().split('\n')
        if len(lines) < 5:
            await message.answer(
                "❌ Неверный формат. Нужно 5 строк:\n"
                "1. Название сервера\n"
                "2. URL панели\n"
                "3. Логин\n"
                "4. Пароль\n"
                "5. ID инбаунда"
            )
            return
        
        host_name = lines[0].strip()
        host_url = lines[1].strip()
        host_username = lines[2].strip()
        host_pass = lines[3].strip()
        
        try:
            host_inbound_id = int(lines[4].strip())
        except:
            await message.answer("❌ ID инбаунда должен быть числом")
            return
        
        # Проверяем подключение
        await message.answer("🔄 Проверяю подключение к X-UI панели...")
        
        success, error = await xui_api.test_connection(host_url, host_username, host_pass)
        
        if not success:
            await message.answer(
                f"❌ Не удалось подключиться к X-UI панели:\n{error}\n\n"
                f"Хост не добавлен."
            )
            return
        
        # Добавляем хост
        db.add_host(host_name, host_url, host_username, host_pass, host_inbound_id)
        db.log_admin_action(message.from_user.id, "add_host", 
                          f"Добавил хост {host_name}")
        
        await message.answer(
            f"✅ <b>Хост добавлен!</b>\n\n"
            f"🖥️ <b>Название:</b> {host_name}\n"
            f"🔗 <b>URL:</b> {host_url}\n"
            f"👤 <b>Логин:</b> {host_username}\n"
            f"🆔 <b>Inbound ID:</b> {host_inbound_id}\n\n"
            f"Теперь вы можете добавить тарифы для этого хоста.",
            reply_markup=create_back_button("admin_hosts")
        )
        
    except Exception as e:
        logger.error(f"Error adding host: {e}")
        await message.answer(f"❌ Ошибка при добавлении хоста: {str(e)[:200]}")
    finally:
        await state.clear()

@dp.callback_query(F.data.startswith("admin_test_host_"))
async def admin_test_host_handler(callback: types.CallbackQuery):
    """Тестирование подключения к хосту"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[3]
    host = db.get_host(host_name)
    
    if not host:
        await callback.answer("Хост не найден", show_alert=True)
        return
    
    await callback.answer("🔄 Тестирую подключение...", show_alert=False)
    
    success, error = await xui_api.test_connection(
        host['host_url'],
        host['host_username'],
        host['host_pass']
    )
    
    if success:
        await callback.answer(f"✅ Подключение успешно", show_alert=True)
    else:
        await callback.answer(f"❌ Ошибка подключения: {error[:100]}", show_alert=True)

@dp.callback_query(F.data.startswith("admin_delete_host_"))
async def admin_delete_host_handler(callback: types.CallbackQuery):
    """Удаление хоста"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
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
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_confirm_delete_host_"))
async def admin_confirm_delete_host_handler(callback: types.CallbackQuery):
    """Подтверждение удаления хоста"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[4]
    db.delete_host(host_name)
    db.log_admin_action(callback.from_user.id, "delete_host", 
                      f"Удалил хост {host_name}")
    
    await callback.answer(f"✅ Хост {host_name} удален", show_alert=True)
    await admin_hosts_handler(callback)

@dp.callback_query(F.data == "admin_plans")
async def admin_plans_handler(callback: types.CallbackQuery):
    """Управление тарифами"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    plans = db.get_all_plans()
    
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
        for plan in host_plans[:5]:
            price_text = f"{int(plan['price'])}₽" if plan['price'].is_integer() else f"{plan['price']:.2f}₽"
            text += f"• {plan['plan_name']} - {plan['months']}м - {price_text}\n"
        
        if len(host_plans) > 5:
            text += f"  ... и еще {len(host_plans) - 5}\n"
        
        text += "\n"
        
        builder.button(text=host_name, callback_data=f"admin_view_host_{host_name}")
    
    builder.button(text="➕ Добавить тариф", callback_data="admin_add_plan_select")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_add_plan_select")
async def admin_add_plan_select_handler(callback: types.CallbackQuery):
    """Выбор хоста для добавления тарифа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    hosts = db.get_all_hosts()
    
    if not hosts:
        await callback.answer("❌ Нет хостов. Сначала добавьте хост.", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    
    for host in hosts:
        builder.button(text=host['host_name'], callback_data=f"admin_add_plan_{host['host_name']}")
    
    builder.button(text="⬅️ Назад", callback_data="admin_plans")
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📦 <b>Добавление тарифа</b>\n\n"
        "Выберите хост для которого добавляется тариф:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_add_plan_"))
async def admin_add_plan_handler(callback: types.CallbackQuery, state: FSMContext):
    """Добавление тарифа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    host_name = callback.data.split("_")[3]
    host = db.get_host(host_name)
    
    if not host:
        await callback.answer("Хост не найден", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_add_plan)
    await state.update_data(host_name=host_name)
    
    await callback.message.edit_text(
        f"📦 <b>Добавление тарифа для {host_name}</b>\n\n"
        "Введите данные в следующем формате:\n\n"
        "<code>Название тарифа\nКоличество месяцев\nЦена в рублях</code>\n\n"
        "<i>Пример:</i>\n"
        "<code>3 месяца\n3\n800</code>\n\n"
        "Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_plans")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_add_plan)
async def process_add_plan(message: Message, state: FSMContext):
    """Обработка добавления тарифа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        data = await state.get_data()
        host_name = data['host_name']
        
        lines = message.text.strip().split('\n')
        if len(lines) < 3:
            await message.answer(
                "❌ Неверный формат. Нужно 3 строки:\n"
                "1. Название тарифа\n"
                "2. Количество месяцев\n"
                "3. Цена в рублях"
            )
            return
        
        plan_name = lines[0].strip()
        
        try:
            months = int(lines[1].strip())
            if months < 1:
                raise ValueError
        except:
            await message.answer("❌ Количество месяцев должно быть положительным числом")
            return
        
        try:
            price = float(lines[2].strip())
            if price <= 0:
                raise ValueError
        except:
            await message.answer("❌ Цена должна быть положительным числом")
            return
        
        # Добавляем тариф
        db.add_plan(host_name, plan_name, months, price)
        db.log_admin_action(message.from_user.id, "add_plan", 
                          f"Добавил тариф {plan_name} для {host_name}")
        
        await message.answer(
            f"✅ <b>Тариф добавлен!</b>\n\n"
            f"📦 <b>Название:</b> {plan_name}\n"
            f"📅 <b>Срок:</b> {months} месяцев\n"
            f"💰 <b>Цена:</b> {price:.2f}₽\n"
            f"🖥️ <b>Сервер:</b> {host_name}",
            reply_markup=create_back_button("admin_plans")
        )
        
    except Exception as e:
        logger.error(f"Error adding plan: {e}")
        await message.answer(f"❌ Ошибка при добавлении тарифа: {str(e)[:200]}")
    finally:
        await state.clear()

@dp.callback_query(F.data == "admin_transactions")
async def admin_transactions_handler(callback: types.CallbackQuery):
    """Транзакции"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    transactions = db.get_all_transactions(50)
    
    text = f"💳 <b>Транзакции</b> (последние {len(transactions)})\n\n"
    
    total_today = 0
    for tx in transactions:
        created_date = tx['created_date']
        if isinstance(created_date, str):
            try:
                date_str = created_date[11:16]
            except:
                date_str = created_date[:16]
        else:
            date_str = created_date.strftime('%H:%M')
        
        status_icon = "✅" if tx['status'] == 'paid' else "⏳" if tx['status'] == 'pending' else "❌"
        
        text += f"{status_icon} <b>{tx['username'] or tx['full_name'] or 'Без имени'}</b>\n"
        text += f"   🕒 {date_str} | 💰 {tx['amount_rub']:.2f}₽\n"
        text += f"   💳 {tx['payment_method']}\n\n"
        
        if tx['status'] == 'paid':
            tx_date = created_date[:10] if isinstance(created_date, str) else created_date.date()
            today = datetime.now().date()
            if str(tx_date) == str(today):
                total_today += tx['amount_rub']
    
    text += f"\n💰 <b>Сумма сегодня:</b> {total_today:.2f}₽"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="admin_transactions")
    builder.button(text="📈 Статистика", callback_data="admin_revenue_stats")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals_handler(callback: types.CallbackQuery):
    """Заявки на вывод"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    withdrawals = db.get_referral_withdrawals()
    
    pending_withdrawals = [w for w in withdrawals if w['status'] == 'pending']
    completed_withdrawals = [w for w in withdrawals if w['status'] == 'completed']
    
    text = (
        f"💸 <b>Заявки на вывод</b>\n\n"
        f"⏳ <b>Ожидают:</b> {len(pending_withdrawals)}\n"
        f"✅ <b>Завершены:</b> {len(completed_withdrawals)}\n\n"
    )
    
    if pending_withdrawals:
        text += f"<b>Последние {min(5, len(pending_withdrawals))} ожидающих:</b>\n"
        for w in pending_withdrawals[:5]:
            created_at = w['created_at']
            if isinstance(created_at, str):
                date_str = created_at[:16]
            else:
                date_str = created_at.strftime('%d.%m.%Y %H:%M')
            
            text += f"• #{w['withdrawal_id']} - {w['username'] or w['full_name']} - {w['amount']:.2f}₽\n"
            text += f"  📅 {date_str}\n\n"
    
    builder = InlineKeyboardBuilder()
    
    if pending_withdrawals:
        builder.button(text="📋 Все ожидающие", callback_data="admin_pending_withdrawals")
    
    builder.button(text="✅ Завершенные", callback_data="admin_completed_withdrawals")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_pending_withdrawals")
async def admin_pending_withdrawals_handler(callback: types.CallbackQuery):
    """Ожидающие заявки на вывод"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    withdrawals = db.get_referral_withdrawals('pending')
    
    if not withdrawals:
        await callback.answer("❌ Нет ожидающих заявок", show_alert=True)
        return
    
    text = f"⏳ <b>Ожидающие заявки на вывод</b> ({len(withdrawals)})\n\n"
    
    builder = InlineKeyboardBuilder()
    
    for w in withdrawals[:20]:
        created_at = w['created_at']
        if isinstance(created_at, str):
            date_str = created_at[:16]
        else:
            date_str = created_at.strftime('%d.%m.%Y %H:%M')
        
        text += f"#{w['withdrawal_id']} - {w['username'] or w['full_name']}\n"
        text += f"💰 {w['amount']:.2f}₽ | 📅 {date_str}\n\n"
        
        builder.button(text=f"#{w['withdrawal_id']}", callback_data=f"admin_view_withdrawal_{w['withdrawal_id']}")
    
    builder.button(text="⬅️ Назад", callback_data="admin_withdrawals")
    builder.adjust(3, 3, 3, 3, 1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_view_withdrawal_"))
async def admin_view_withdrawal_handler(callback: types.CallbackQuery):
    """Просмотр заявки на вывод"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        withdrawal_id = int(callback.data.split("_")[3])
        withdrawals = db.get_referral_withdrawals()
        
        withdrawal = None
        for w in withdrawals:
            if w['withdrawal_id'] == withdrawal_id:
                withdrawal = w
                break
        
        if not withdrawal:
            await callback.answer("Заявка не найдена", show_alert=True)
            return
        
        created_at = withdrawal['created_at']
        if isinstance(created_at, str):
            date_str = created_at[:16]
        else:
            date_str = created_at.strftime('%d.%m.%Y %H:%M')
        
        text = (
            f"💸 <b>Заявка на вывод #{withdrawal_id}</b>\n\n"
            f"👤 <b>Пользователь:</b> {withdrawal['username'] or withdrawal['full_name']} (ID: {withdrawal['user_id']})\n"
            f"💰 <b>Сумма:</b> {withdrawal['amount']:.2f}₽\n"
            f"📅 <b>Дата:</b> {date_str}\n"
            f"📊 <b>Статус:</b> {withdrawal['status']}\n\n"
            f"📋 <b>Реквизиты:</b>\n<code>{withdrawal['details']}</code>\n"
        )
        
        if withdrawal.get('admin_notes'):
            text += f"\n📝 <b>Заметки админа:</b>\n{withdrawal['admin_notes']}\n"
        
        builder = InlineKeyboardBuilder()
        
        if withdrawal['status'] == 'pending':
            builder.button(text="✅ Одобрить", callback_data=f"admin_approve_withdrawal_{withdrawal_id}")
            builder.button(text="❌ Отклонить", callback_data=f"admin_reject_withdrawal_{withdrawal_id}")
        
        builder.button(text="⬅️ Назад", callback_data="admin_pending_withdrawals")
        builder.adjust(2, 1)
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error viewing withdrawal: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_approve_withdrawal_"))
async def admin_approve_withdrawal_handler(callback: types.CallbackQuery):
    """Одобрение заявки на вывод"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        withdrawal_id = int(callback.data.split("_")[3])
        
        # Обновляем статус
        db.update_withdrawal_status(withdrawal_id, 'completed', 'Заявка одобрена администратором')
        db.log_admin_action(callback.from_user.id, "approve_withdrawal", 
                          f"Одобрил вывод #{withdrawal_id}")
        
        # Получаем данные заявки для уведомления пользователя
        withdrawals = db.get_referral_withdrawals()
        withdrawal = None
        for w in withdrawals:
            if w['withdrawal_id'] == withdrawal_id:
                withdrawal = w
                break
        
        if withdrawal:
            try:
                user_text = (
                    f"✅ <b>Ваша заявка на вывод #{withdrawal_id} одобрена!</b>\n\n"
                    f"💰 <b>Сумма:</b> {withdrawal['amount']:.2f}₽\n"
                    f"📅 <b>Дата обработки:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"Средства будут перечислены в ближайшее время согласно указанным реквизитам."
                )
                await bot.send_message(withdrawal['user_id'], user_text)
            except:
                pass
        
        await callback.answer(f"✅ Заявка #{withdrawal_id} одобрена", show_alert=True)
        await admin_view_withdrawal_handler(callback)
        
    except Exception as e:
        logger.error(f"Error approving withdrawal: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("admin_reject_withdrawal_"))
async def admin_reject_withdrawal_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отклонение заявки на вывод"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        withdrawal_id = int(callback.data.split("_")[3])
        await state.update_data(withdrawal_id=withdrawal_id)
        
        await callback.message.edit_text(
            f"❌ <b>Отклонение заявки #{withdrawal_id}</b>\n\n"
            f"Укажите причину отклонения:",
            parse_mode="HTML"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="❌ Отмена", callback_data=f"admin_view_withdrawal_{withdrawal_id}")
        
        await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error starting reject withdrawal: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.message(StateFilter(Form.waiting_for_settings))
async def process_reject_withdrawal_reason(message: Message, state: FSMContext):
    """Обработка причины отклонения заявки"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        data = await state.get_data()
        withdrawal_id = data.get('withdrawal_id')
        
        if not withdrawal_id:
            await message.answer("❌ Ошибка: ID заявки не найден")
            await state.clear()
            return
        
        reason = message.text.strip()
        
        if not reason:
            await message.answer("❌ Введите причину отклонения")
            return
        
        # Обновляем статус и возвращаем средства
        withdrawals = db.get_referral_withdrawals()
        withdrawal = None
        for w in withdrawals:
            if w['withdrawal_id'] == withdrawal_id:
                withdrawal = w
                break
        
        if withdrawal:
            # Возвращаем средства на баланс
            db.add_referral_balance(withdrawal['user_id'], withdrawal['amount'])
            
            # Обновляем статус заявки
            db.update_withdrawal_status(withdrawal_id, 'rejected', f"Отклонено: {reason}")
            
            db.log_admin_action(message.from_user.id, "reject_withdrawal", 
                              f"Отклонил вывод #{withdrawal_id}: {reason}")
            
            # Уведомляем пользователя
            try:
                user_text = (
                    f"❌ <b>Ваша заявка на вывод #{withdrawal_id} отклонена</b>\n\n"
                    f"💰 <b>Сумма:</b> {withdrawal['amount']:.2f}₽\n"
                    f"📅 <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                    f"📝 <b>Причина:</b> {reason}\n\n"
                    f"Средства возвращены на ваш реферальный баланс."
                )
                await bot.send_message(withdrawal['user_id'], user_text)
            except:
                pass
        
        await message.answer(
            f"✅ <b>Заявка #{withdrawal_id} отклонена</b>\n\n"
            f"📝 <b>Причина:</b> {reason}\n\n"
            f"Средства возвращены на баланс пользователя.",
            reply_markup=create_back_button("admin_withdrawals")
        )
        
    except Exception as e:
        logger.error(f"Error rejecting withdrawal: {e}")
        await message.answer("❌ Ошибка при отклонении заявки")
    finally:
        await state.clear()

@dp.callback_query(F.data == "admin_support")
async def admin_support_handler(callback: types.CallbackQuery):
    """Сообщения в поддержку"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    messages = db.get_support_messages('open', 10)
    
    text = f"💬 <b>Сообщения в поддержку</b> (открытых: {len(messages)})\n\n"
    
    if not messages:
        text += "Нет открытых сообщений"
    else:
        for msg in messages[:5]:
            created_at = msg['created_at']
            if isinstance(created_at, str):
                date_str = created_at[:16]
            else:
                date_str = created_at.strftime('%d.%m.%Y %H:%M')
            
            preview = msg['message'][:50] + "..." if len(msg['message']) > 50 else msg['message']
            text += f"#{msg['message_id']} - {msg['username'] or msg['full_name']}\n"
            text += f"📅 {date_str}\n"
            text += f"📝 {preview}\n\n"
    
    builder = InlineKeyboardBuilder()
    
    if messages:
        builder.button(text="📋 Все открытые", callback_data="admin_open_support")
        builder.button(text="📁 Архив", callback_data="admin_closed_support")
    
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_crypto_payments")
async def admin_crypto_payments_handler(callback: types.CallbackQuery):
    """Платежи CryptoBot"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    pending_payments = db.get_payments_by_status('pending', 10)
    paid_payments = db.get_payments_by_status('paid', 10)
    
    text = (
        f"🤖 <b>Платежи CryptoBot</b>\n\n"
        f"⏳ <b>Ожидают:</b> {len(pending_payments)}\n"
        f"✅ <b>Оплачены:</b> {len(paid_payments)}\n\n"
    )
    
    if pending_payments:
        text += "<b>Последние ожидающие:</b>\n"
        for payment in pending_payments[:3]:
            created_at = payment['created_at']
            if isinstance(created_at, str):
                date_str = created_at[11:16]
            else:
                date_str = created_at.strftime('%H:%M')
            
            text += f"• {payment['username']} - {payment['amount']} {payment['asset']}\n"
            text += f"  {payment['plan_name']} | {date_str}\n\n"
    
    builder = InlineKeyboardBuilder()
    
    if pending_payments:
        builder.button(text="⏳ Ожидающие", callback_data="admin_pending_crypto_payments")
    
    builder.button(text="✅ Оплаченные", callback_data="admin_paid_crypto_payments")
    builder.button(text="🔄 Проверить все", callback_data="admin_check_all_payments")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_check_all_payments")
async def admin_check_all_payments_handler(callback: types.CallbackQuery):
    """Проверка всех платежей"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    await callback.answer("🔄 Начинаю проверку всех платежей...", show_alert=False)
    
    try:
        pending_payments = db.get_pending_payments()
        checked = 0
        paid = 0
        
        for payment in pending_payments:
            invoice_id = payment['invoice_id']
            
            # Проверяем статус в CryptoBot
            result = await crypto_bot.get_invoices(invoice_ids=[invoice_id])
            
            if result.get('success'):
                invoices = result.get('invoices', [])
                if invoices and invoices[0]['status'] == 'paid':
                    # Обновляем статус
                    db.update_crypto_payment_status(invoice_id, 'paid')
                    paid += 1
            
            checked += 1
            await asyncio.sleep(0.5)  # Задержка чтобы не нагружать API
        
        await callback.answer(
            f"✅ Проверено {checked} платежей\n"
            f"💰 Найдено {paid} оплаченных",
            show_alert=True
        )
        
    except Exception as e:
        logger.error(f"Error checking payments: {e}")
        await callback.answer("❌ Ошибка при проверке платежей", show_alert=True)

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
    """Рассылка сообщений"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_broadcast)
    
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Введите сообщение для рассылки всем пользователям:\n\n"
        "Вы можете использовать HTML разметку:\n"
        "• <code>&lt;b&gt;жирный&lt;/b&gt;</code>\n"
        "• <code>&lt;i&gt;курсив&lt;/i&gt;</code>\n"
        "• <code>&lt;code&gt;код&lt;/code&gt;</code>\n\n"
        "Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_panel")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_broadcast)
async def process_broadcast(message: Message, state: FSMContext):
    """Обработка рассылки"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        broadcast_text = message.text.strip()
        
        if not broadcast_text:
            await message.answer("❌ Сообщение не может быть пустым")
            return
        
        # Получаем всех пользователей
        users = db.get_all_users()
        total_users = len(users)
        
        if total_users == 0:
            await message.answer("❌ Нет пользователей для рассылки")
            await state.clear()
            return
        
        # Создаем запись о рассылке
        broadcast_id = db.create_broadcast(message.from_user.id, broadcast_text, total_users)
        
        await message.answer(
            f"📢 <b>Начинаю рассылку</b>\n\n"
            f"👥 <b>Пользователей:</b> {total_users}\n"
            f"📝 <b>Сообщение:</b>\n{broadcast_text[:100]}...\n\n"
            f"Рассылка начата. ID рассылки: #{broadcast_id}",
            parse_mode="HTML"
        )
        
        # Запускаем рассылку в фоне
        asyncio.create_task(send_broadcast(broadcast_id, broadcast_text, users))
        
    except Exception as e:
        logger.error(f"Error starting broadcast: {e}")
        await message.answer(f"❌ Ошибка при начале рассылки: {str(e)[:200]}")
    finally:
        await state.clear()

async def send_broadcast(broadcast_id: int, message_text: str, users: List[Dict]):
    """Асинхронная рассылка сообщений"""
    sent = 0
    failed = 0
    
    for user in users:
        try:
            # Пропускаем заблокированных пользователей
            if user.get('is_banned'):
                failed += 1
                continue
            
            await bot.send_message(user['telegram_id'], message_text, parse_mode="HTML")
            sent += 1
            
            # Обновляем статистику каждые 10 отправок
            if (sent + failed) % 10 == 0:
                db.update_broadcast_stats(broadcast_id, sent, failed, 'sending')
                sent = 0
                failed = 0
            
            # Задержка чтобы не превысить лимиты Telegram
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error sending broadcast to user {user['telegram_id']}: {e}")
            failed += 1
    
    # Финальное обновление статистики
    db.update_broadcast_stats(broadcast_id, sent, failed, 'completed')
    
    # Уведомляем админа
    try:
        await bot.send_message(
            ADMIN_ID,
            f"✅ <b>Рассылка #{broadcast_id} завершена</b>\n\n"
            f"✅ Отправлено: {sent}\n"
            f"❌ Не отправлено: {failed}\n"
            f"👥 Всего пользователей: {len(users)}",
            parse_mode="HTML"
        )
    except:
        pass

@dp.callback_query(F.data == "admin_settings")
async def admin_settings_handler(callback: types.CallbackQuery):
    """Настройки бота"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    settings = db.get_all_settings()
    
    text = "⚙️ <b>Настройки бота</b>\n\n"
    
    # Группируем настройки
    text += "<b>Основные:</b>\n"
    for key in ['bot_name', 'bot_username', 'admin_id']:
        if key in settings:
            value = settings[key]
            text += f"• {key}: {value}\n"
    
    text += "\n<b>Триал:</b>\n"
    for key in ['trial_enabled', 'trial_duration_days']:
        if key in settings:
            text += f"• {key}: {settings[key]}\n"
    
    text += "\n<b>Рефералы:</b>\n"
    for key in ['enable_referrals', 'referral_percentage', 'referral_discount', 'minimum_withdrawal']:
        if key in settings:
            text += f"• {key}: {settings[key]}\n"
    
    text += "\n<b>Платежные системы:</b>\n"
    cryptobot_token = settings.get('cryptobot_token', '')
    text += f"• cryptobot_token: {'✅ Настроено' if cryptobot_token else '❌ Не настроено'}\n"
    
    text += "\n<b>Тексты:</b>\n"
    for key in ['about_text', 'support_text', 'support_user']:
        if key in settings:
            value = settings[key]
            preview = value[:30] + "..." if len(value) > 30 else value
            text += f"• {key}: {preview}\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data="admin_edit_settings")
    builder.button(text="🔄 Сбросить к default", callback_data="admin_reset_settings")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_edit_settings")
async def admin_edit_settings_handler(callback: types.CallbackQuery):
    """Редактирование настроек"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    settings = db.get_all_settings()
    
    text = "✏️ <b>Редактирование настроек</b>\n\n"
    text += "Выберите настройку для редактирования:\n\n"
    
    # Группируем настройки для удобства
    groups = {
        "Основные": ['bot_name', 'bot_username', 'admin_id'],
        "Триал": ['trial_enabled', 'trial_duration_days'],
        "Рефералы": ['enable_referrals', 'referral_percentage', 'referral_discount', 'minimum_withdrawal'],
        "Платежи": ['cryptobot_token'],
        "Тексты": ['about_text', 'support_text', 'support_user'],
        "Ссылки": ['channel_url', 'terms_url', 'privacy_url', 'android_url', 'ios_url', 'windows_url', 'linux_url']
    }
    
    builder = InlineKeyboardBuilder()
    
    for group_name, keys in groups.items():
        for key in keys:
            if key in settings:
                value = settings[key]
                preview = str(value)[:20] + "..." if len(str(value)) > 20 else str(value)
                builder.button(text=f"{key}: {preview}", callback_data=f"admin_edit_setting_{key}")
    
    builder.button(text="⬅️ Назад", callback_data="admin_settings")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_edit_setting_"))
async def admin_edit_setting_handler(callback: types.CallbackQuery, state: FSMContext):
    """Редактирование конкретной настройки"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    setting_key = callback.data.split("_")[3]
    current_value = db.get_setting(setting_key, "")
    
    await state.set_state(Form.waiting_for_edit_setting)
    await state.update_data(setting_key=setting_key)
    
    await callback.message.edit_text(
        f"✏️ <b>Редактирование настройки: {setting_key}</b>\n\n"
        f"Текущее значение: <code>{current_value}</code>\n\n"
        f"Введите новое значение:\n\n"
        f"Или нажмите кнопку для отмены:",
        parse_mode="HTML"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_edit_settings")
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer()

@dp.message(Form.waiting_for_edit_setting)
async def process_edit_setting(message: Message, state: FSMContext):
    """Обработка редактирования настройки"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        data = await state.get_data()
        setting_key = data['setting_key']
        new_value = message.text.strip()
        
        # Валидация для некоторых настроек
        if setting_key == 'admin_id':
            if not new_value.isdigit():
                await message.answer("❌ ID администратора должен быть числом")
                return
        
        elif setting_key == 'trial_duration_days':
            try:
                days = int(new_value)
                if days < 1 or days > 30:
                    raise ValueError
            except:
                await message.answer("❌ Длительность триала должна быть числом от 1 до 30")
                return
        
        elif setting_key in ['referral_percentage', 'referral_discount']:
            try:
                percent = float(new_value)
                if percent < 0 or percent > 100:
                    raise ValueError
            except:
                await message.answer("❌ Процент должен быть числом от 0 до 100")
                return
        
        elif setting_key == 'minimum_withdrawal':
            try:
                amount = float(new_value)
                if amount < 0:
                    raise ValueError
            except:
                await message.answer("❌ Сумма должна быть положительным числом")
                return
        
        # Обновляем настройку
        db.update_setting(setting_key, new_value)
        db.log_admin_action(message.from_user.id, "update_setting", 
                          f"Обновил настройку {setting_key}: {new_value}")
        
        await message.answer(
            f"✅ <b>Настройка обновлена!</b>\n\n"
            f"🔑 <b>Ключ:</b> {setting_key}\n"
            f"📝 <b>Новое значение:</b> {new_value}",
            reply_markup=create_back_button("admin_edit_settings")
        )
        
    except Exception as e:
        logger.error(f"Error updating setting: {e}")
        await message.answer(f"❌ Ошибка при обновлении настройки: {str(e)[:200]}")
    finally:
        await state.clear()

@dp.callback_query(F.data == "admin_logs")
async def admin_logs_handler(callback: types.CallbackQuery):
    """Логи административных действий"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    logs = db.get_admin_logs(50)
    
    text = f"📋 <b>Логи административных действий</b> (последние {len(logs)})\n\n"
    
    for log in logs[:10]:
        created_at = log['created_at']
        if isinstance(created_at, str):
            time_str = created_at[11:16]
        else:
            time_str = created_at.strftime('%H:%M')
        
        username = log['username'] or log['full_name'] or f"ID: {log['admin_id']}"
        action = log['action']
        details = log['details'][:50] + "..." if len(log['details']) > 50 else log['details']
        
        text += f"🕒 {time_str} - {username}\n"
        text += f"📝 {action}: {details}\n\n"
    
    if len(logs) > 10:
        text += f"... и еще {len(logs) - 10} записей\n"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="admin_logs")
    builder.button(text="🗑️ Очистить логи", callback_data="admin_clear_logs")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(2)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_clear_logs")
async def admin_clear_logs_handler(callback: types.CallbackQuery):
    """Очистка логов"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, очистить", callback_data="admin_confirm_clear_logs")
    builder.button(text="❌ Нет, отмена", callback_data="admin_logs")
    
    await callback.message.edit_text(
        "🗑️ <b>Очистка логов</b>\n\n"
        "Вы уверены, что хотите очистить все логи административных действий?\n"
        "⚠️ Это действие нельзя отменить!",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_confirm_clear_logs")
async def admin_confirm_clear_logs_handler(callback: types.CallbackQuery):
    """Подтверждение очистки логов"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Нет прав доступа", show_alert=True)
        return
    
    try:
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM admin_logs")
            cursor.execute("VACUUM")
        
        db.log_admin_action(callback.from_user.id, "clear_logs", "Очистил логи")
        
        await callback.answer("✅ Логи очищены", show_alert=True)
        await admin_logs_handler(callback)
        
    except Exception as e:
        logger.error(f"Error clearing logs: {e}")
        await callback.answer("❌ Ошибка при очистке логов", show_alert=True)

# === ПЕРИОДИЧЕСКИЕ ЗАДАЧИ ===
async def check_pending_payments():
    """Периодическая проверка pending платежей"""
    while True:
        try:
            payments = db.get_pending_payments()
            
            for payment in payments:
                invoice_id = payment['invoice_id']
                
                # Проверяем статус в CryptoBot
                result = await crypto_bot.get_invoices(invoice_ids=[invoice_id])
                
                if result.get('success'):
                    invoices = result.get('invoices', [])
                    
                    if invoices and invoices[0]['status'] == 'paid':
                        # Обновляем статус
                        db.update_crypto_payment_status(invoice_id, 'paid')
                        
                        # Обрабатываем успешный платеж
                        await process_successful_payment(payment)
            
            # Проверяем каждые 30 секунд
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in check_pending_payments: {e}")
            await asyncio.sleep(60)

async def process_successful_payment(payment_data: Dict):
    """Обработка успешного платежа"""
    try:
        user_id = payment_data['user_id']
        plan_id = payment_data['plan_id']
        plan = db.get_plan_by_id(plan_id)
        
        if not plan:
            logger.error(f"Plan {plan_id} not found for payment {payment_data['invoice_id']}")
            return
        
        host_data = db.get_host(plan['host_name'])
        if not host_data:
            logger.error(f"Host {plan['host_name']} not found for payment {payment_data['invoice_id']}")
            return
        
        # Создаем ключ
        email = f"user{user_id}-{plan_id}@{host_data['host_name'].replace(' ', '').lower()}.vpn"
        
        result = await xui_api.create_client(
            host_data,
            email,
            plan['months'] * 30
        )
        
        if result.get('error'):
            logger.error(f"Error creating key for payment {payment_data['invoice_id']}: {result['error']}")
            
            # Уведомляем админа об ошибке
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"❌ <b>Ошибка создания ключа</b>\n\n"
                    f"💰 <b>Платеж:</b> {payment_data['invoice_id']}\n"
                    f"👤 <b>Пользователь:</b> {payment_data['username']} (ID: {user_id})\n"
                    f"📦 <b>План:</b> {plan['plan_name']}\n"
                    f"🖥️ <b>Сервер:</b> {plan['host_name']}\n"
                    f"❌ <b>Ошибка:</b> {result['error'][:200]}",
                    parse_mode="HTML"
                )
            except:
                pass
            
            return
        
        # Сохраняем ключ
        key_id = db.add_key(
            user_id,
            host_data['host_name'],
            result['client_uuid'],
            email,
            result['expiry_date']
        )
        
        # Обновляем статистику
        metadata = json.loads(payment_data['metadata'])
        price_rub = metadata.get('price_rub', 0)
        
        db.update_user_stats(user_id, price_rub, plan['months'])
        
        # Логируем транзакцию
        user_data = db.get_user(user_id)
        db.log_transaction(
            username=user_data.get('username') or user_data.get('full_name'),
            user_id=user_id,
            status='paid',
            amount_rub=price_rub,
            payment_method='CryptoBot',
            metadata={
                'plan_id': plan_id,
                'key_id': key_id,
                'invoice_id': payment_data['invoice_id']
            }
        )
        
        # Начисляем реферальный бонус
        if ENABLE_REFERRALS and user_data and user_data.get('referred_by'):
            referrer_id = user_data['referred_by']
            bonus_amount = price_rub * (REFERRAL_PERCENTAGE / 100)
            db.add_referral_balance(referrer_id, bonus_amount)
        
        # Обновляем payment с key_id
        db.create_crypto_payment(
            invoice_id=payment_data['invoice_id'],
            user_id=user_id,
            plan_id=plan_id,
            amount=payment_data['amount'],
            asset=payment_data['asset'],
            metadata=metadata,
            key_id=key_id
        )
        
        # Отправляем ключ пользователю
        expiry_date = result['expiry_date']
        expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
        
        success_text = (
            f"🎉 <b>Оплата подтверждена!</b>\n\n"
            f"✅ <b>Ваш VPN ключ создан:</b>\n"
            f"🖥️ <b>Сервер:</b> {host_data['host_name']}\n"
            f"📅 <b>Действует до:</b> {expiry_formatted}\n"
            f"📅 <b>Срок:</b> {plan['months']} месяцев\n\n"
            f"<code>{result['connection_string']}</code>"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="📱 QR-код", callback_data=f"qr_{key_id}")
        builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
        builder.adjust(2)
        
        try:
            await bot.send_message(user_id, success_text, reply_markup=builder.as_markup())
        except Exception as e:
            logger.error(f"Error sending message to user {user_id}: {e}")
            
            # Пытаемся отправить через админа
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"📩 <b>Не удалось отправить ключ пользователю</b>\n\n"
                    f"👤 <b>Пользователь:</b> ID: {user_id}\n"
                    f"🔑 <b>Ключ:</b> #{key_id}\n"
                    f"🖥️ <b>Сервер:</b> {host_data['host_name']}\n"
                    f"📅 <b>Срок:</b> до {expiry_formatted}\n\n"
                    f"<code>{result['connection_string']}</code>",
                    parse_mode="HTML"
                )
            except:
                pass
        
    except Exception as e:
        logger.error(f"Error processing payment: {e}")

async def check_expiring_keys():
    """Проверка истекающих ключей"""
    while True:
        try:
            # Ключи которые истекут в ближайшие 3 дня
            expiring_keys = db.get_expiring_keys(3)
            
            for key in expiring_keys:
                expiry_date = datetime.fromisoformat(key['expiry_date']) if isinstance(key['expiry_date'], str) else key['expiry_date']
                days_left = calculate_days_left(expiry_date)
                
                # Отправляем напоминание за 1 день до истечения
                if days_left == 1:
                    try:
                        reminder_text = (
                            f"⏰ <b>Напоминание!</b>\n\n"
                            f"Ваш ключ VPN истекает через <b>1 день</b>!\n"
                            f"🖥️ <b>Сервер:</b> {key['host_name']}\n"
                            f"📅 <b>Истекает:</b> {expiry_date.strftime('%d.%m.%Y %H:%M')}\n\n"
                            f"Чтобы продолжить использование, продлите или купите новый ключ."
                        )
                        
                        builder = InlineKeyboardBuilder()
                        builder.button(text="🛒 Купить VPN", callback_data="buy_new_key")
                        builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
                        builder.adjust(1)
                        
                        await bot.send_message(key['user_id'], reminder_text, reply_markup=builder.as_markup())
                    except:
                        pass
            
            # Проверяем раз в день
            await asyncio.sleep(24 * 60 * 60)  # 24 часа
            
        except Exception as e:
            logger.error(f"Error checking expiring keys: {e}")
            await asyncio.sleep(60 * 60)  # 1 час при ошибке

# === ЗАПУСК БОТА ===
async def main():
    """Основная функция запуска бота"""
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не настроен!")
        sys.exit(1)
    
    print("🚀 Запуск VLESS Telegram Bot v8.0...")
    
    try:
        bot_info = await bot.get_me()
        print(f"✅ Бот запущен: @{bot_info.username}")
        print(f"👑 Админ ID: {ADMIN_ID}")
        print(f"🗄️  База данных: vless_bot.db")
        print(f"🤖 CryptoBot: {'✅ Подключен' if crypto_bot else '❌ Не настроен'}")
        print(f"🖥️  X-UI хостов: {db.get_hosts_count()}")
        print(f"📦  Тарифов: {db.get_plans_count()}")
        print(f"👥  Пользователей: {db.get_user_count()}")
        
        # Запускаем периодические задачи в фоне
        if crypto_bot:
            asyncio.create_task(check_pending_payments())
        
        asyncio.create_task(check_expiring_keys())
        
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"❌ Критическая ошибка: {e}")
        
    finally:
        await bot.session.close()
        if crypto_bot:
            await crypto_bot.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {e}")
        sys.exit(1)
