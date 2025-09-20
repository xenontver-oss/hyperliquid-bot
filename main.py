import json
import time
import requests
from hyperliquid.info import Info
from hyperliquid.utils.error import ClientError
from dotenv import load_dotenv
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sqlite3
import logging
import signal
import sys

# Настройка логирования в debug.log
logging.basicConfig(
    filename='debug.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
debug_logger = logging.getLogger('debug')

# Загрузи .env файл
load_dotenv()

# Настройки из .env
API_URL = os.getenv("API_URL")
WALLETS = os.getenv("WALLETS")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# Проверка, что все переменные загружены
missing_vars = []
if not API_URL:
    missing_vars.append("API_URL")
if not WALLETS:
    missing_vars.append("WALLETS")
if not TG_TOKEN:
    missing_vars.append("TG_TOKEN")
if not TG_CHAT_ID:
    missing_vars.append("TG_CHAT_ID")
if missing_vars:
    raise ValueError(f"Следующие переменные окружения не заданы: {', '.join(missing_vars)}")

# Безопасная обработка WALLETS
if WALLETS:
    WALLETS = WALLETS.split(",")
    WALLETS = [w.strip() for w in WALLETS]
else:
    WALLETS = []
print(f"WALLETS: {WALLETS}")
debug_logger.info(f"WALLETS: {WALLETS}")

# Загрузи конфиг для API
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("Файл config.json не найден!")
    debug_logger.error("Файл config.json не найден")
    with open("app_errors.log", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Файл config.json не найден\n")
    raise

# Загрузка названий кошельков
try:
    with open('wallet_names.json', 'r') as f:
        wallet_names_data = json.load(f)
        WALLET_NAMES = wallet_names_data.get('wallet_names', {})
except FileNotFoundError:
    print("Файл wallet_names.json не найден, используем короткие адреса в уведомлениях")
    debug_logger.warning("Файл wallet_names.json не найден")
    WALLET_NAMES = {}

# Инициализация API
try:
    info = Info(API_URL, skip_ws=False)
except ClientError as e:
    print(f"Ошибка инициализации Info: {e}")
    debug_logger.error(f"Ошибка инициализации Info: {e}")
    with open("app_errors.log", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Ошибка инициализации Info: {e}\n")
    info = Info(API_URL, skip_ws=False)

# Подключение к SQLite БД
conn = sqlite3.connect('history.db')
cursor = conn.cursor()

# Создание таблиц для каждого кошелька с добавлением колонок для комиссий
for wallet in WALLETS:
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS trades_{wallet} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            oid TEXT,
            coin TEXT,
            price REAL,
            size REAL,
            side TEXT,
            direction TEXT,
            closedPnl REAL,
            fee REAL,
            exchange_fee REAL DEFAULT 0.0,
            bot_fee REAL DEFAULT 0.0,
            startPosition REAL,
            total_open_size_long REAL,
            total_close_size_long REAL,
            remaining_open_size_long REAL,
            total_open_size_short REAL,
            total_close_size_short REAL,
            remaining_open_size_short REAL,
            accumulated_fees_long REAL DEFAULT 0.0,
            accumulated_fees_short REAL DEFAULT 0.0,
            realized_fees REAL DEFAULT 0.0,
            net_pnl REAL DEFAULT 0.0,
            UNIQUE(oid, coin, timestamp, size, startPosition)
        )
    ''')
    
    # Добавляем новые колонки к существующим таблицам если их нет
    for column in ['exchange_fee', 'bot_fee', 'accumulated_fees_long', 'accumulated_fees_short', 'realized_fees', 'net_pnl']:
        try:
            cursor.execute(f'ALTER TABLE trades_{wallet} ADD COLUMN {column} REAL DEFAULT 0.0')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
    
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS sent_trades_{wallet} (
            oid TEXT PRIMARY KEY
        )
    ''')
conn.commit()

# НЕ очищаем sent_trades для избежания дублирования уведомлений
# for wallet in WALLETS:
#     cursor.execute(f"DELETE FROM sent_trades_{wallet}")
# conn.commit()
debug_logger.info("Skipped clearing sent_trades to avoid duplicate notifications")

# Загрузка истории из БД
trades_history = {}
sent_trades_history = {}
for wallet in WALLETS:
    trades_history[wallet] = pd.read_sql(f"SELECT * FROM trades_{wallet} ORDER BY id", conn)
    sent_trades_history[wallet] = pd.read_sql(f"SELECT * FROM sent_trades_{wallet} ORDER BY oid", conn)
    debug_logger.info(f"Loaded trades_history for {wallet}: {len(trades_history[wallet])} trades")
    debug_logger.info(f"Loaded sent_trades_history for {wallet}: {len(sent_trades_history[wallet])} trades")

# Функция для отправки в Telegram
def send_tg_message(message):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    max_length = 4096
    if len(message) > max_length:
        parts = []
        current_part = ""
        for line in message.split("\n"):
            if len(current_part) + len(line) + 1 > max_length:
                parts.append(current_part)
                current_part = line + "\n"
            else:
                current_part += line + "\n"
        if current_part:
            parts.append(current_part)
    else:
        parts = [message]
    for part in parts:
        params = {"chat_id": TG_CHAT_ID, "text": part}
        try:
            response = requests.post(url, params=params, timeout=10)
            response.raise_for_status()
            print(f"Сообщение успешно отправлено в Telegram: {part[:50]}...")
            debug_logger.info(f"Сообщение успешно отправлено в Telegram: {part[:50]}...")
        except requests.RequestException as e:
            print(f"Ошибка отправки в Telegram: {e}")
            debug_logger.error(f"Ошибка отправки в Telegram: {e}")
            with open("telegram_errors.log", "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Ошибка отправки в Telegram: {e}\n")
        time.sleep(1)

# Функция для безопасного преобразования в float
def safe_float(value, default=0.0):
    if isinstance(value, (int, float)):
        return float(value)
    elif isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            debug_logger.error(f"Ошибка преобразования строки в float: {value}")
            return default
    elif isinstance(value, dict):
        debug_logger.warning(f"Найден словарь вместо числа: {value}")
        for key in ('value', 'amount', 'closedPnl', 'px', 'sz'):
            if key in value:
                try:
                    return float(value[key])
                except (ValueError, TypeError):
                    debug_logger.error(f"Не удалось преобразовать {key} из словаря: {value[key]}")
        return default
    debug_logger.error(f"Неизвестный тип данных: {type(value)} -> {value}")
    return default

def collect_all_pnl_data():
    """Собирает все данные PnL по всем кошелькам с временными метками для графиков"""
    all_trades = []
    
    for wallet in WALLETS:
        try:
            cursor.execute(f"""
                SELECT timestamp, closedPnl, net_pnl, coin, oid
                FROM trades_{wallet}
                ORDER BY timestamp ASC
            """)
            
            wallet_trades = cursor.fetchall()
            
            for trade in wallet_trades:
                all_trades.append({
                    'timestamp': trade[0],
                    'closedPnl': safe_float(trade[1]),
                    'net_pnl': safe_float(trade[2]),
                    'coin': trade[3],
                    'oid': trade[4],
                    'wallet': wallet
                })
                
        except sqlite3.Error as e:
            print(f"Ошибка при получении данных для кошелька {wallet}: {e}")
            debug_logger.error(f"Ошибка при получении данных для кошелька {wallet}: {e}")
    
    # Сортируем все сделки по времени
    all_trades.sort(key=lambda x: x['timestamp'])
    
    return all_trades

def create_cumulative_pnl_charts():
    """Создает интерактивные графики кумулятивных Closed PnL и Net PnL"""
    try:
        print("\n📊 Создание интерактивных графиков кумулятивного PnL...")
        
        # Получаем все данные
        all_trades = collect_all_pnl_data()
        
        if not all_trades:
            print("⚠️ Нет данных для создания графиков")
            return
        
        # Рассчитываем кумулятивные суммы
        cumulative_closed_pnl = 0
        cumulative_net_pnl = 0
        timestamps = []
        closed_pnl_values = []
        net_pnl_values = []
        
        for trade in all_trades:
            cumulative_closed_pnl += safe_float(trade['closedPnl']) or 0.0
            cumulative_net_pnl += safe_float(trade['net_pnl']) or 0.0
            
            timestamps.append(datetime.fromtimestamp(trade['timestamp'] / 1000))
            closed_pnl_values.append(cumulative_closed_pnl)
            net_pnl_values.append(cumulative_net_pnl)
        
        # Создаем график для Closed PnL
        fig_closed = go.Figure()
        fig_closed.add_trace(go.Scatter(
            x=timestamps,
            y=closed_pnl_values,
            mode='lines',
            name='Кумулятивный Closed PnL',
            line=dict(color='blue', width=2),
            hovertemplate='<b>Время:</b> %{x}<br>' +
                         '<b>Кумулятивный Closed PnL:</b> %{y:.4f}<br>' +
                         '<extra></extra>'
        ))
        
        fig_closed.update_layout(
            title='Кумулятивный Closed PnL по всем кошелькам',
            xaxis_title='Время',
            yaxis_title='Кумулятивный Closed PnL',
            template='plotly_white',
            hovermode='x unified'
        )
        
        # Создаем график для Net PnL
        fig_net = go.Figure()
        fig_net.add_trace(go.Scatter(
            x=timestamps,
            y=net_pnl_values,
            mode='lines',
            name='Кумулятивный Net PnL',
            line=dict(color='red' if net_pnl_values[-1] < 0 else 'green', width=2),
            hovertemplate='<b>Время:</b> %{x}<br>' +
                         '<b>Кумулятивный Net PnL:</b> %{y:.4f}<br>' +
                         '<extra></extra>'
        ))
        
        # Добавляем горизонтальную линию на нуле
        fig_net.add_hline(y=0, line_dash="dash", line_color="gray", 
                         annotation_text="Нулевая линия")
        
        fig_net.update_layout(
            title='Кумулятивный Net PnL по всем кошелькам',
            xaxis_title='Время',
            yaxis_title='Кумулятивный Net PnL',
            template='plotly_white',
            hovermode='x unified'
        )
        
        # Сохраняем графики в HTML файлы
        fig_closed.write_html("cumulative_closed_pnl.html")
        fig_net.write_html("cumulative_net_pnl.html")
        
        print(f"📈 График Closed PnL сохранен: cumulative_closed_pnl.html")
        print(f"💰 График Net PnL сохранен: cumulative_net_pnl.html")
        final_closed_pnl = safe_float(closed_pnl_values[-1]) or 0.0 if closed_pnl_values else 0.0
        final_net_pnl = safe_float(net_pnl_values[-1]) or 0.0 if net_pnl_values else 0.0
        print(f"🎯 Итоговый Closed PnL: {final_closed_pnl:.4f}")
        print(f"💹 Итоговый Net PnL: {final_net_pnl:.4f}")
        
    except Exception as e:
        error_msg = f"Ошибка при создании графиков: {e}"
        print(error_msg)
        debug_logger.error(error_msg)

def get_last_trade_timestamp(wallet):
    """Получает временную метку последней сделки для кошелька"""
    try:
        cursor.execute(f"""
            SELECT MAX(timestamp) FROM trades_{wallet}
        """)
        result = cursor.fetchone()
        return result[0] if result[0] else 0
    except sqlite3.Error as e:
        debug_logger.error(f"Ошибка при получении последней временной метки для {wallet}: {e}")
        return 0

def trade_exists_in_db(wallet, oid, coin, timestamp, size, start_position):
    """Проверяет существование сделки в базе по 5 ключевым параметрам для максимальной точности"""
    try:
        cursor.execute(f"""
            SELECT COUNT(*) FROM trades_{wallet} 
            WHERE oid = ? AND coin = ? AND timestamp = ? AND size = ? AND startPosition = ?
        """, (oid, coin, timestamp, size, start_position))
        result = cursor.fetchone()
        return result[0] > 0
    except sqlite3.Error as e:
        debug_logger.error(f"Ошибка при проверке существования сделки {oid}/{coin} для {wallet}: {e}")
        return False

def send_telegram_message(message, silent=False):
    """Отправляет сообщение в Telegram"""
    if not TG_TOKEN or not TG_CHAT_ID:
        debug_logger.warning("Telegram токен или chat_id не настроены")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        payload = {
            'chat_id': TG_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML',
            'disable_notification': silent
        }
        
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            debug_logger.info("✅ Telegram сообщение отправлено успешно")
            return True
        else:
            error_msg = f"Ошибка отправки Telegram: {response.status_code} - {response.text}"
            debug_logger.error(error_msg)
            with open("telegram_errors.log", "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")
            return False
            
    except Exception as e:
        error_msg = f"Исключение при отправке Telegram: {e}"
        debug_logger.error(error_msg)
        with open("telegram_errors.log", "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")
        return False

def is_closing_direction(direction):
    """Проверяет, является ли направление сделки закрытием или сокращением позиции"""
    closing_directions = [
        'Close Long', 'Close Short', 'Decrease Long', 'Decrease Short',
        'Close Long + Open Short', 'Close Short + Open Long'
    ]
    return direction in closing_directions

def get_wallet_name(wallet_address):
    """Получает название кошелька или возвращает короткий адрес"""
    if wallet_address in WALLET_NAMES:
        return WALLET_NAMES[wallet_address]
    else:
        # Возвращаем короткий адрес (первые 6 и последние 4 символа)
        return f"{wallet_address[:6]}...{wallet_address[-4:]}"

def format_trade_notification(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid):
    """Форматирует уведомление о сделке для Telegram"""
    
    # Определяем эмодзи для направления
    direction_emoji = {
        'Close Long': '📈🔴',
        'Close Short': '📉🔴', 
        'Decrease Long': '📈🔹',
        'Decrease Short': '📉🔹'
    }.get(direction, '💹')
    
    # Форматируем время
    trade_time = datetime.fromtimestamp(timestamp / 1000).strftime('%H:%M:%S')
    
    # Определяем символ для PnL
    pnl_symbol = "🟢" if net_pnl >= 0 else "🔴"
    
    # Получаем название кошелька
    wallet_name = get_wallet_name(wallet)
    
    message = f"""🚨 <b>{wallet_name} закрыл позицию!</b>
    
🔍 <b>OID:</b> <code>{oid}</code>
{direction_emoji} <b>Направление:</b> {direction}
💎 <b>Монета:</b> {coin}
💰 <b>Размер:</b> {size}
💵 <b>Цена:</b> ${price:.4f}
📊 <b>Closed PnL:</b> {closed_pnl:+.4f}
{pnl_symbol} <b>Net PnL:</b> {net_pnl:+.4f}
🕐 <b>Время:</b> {trade_time}
👛 <code>{wallet}</code>"""

    return message

def format_negative_pnl_alert(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid):
    """Форматирует предупреждение об отрицательном Net PnL с полными данными сделки"""
    
    # Определяем эмодзи для направления
    direction_emoji = {
        'Close Long': '📈🔴',
        'Close Short': '📉🔴', 
        'Decrease Long': '📈🔹',
        'Decrease Short': '📉🔹'
    }.get(direction, '💹')
    
    trade_time = datetime.fromtimestamp(timestamp / 1000).strftime('%H:%M:%S')
    
    # Получаем название кошелька
    wallet_name = get_wallet_name(wallet)
    
    message = f"""⚠️ <b>ПРЕДУПРЕЖДЕНИЕ: {wallet_name} - Отрицательный Net PnL</b>
    
🔍 <b>OID:</b> <code>{oid}</code>
{direction_emoji} <b>Направление:</b> {direction}
💎 <b>Монета:</b> {coin}
💰 <b>Размер:</b> {size}
💵 <b>Цена:</b> ${price:.4f}
📊 <b>Closed PnL:</b> {closed_pnl:+.4f}
🔴 <b>Net PnL:</b> {net_pnl:.4f}
🕐 <b>Время:</b> {trade_time}
👛 <code>{wallet}</code>
    
❗️ Рекомендуется проанализировать торговую стратегию"""

    return message

def send_trade_notifications(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid):
    """Отправляет уведомления о сделках в Telegram"""
    try:
        # Уведомление о сделках закрытия или сокращения позиций
        if is_closing_direction(direction):
            message = format_trade_notification(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid)
            send_telegram_message(message)
        
        # Предупреждение об отрицательном Net PnL только для закрытия/сокращения позиций
        if net_pnl is not None and net_pnl < 0 and is_closing_direction(direction):
            alert_message = format_negative_pnl_alert(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid)
            send_telegram_message(alert_message)
            
    except Exception as e:
        debug_logger.error(f"Ошибка при отправке уведомлений для сделки {coin}: {e}")

def get_open_positions_summary():
    """Получает сводку по всем открытым позициям напрямую из API"""
    open_positions = []
    
    for wallet in WALLETS:
        try:
            debug_logger.info(f"📊 Получение позиций из API для кошелька {wallet}")
            
            # Получаем данные о состоянии аккаунта напрямую из API  
            user_state = info.user_state(wallet)
            
            if not user_state:
                debug_logger.info(f"Нет данных user_state для кошелька {wallet}")
                continue
            
            # Извлекаем позиции из API
            asset_positions = user_state.get("assetPositions", [])
            
            if not asset_positions:
                debug_logger.info(f"Нет открытых позиций для кошелька {wallet}")
                continue
            
            # Обрабатываем позиции
            wallet_positions = []
            total_unrealized_pnl = 0.0
            
            for asset_position in asset_positions:
                position = asset_position.get("position", {})
                coin = position.get("coin", "")
                size = safe_float(position.get("szi", 0.0))  # szi = signed size (+ для long, - для short)
                unrealized_pnl = safe_float(position.get("unrealizedPnl", 0.0))
                entry_price = safe_float(position.get("entryPx", 0.0))
                margin_used = safe_float(position.get("marginUsed", 0.0))
                
                # Если размер позиции не равен 0, значит есть открытая позиция
                if abs(size) > 0:
                    long_size = max(0, size)  # Если size > 0, это long позиция
                    short_size = abs(min(0, size))  # Если size < 0, это short позиция
                    
                    wallet_positions.append({
                        'coin': coin,
                        'long': long_size,
                        'short': short_size,
                        'unrealized_pnl': unrealized_pnl,
                        'entry_price': entry_price,
                        'margin_used': margin_used
                    })
                    
                    total_unrealized_pnl += unrealized_pnl
            
            # Если есть открытые позиции, добавляем в результат
            if wallet_positions:
                # Получаем накопленный Net PnL из нашей базы для сравнения
                cursor.execute(f"""
                    SELECT SUM(net_pnl)
                    FROM trades_{wallet}
                """)
                result = cursor.fetchone()
                realized_net_pnl = safe_float(result[0]) if result and result[0] is not None else 0.0
                
                # Общий PnL = realized (из базы) + unrealized (из API)
                total_pnl = realized_net_pnl + total_unrealized_pnl
                
                open_positions.append({
                    'wallet': wallet,
                    'positions': wallet_positions,
                    'realized_pnl': realized_net_pnl,
                    'unrealized_pnl': total_unrealized_pnl,
                    'total_pnl': total_pnl
                })
                
                debug_logger.info(f"✅ Кошелек {wallet}: {len(wallet_positions)} позиций, unrealized PnL: {total_unrealized_pnl:.4f}")
                
        except Exception as e:
            debug_logger.error(f"Ошибка при получении позиций из API для {wallet}: {e}")
            continue
    
    return open_positions

def format_hourly_positions_report(open_positions):
    """Форматирует часовой отчет по открытым позициям"""
    current_time = datetime.now().strftime('%H:%M:%S')
    
    if not open_positions:
        return f"""📊 <b>ЧАСОВОЙ ОТЧЕТ ПО ПОЗИЦИЯМ</b>
        
✅ Все позиции закрыты!
🕐 <b>Время:</b> {current_time}"""
    
    report_lines = [f"📊 <b>ЧАСОВОЙ ОТЧЕТ ПО ОТКРЫТЫМ ПОЗИЦИЯМ</b>"]
    report_lines.append(f"🕐 <b>Время:</b> {current_time}")
    report_lines.append("")
    
    # Сортируем по общему PnL (убыточные первыми для контроля)
    sorted_positions = sorted(open_positions, key=lambda x: x['total_pnl'])
    
    for pos in sorted_positions:
        wallet = pos['wallet']
        wallet_name = get_wallet_name(wallet)
        realized_pnl = pos['realized_pnl']
        unrealized_pnl = pos['unrealized_pnl']
        total_pnl = pos['total_pnl']
        positions = pos['positions']
        
        # Эмодзи в зависимости от результата
        pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
        
        report_lines.append(f"📍 <b>{wallet_name}</b>")
        report_lines.append(f"   💰 Общий PnL: ${total_pnl:.2f} {pnl_emoji}")
        report_lines.append(f"   📈 Realized: ${realized_pnl:.2f} | 🔄 Unrealized: ${unrealized_pnl:.2f}")
        
        # Показываем позиции по монетам с деталями
        for coin_pos in positions:
            coin = coin_pos['coin']
            long_pos = coin_pos['long']
            short_pos = coin_pos['short']
            unrealized = coin_pos['unrealized_pnl']
            entry_px = coin_pos['entry_price']
            
            pos_info = []
            if long_pos > 0:
                pos_info.append(f"📈 Long: {long_pos:.4f}")
            if short_pos > 0:
                pos_info.append(f"📉 Short: {short_pos:.4f}")
            
            if pos_info:
                position_line = f"   🪙 <b>{coin}:</b> {' | '.join(pos_info)}"
                if entry_px > 0:
                    position_line += f" @ ${entry_px:.4f}"
                if abs(unrealized) > 0.01:
                    unrealized_emoji = "🟢" if unrealized >= 0 else "🔴"
                    position_line += f" (${unrealized:+.2f} {unrealized_emoji})"
                report_lines.append(position_line)
        
        report_lines.append(f"   👛 <code>{wallet}</code>")
        report_lines.append("")
    
    # Общая статистика
    total_wallets = len(open_positions)
    positive_pnl = sum(1 for pos in open_positions if pos['total_pnl'] > 0)
    negative_pnl = total_wallets - positive_pnl
    total_unrealized = sum(pos['unrealized_pnl'] for pos in open_positions)
    
    report_lines.append("📈 <b>СВОДКА:</b>")
    report_lines.append(f"   🔢 Кошельков с позициями: {total_wallets}")
    report_lines.append(f"   🟢 В плюсе: {positive_pnl}")
    report_lines.append(f"   🔴 В минусе: {negative_pnl}")
    report_lines.append(f"   🔄 Общий Unrealized PnL: ${total_unrealized:.2f}")
    
    return '\n'.join(report_lines)

def send_startup_daily_summary():
    """Отправляет суточный отчет NET PnL за последние 24 часа при запуске"""
    try:
        print("\n📅 Создание суточного отчета NET PnL за 24 часа...")
        debug_logger.info("📅 Создание суточного отчета NET PnL за 24 часа...")
        
        # Временная метка 24 часа назад (в миллисекундах)
        twenty_four_hours_ago = int((time.time() - (24 * 60 * 60)) * 1000)
        
        wallet_results = []
        
        # Собираем данные по каждому кошельку
        for wallet in WALLETS:
            try:
                cursor.execute(f"""
                    SELECT 
                        SUM(net_pnl) as daily_net_pnl,
                        COUNT(*) as trades_count
                    FROM trades_{wallet} 
                    WHERE timestamp >= ?
                """, (twenty_four_hours_ago,))
                
                result = cursor.fetchone()
                if result and result[0] is not None:
                    daily_net_pnl = safe_float(result[0], 0.0)
                    trades_count = int(result[1]) if result[1] else 0
                    
                    wallet_results.append({
                        'wallet': wallet,
                        'wallet_name': get_wallet_name(wallet),
                        'daily_net_pnl': daily_net_pnl,
                        'trades_count': trades_count
                    })
                else:
                    # Нет сделок за 24 часа
                    wallet_results.append({
                        'wallet': wallet,
                        'wallet_name': get_wallet_name(wallet),
                        'daily_net_pnl': 0.0,
                        'trades_count': 0
                    })
                    
            except Exception as e:
                debug_logger.error(f"Ошибка получения данных для кошелька {wallet}: {e}")
                wallet_results.append({
                    'wallet': wallet,
                    'wallet_name': get_wallet_name(wallet),
                    'daily_net_pnl': 0.0,
                    'trades_count': 0
                })
        
        # Сортируем кошельки от лучших к худшим
        wallet_results.sort(key=lambda x: x['daily_net_pnl'], reverse=True)
        
        # Форматируем отчет
        report_message = format_daily_summary_report(wallet_results)
        
        # Отправляем в Telegram
        if send_telegram_message(report_message):
            print(f"✅ Суточный отчет NET PnL отправлен (кошельков: {len(wallet_results)})")
            debug_logger.info(f"✅ Суточный отчет NET PnL отправлен (кошельков: {len(wallet_results)})")
        else:
            print("❌ Ошибка при отправке суточного отчета")
            
    except Exception as e:
        error_msg = f"❌ Ошибка при создании суточного отчета: {e}"
        print(error_msg)
        debug_logger.error(error_msg)

def format_daily_summary_report(wallet_results):
    """Форматирует суточный отчет NET PnL"""
    current_time = datetime.now().strftime('%d.%m.%Y %H:%M')
    
    # Фильтруем на прибыльные и убыточные
    profitable_wallets = [w for w in wallet_results if w['daily_net_pnl'] > 0]
    losing_wallets = [w for w in wallet_results if w['daily_net_pnl'] < 0]
    zero_wallets = [w for w in wallet_results if w['daily_net_pnl'] == 0]
    
    # Считаем общую статистику
    total_net_pnl = sum(w['daily_net_pnl'] for w in wallet_results)
    total_trades = sum(w['trades_count'] for w in wallet_results)
    
    report_lines = [f"📅 <b>СУТОЧНАЯ СВОДКА NET PnL (24ч)</b>"]
    report_lines.append(f"🕐 {current_time}")
    report_lines.append("")
    
    # ТОП-3 лучших кошелька
    if profitable_wallets:
        report_lines.append("🏆 <b>ТОП КОШЕЛЬКИ:</b>")
        top_count = min(3, len(profitable_wallets))
        medals = ["🥇", "🥈", "🥉"]
        
        for i in range(top_count):
            wallet = profitable_wallets[i]
            medal = medals[i] if i < len(medals) else "🔹"
            trades_info = f" ({wallet['trades_count']} сделок)" if wallet['trades_count'] > 0 else ""
            # Название кошелька жирное, адрес копируется полностью
            wallet_name_bold = f"<b>{wallet['wallet_name']}</b>"
            wallet_addr_copy = f"<code>{wallet['wallet']}</code>"
            report_lines.append(f"{medal} {wallet_name_bold}")
            report_lines.append(f"💼 {wallet_addr_copy}")
            report_lines.append(f"💰 +${wallet['daily_net_pnl']:.2f}{trades_info}")
            report_lines.append("")  # Пробел между кошельками
        
        report_lines.append("")
    
    # ТОП-3 худших кошелька  
    if losing_wallets:
        report_lines.append("⚠️ <b>ПРОБЛЕМНЫЕ КОШЕЛЬКИ:</b>")
        worst_count = min(3, len(losing_wallets))
        # Худшие в конце списка (уже отсортированы)
        worst_wallets = wallet_results[-worst_count:] if len(wallet_results) >= worst_count else losing_wallets
        
        for wallet in reversed(worst_wallets):
            if wallet['daily_net_pnl'] < 0:
                trades_info = f" ({wallet['trades_count']} сделок)" if wallet['trades_count'] > 0 else ""
                # Название кошелька жирное, адрес копируется полностью
                wallet_name_bold = f"<b>{wallet['wallet_name']}</b>"
                wallet_addr_copy = f"<code>{wallet['wallet']}</code>"
                report_lines.append(f"🔴 {wallet_name_bold}")
                report_lines.append(f"💼 {wallet_addr_copy}")
                report_lines.append(f"💰 ${wallet['daily_net_pnl']:.2f}{trades_info}")
                report_lines.append("")  # Пробел между кошельками
        
        report_lines.append("")
    
    # Общая статистика
    profit_count = len(profitable_wallets)
    loss_count = len(losing_wallets)
    zero_count = len(zero_wallets)
    total_wallets = len(wallet_results)
    
    report_lines.append("📊 <b>ОБЩАЯ СТАТИСТИКА:</b>")
    report_lines.append(f"🟢 Прибыльных: {profit_count} / 🔴 Убыточных: {loss_count} / ⚪ Без сделок: {zero_count}")
    
    # Эмодзи для общего результата
    total_emoji = "🟢" if total_net_pnl > 0 else "🔴" if total_net_pnl < 0 else "⚪"
    report_lines.append(f"💹 <b>Суммарный NET PnL:</b> ${total_net_pnl:+.2f} {total_emoji}")
    report_lines.append(f"📈 <b>Всего сделок:</b> {total_trades}")
    
    return "\n".join(report_lines)

def send_hourly_positions_report():
    """Отправляет часовой отчет по открытым позициям"""
    try:
        print("\n📊 Создание часового отчета по позициям...")
        debug_logger.info("📊 Создание часового отчета по позициям...")
        
        open_positions = get_open_positions_summary()
        report_message = format_hourly_positions_report(open_positions)
        
        # Отправляем в Telegram
        if send_telegram_message(report_message):
            print(f"✅ Часовой отчет отправлен (кошельков с позициями: {len(open_positions)})")
            debug_logger.info(f"✅ Часовой отчет отправлен (кошельков с позициями: {len(open_positions)})")
        else:
            print("❌ Ошибка при отправке часового отчета")
            
    except Exception as e:
        error_msg = f"❌ Ошибка при создании часового отчета: {e}"
        print(error_msg)
        debug_logger.error(error_msg)

def send_monitoring_start_notification():
    """Отправляет уведомление о запуске мониторинга с общей сводкой"""
    try:
        # Получаем общую сводку по кошелькам
        total_closed_pnl = 0
        total_net_pnl = 0
        positive_wallets = 0
        negative_wallets = 0
        
        for wallet in WALLETS:
            try:
                cursor.execute(f"""
                    SELECT SUM(closedPnl) as total_closed_pnl, SUM(net_pnl) as total_net_pnl 
                    FROM trades_{wallet}
                """)
                
                result = cursor.fetchone()
                if result and result[0] is not None:
                    wallet_closed_pnl = safe_float(result[0])
                    wallet_net_pnl = safe_float(result[1])
                    
                    total_closed_pnl += wallet_closed_pnl
                    total_net_pnl += wallet_net_pnl
                    
                    if wallet_net_pnl >= 0:
                        positive_wallets += 1
                    else:
                        negative_wallets += 1
                        
            except sqlite3.Error:
                continue
        
        # Форматируем уведомление
        pnl_symbol = "🟢" if total_net_pnl >= 0 else "🔴"
        
        message = f"""🚀 <b>Мониторинг Hyperliquid запущен</b>

📊 <b>Общая сводка по портфелю:</b>
📈 Closed PnL: {total_closed_pnl:+.4f}
{pnl_symbol} Net PnL: {total_net_pnl:+.4f}

💼 <b>Статистика кошельков:</b>
🟢 Прибыльных: {positive_wallets}
🔴 Убыточных: {negative_wallets}

⏰ <b>Автоматический мониторинг каждые 5 минут</b>
🔔 Уведомления о сделках закрытия и отрицательном PnL активированы"""

        send_telegram_message(message, silent=True)
        
    except Exception as e:
        debug_logger.error(f"Ошибка при отправке уведомления о запуске мониторинга: {e}")

def send_incremental_summary(total_new_trades, wallets_with_new_trades):
    """Отправляет краткую сводку об инкрементальном обновлении"""
    if total_new_trades == 0:
        return  # Не отправляем уведомление если нет новых сделок
        
    try:
        current_time = datetime.now().strftime('%H:%M:%S')
        
        message = f"""📊 <b>Обновление данных</b> - {current_time}

🆕 <b>Найдено новых сделок:</b> {total_new_trades}
💼 <b>Обновлено кошельков:</b> {len(wallets_with_new_trades)}

📈 Графики и отчеты обновлены автоматически"""

        send_telegram_message(message, silent=True)
        
    except Exception as e:
        debug_logger.error(f"Ошибка при отправке сводки инкрементального обновления: {e}")

def debug_wallet_data(wallet):
    """Отладочная функция для проверки данных кошелька"""
    try:
        debug_logger.info(f"🔧 Отладка данных для кошелька {wallet}")
        
        # Проверяем данные в базе
        cursor.execute(f"""
            SELECT COUNT(*) as total_trades, 
                   MIN(timestamp) as min_ts, 
                   MAX(timestamp) as max_ts 
            FROM trades_{wallet}
        """)
        
        result = cursor.fetchone()
        if result:
            total_trades, min_ts, max_ts = result
            debug_logger.info(f"📊 База данных: {total_trades} сделок, диапазон: {min_ts} - {max_ts}")
            
            # Показываем последние 3 сделки
            cursor.execute(f"""
                SELECT timestamp, coin, oid, size, direction 
                FROM trades_{wallet} 
                ORDER BY timestamp DESC 
                LIMIT 3
            """)
            
            recent_trades = cursor.fetchall()
            debug_logger.info(f"🕐 Последние сделки в базе:")
            for trade in recent_trades:
                debug_logger.info(f"  - {trade[0]} | {trade[1]} | {trade[2][:8]} | {trade[3]} | {trade[4]}")
        
        # Проверяем данные из API
        user_fills = info.user_fills(wallet)
        if user_fills:
            debug_logger.info(f"🌐 API данные: {len(user_fills)} сделок")
            
            # Показываем последние 3 сделки из API
            sorted_fills = sorted(user_fills, key=lambda x: x.get('time', 0), reverse=True)
            debug_logger.info(f"🕐 Последние сделки из API:")
            for i, fill in enumerate(sorted_fills[:3]):
                timestamp = int(fill.get('time', 0))
                coin = fill.get('coin', '')
                oid = str(fill.get('oid', ''))[:8]
                size = safe_float(fill.get('sz', 0.0))
                debug_logger.info(f"  - {timestamp} | {coin} | {oid} | {size}")
                
    except Exception as e:
        debug_logger.error(f"Ошибка при отладке данных кошелька {wallet}: {e}")

def process_incremental_trades_for_wallet(wallet):
    """Обрабатывает только новые сделки для одного кошелька"""
    try:
        # Получаем временную метку последней сделки из базы
        last_timestamp = get_last_trade_timestamp(wallet)
        debug_logger.info(f"🕐 Последняя временная метка для {wallet}: {last_timestamp}")
        
        # Получаем данные о сделках из API
        user_fills = info.user_fills(wallet)
        
        if not user_fills:
            debug_logger.info(f"Нет данных о сделках для кошелька {wallet}")
            return 0
        
        debug_logger.info(f"📊 Получено {len(user_fills)} сделок из API для кошелька {wallet}")
        
        # Фильтруем только новые сделки
        new_trades = []
        api_timestamps = []
        
        for fill in user_fills:
            trade_timestamp = int(fill.get('time', 0))
            trade_oid = str(fill.get('oid', ''))
            coin = fill.get('coin', '')
            size = safe_float(fill.get('sz', 0.0))
            start_position = safe_float(fill.get('startPosition', 0.0))
            
            api_timestamps.append(trade_timestamp)
            
            # Проверяем, что сделка новее последней И не существует в базе (5-параметровая проверка)
            # Используем >= потому что API может возвращать сделки с той же временной меткой, но они могут быть новыми
            if trade_timestamp >= last_timestamp:
                exists_in_db = trade_exists_in_db(wallet, trade_oid, coin, trade_timestamp, size, start_position)
                debug_logger.info(f"🔍 Сделка {trade_oid}/{coin} timestamp={trade_timestamp}: новее_или_равно={trade_timestamp >= last_timestamp}, в_базе={exists_in_db}")
                
                if not exists_in_db:
                    new_trades.append(fill)
                    debug_logger.info(f"✅ Новая сделка добавлена: {trade_oid}/{coin}")
                else:
                    debug_logger.info(f"⏭️ Сделка {trade_oid}/{coin} уже в базе, пропускаем")
        
        if api_timestamps:
            min_api_ts = min(api_timestamps)
            max_api_ts = max(api_timestamps)
            debug_logger.info(f"⏰ Диапазон API: {min_api_ts} - {max_api_ts}, База: {last_timestamp}")
        
        if not new_trades:
            debug_logger.info(f"✅ Новых сделок для кошелька {wallet} не найдено")
            return 0
        
        debug_logger.info(f"🆕 Найдено {len(new_trades)} новых сделок для кошелька {wallet}")
        
        # Обрабатываем новые сделки (от старых к новым)
        new_trades_sorted = sorted(new_trades, key=lambda x: x.get('time', 0))
        processed_count = 0
        
        for fill in new_trades_sorted:
            try:
                # Извлекаем данные сделки
                oid = str(fill.get('oid', ''))
                coin = fill.get('coin', '')
                timestamp = int(fill.get('time', 0))
                price = safe_float(fill.get('px', 0.0))
                size = safe_float(fill.get('sz', 0.0))
                side = fill.get('side', '')
                closed_pnl = safe_float(fill.get('closedPnl', 0.0))
                fee = safe_float(fill.get('fee', 0.0))
                start_position = safe_float(fill.get('startPosition', 0.0))
                
                # Рассчитываем комиссию бота
                bot_fee_bps = config.get('bot_fee_bps', 5)
                bot_fee = calculate_bot_fee(size, price, bot_fee_bps)
                
                # Определяем направление сделки
                direction = get_direction(fill, wallet)
                
                # ИСПРАВЛЕНИЕ: Получаем текущие накопленные значения для данной монеты
                current_totals = get_current_totals(wallet, coin, cursor)
                
                # ИСПРАВЛЕНИЕ: Рассчитываем новые накопленные значения (с комиссиями) 
                new_totals = calculate_new_totals(current_totals, direction, size, fee, bot_fee)
                
                # ИСПРАВЛЕНИЕ: Рассчитываем чистый PnL с учетом всех комиссий
                close_fee_only = new_totals.get('close_fee_only', None)
                net_pnl = calculate_net_pnl(closed_pnl, fee, bot_fee, new_totals['realized_fees'], close_fee_only)
                
                # Вставляем новую сделку в базу с накопленными значениями
                cursor.execute(f'''
                    INSERT INTO trades_{wallet} (
                        oid, coin, timestamp, price, size, side, direction, 
                        closedPnl, fee, startPosition,
                        total_open_size_long, total_close_size_long, remaining_open_size_long,
                        total_open_size_short, total_close_size_short, remaining_open_size_short,
                        accumulated_fees_long, accumulated_fees_short, realized_fees, net_pnl,
                        exchange_fee, bot_fee
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (oid, coin, timestamp, price, size, side, direction,
                     closed_pnl, fee, start_position,
                     new_totals['total_open_size_long'], new_totals['total_close_size_long'], new_totals['remaining_open_size_long'],
                     new_totals['total_open_size_short'], new_totals['total_close_size_short'], new_totals['remaining_open_size_short'],
                     new_totals['accumulated_fees_long'], new_totals['accumulated_fees_short'], new_totals['realized_fees'], net_pnl,
                     fee, bot_fee))
                
                processed_count += 1
                
                # Отправляем уведомления только для новых сделок (инкрементальное обновление)
                send_trade_notifications(wallet, coin, direction, size, price, closed_pnl, net_pnl, timestamp, oid)
                
            except Exception as e:
                debug_logger.error(f"Ошибка при обработке сделки {fill.get('oid', 'unknown')}: {e}")
                continue
        
        # Сохраняем изменения
        conn.commit()
        debug_logger.info(f"✅ Обработано {processed_count} новых сделок для кошелька {wallet}")
        return processed_count
        
    except Exception as e:
        debug_logger.error(f"Ошибка при инкрементальной обработке кошелька {wallet}: {e}")
        return 0

def collect_pnl_data_by_wallets():
    """Собирает данные PnL для каждого кошелька отдельно"""
    wallets_data = {}
    
    for wallet in WALLETS:
        try:
            cursor.execute(f"""
                SELECT timestamp, closedPnl, net_pnl, coin, oid
                FROM trades_{wallet}
                ORDER BY timestamp ASC
            """)
            
            wallet_trades = cursor.fetchall()
            wallet_trades_list = []
            
            for trade in wallet_trades:
                wallet_trades_list.append({
                    'timestamp': trade[0],
                    'closedPnl': safe_float(trade[1]),
                    'net_pnl': safe_float(trade[2]),
                    'coin': trade[3],
                    'oid': trade[4]
                })
            
            wallets_data[wallet] = wallet_trades_list
            
        except sqlite3.Error as e:
            print(f"Ошибка при получении данных для кошелька {wallet}: {e}")
            debug_logger.error(f"Ошибка при получении данных для кошелька {wallet}: {e}")
            wallets_data[wallet] = []
    
    return wallets_data

def create_per_wallet_pnl_charts():
    """Создает графики с детализацией по каждому кошельку на одном графике"""
    try:
        print("\n📊 Создание графиков PnL по кошелькам...")
        
        # Получаем данные по каждому кошельку
        wallets_data = collect_pnl_data_by_wallets()
        
        # Создаем график для Closed PnL по кошелькам
        fig_closed_wallets = go.Figure()
        
        # Создаем график для Net PnL по кошелькам  
        fig_net_wallets = go.Figure()
        
        # Цвета для разных кошельков
        colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
        
        for i, (wallet, trades) in enumerate(wallets_data.items()):
            if not trades:
                continue
                
            # Рассчитываем кумулятивные значения для кошелька
            cumulative_closed_pnl = 0
            cumulative_net_pnl = 0
            timestamps = []
            closed_pnl_values = []
            net_pnl_values = []
            
            for trade in trades:
                cumulative_closed_pnl += safe_float(trade['closedPnl']) or 0.0
                cumulative_net_pnl += safe_float(trade['net_pnl']) or 0.0
                
                timestamps.append(datetime.fromtimestamp(trade['timestamp'] / 1000))
                closed_pnl_values.append(cumulative_closed_pnl)
                net_pnl_values.append(cumulative_net_pnl)
            
            # Короткое имя кошелька для легенды
            wallet_short = f"{wallet[:6]}...{wallet[-4:]}"
            color = colors[i % len(colors)]
            
            # Добавляем линию для Closed PnL
            fig_closed_wallets.add_trace(go.Scatter(
                x=timestamps,
                y=closed_pnl_values,
                mode='lines',
                name=f'{wallet_short}',
                line=dict(color=color, width=2),
                hovertemplate=f'<b>Кошелек:</b> {wallet_short}<br>' +
                             '<b>Время:</b> %{x}<br>' +
                             '<b>Кумулятивный Closed PnL:</b> %{y:.4f}<br>' +
                             '<extra></extra>'
            ))
            
            # Добавляем линию для Net PnL
            fig_net_wallets.add_trace(go.Scatter(
                x=timestamps,
                y=net_pnl_values,
                mode='lines',
                name=f'{wallet_short}',
                line=dict(color=color, width=2),
                hovertemplate=f'<b>Кошелек:</b> {wallet_short}<br>' +
                             '<b>Время:</b> %{x}<br>' +
                             '<b>Кумулятивный Net PnL:</b> %{y:.4f}<br>' +
                             '<extra></extra>'
            ))
        
        # Настраиваем график Closed PnL
        fig_closed_wallets.update_layout(
            title='Кумулятивный Closed PnL по кошелькам (детализация)',
            xaxis_title='Время',
            yaxis_title='Кумулятивный Closed PnL',
            template='plotly_white',
            hovermode='x unified',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        
        # Настраиваем график Net PnL
        fig_net_wallets.add_hline(y=0, line_dash="dash", line_color="gray", 
                                 annotation_text="Нулевая линия")
        
        fig_net_wallets.update_layout(
            title='Кумулятивный Net PnL по кошелькам (детализация)',
            xaxis_title='Время',
            yaxis_title='Кумулятивный Net PnL',
            template='plotly_white',
            hovermode='x unified',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        
        # Сохраняем графики
        fig_closed_wallets.write_html("closed_pnl_by_wallets.html")
        fig_net_wallets.write_html("net_pnl_by_wallets.html")
        
        print(f"📈 График Closed PnL по кошелькам сохранен: closed_pnl_by_wallets.html")
        print(f"💰 График Net PnL по кошелькам сохранен: net_pnl_by_wallets.html")
        
    except Exception as e:
        error_msg = f"Ошибка при создании графиков по кошелькам: {e}"
        print(error_msg)
        debug_logger.error(error_msg)

# Функция для форматирования времени
def format_timestamp(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        debug_logger.error(f"Ошибка форматирования времени: {e}")
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

# Функция для определения направления сделки (исправлена критическая ошибка)
def get_direction(fill, wallet_address):
    dir_field = fill.get('dir') or fill.get('direction') or ''
    side = fill.get('side', '').lower()
    start_position = safe_float(fill.get('startPosition', 0.0))
    sz = safe_float(fill.get('sz', 0.0))
    liquidation = fill.get('liquidation', None)
    address = wallet_address.lower()
    debug_logger.debug(f"get_direction: oid={fill.get('oid', 'N/A')}, dir={dir_field}, side={side}, startPosition={start_position}, sz={sz}, liquidation={liquidation}, address={address}")
    
    if sz <= 0:
        debug_logger.warning(f"Invalid sz {sz} for oid {fill.get('oid', 'N/A')}")
        return f"Invalid (sz: {sz})"
    
    # Обработка ликвидации
    if liquidation and liquidation.get('method') == 'market':
        debug_logger.info(f"Liquidation detected for oid {fill.get('oid', 'N/A')}")
        liquidated_user = liquidation.get('liquidatedUser', 'N/A').lower()
        
        # ИСПРАВЛЕНИЕ: Используем поле dir из API вместо вычисления по start_position
        # API точно знает направление ликвидации
        if 'Long' in dir_field:
            base_direction = 'Long'
        elif 'Short' in dir_field:
            base_direction = 'Short'
        else:
            # Fallback на старую логику только если dir_field неясен
            base_direction = 'Long' if start_position >= 0 else 'Short'
        
        debug_logger.info(f"Liquidation direction determined: dir_field='{dir_field}' -> base_direction='{base_direction}'")
        
        if liquidated_user == address:
            # Для short позиции: если sz равен abs(start_position), то полное закрытие
            # Для long позиции: если sz равен start_position, то полное закрытие
            if start_position < 0:  # Short позиция
                if abs(sz - abs(start_position)) < 1e-8:
                    return f'Close {base_direction} (Liquidation)'
                else:
                    return f'Decrease {base_direction} (Partial Liquidation)'
            else:  # Long позиция
                if abs(sz - start_position) < 1e-8:
                    return f'Close {base_direction} (Liquidation)'
                else:
                    return f'Decrease {base_direction} (Partial Liquidation)'
    
    # ИСПРАВЛЕНИЕ: Используем логику на основе изменения позиции и стороны
    # API возвращает side как 'a' (ask/sell) или 'b' (bid/buy)
    # Определяем направление на основе startPosition и изменения
    
    # Для buy сделок (side 'b'): позиция увеличивается (становится более положительной)
    if side == 'b' or side == 'buy':
        if start_position <= 0:  # Было short или ноль
            if abs(start_position + sz) < 1e-10:  # Позиция закрывается полностью
                return 'Close Short'
            elif start_position + sz > 0:  # Позиция переходит в long
                if abs(start_position) < 1e-10:
                    return 'Open Long'
                else:
                    return 'Close Short + Open Long'  # Частичное закрытие short + открытие long
            else:  # Остается short, но уменьшается
                return 'Decrease Short'
        else:  # Было long
            return 'Increase Long'
    
    # Для sell сделок (side 'a'): позиция уменьшается (становится более отрицательной)
    elif side == 'a' or side == 'sell':
        if start_position >= 0:  # Было long или ноль
            if abs(start_position - sz) < 1e-10:  # Позиция закрывается полностью
                return 'Close Long'
            elif start_position - sz < 0:  # Позиция переходит в short
                if abs(start_position) < 1e-10:
                    return 'Open Short'
                else:
                    return 'Close Long + Open Short'  # Частичное закрытие long + открытие short
            else:  # Остается long, но уменьшается
                return 'Decrease Long'
        else:  # Было short
            return 'Increase Short'
    
    # Fallback для неопределенных случаев
    debug_logger.warning(f"Unknown direction for oid {fill.get('oid', 'N/A')}: dir={dir_field}, side={side}, startPosition={start_position}, sz={sz}")
    return f"Unknown (dir: {dir_field}, side: {side}, startPosition: {start_position}, sz: {sz})"

# Функция для расчета комиссии бота
def calculate_bot_fee(size, price, bot_fee_bps=5):
    """Рассчитывает комиссию бота от объема сделки"""
    volume = safe_float(size, 0.0) * safe_float(price, 0.0)
    bot_fee = volume * (bot_fee_bps / 10000.0)  # bps в десятичную дробь
    return bot_fee

# Функция для получения текущих накопленных значений для монеты
def get_current_totals(wallet, coin, cursor):
    """Получает текущие накопленные значения для данной монеты из последней записи"""
    cursor.execute(f'''
        SELECT total_open_size_long, total_close_size_long, 
               total_open_size_short, total_close_size_short,
               accumulated_fees_long, accumulated_fees_short
        FROM trades_{wallet} 
        WHERE coin = ? 
        ORDER BY id DESC 
        LIMIT 1
    ''', (coin,))
    
    result = cursor.fetchone()
    if result:
        return {
            'total_open_size_long': safe_float(result[0], 0.0),
            'total_close_size_long': safe_float(result[1], 0.0),
            'total_open_size_short': safe_float(result[2], 0.0),
            'total_close_size_short': safe_float(result[3], 0.0),
            'accumulated_fees_long': safe_float(result[4], 0.0),
            'accumulated_fees_short': safe_float(result[5], 0.0)
        }
    else:
        return {
            'total_open_size_long': 0.0,
            'total_close_size_long': 0.0,
            'total_open_size_short': 0.0,
            'total_close_size_short': 0.0,
            'accumulated_fees_long': 0.0,
            'accumulated_fees_short': 0.0
        }

# Функция для расчета новых накопленных значений с комиссиями
def calculate_new_totals(current_totals, direction, size, exchange_fee, bot_fee):
    open_directions = ['Open Long', 'Increase Long', 'Open Long (Liquidation Taker)', 'Increase Long (Liquidation Taker)']
    close_directions = ['Close Long', 'Decrease Long', 'Close Long (Liquidation)', 'Decrease Long (Partial Liquidation)']
    short_open_directions = ['Open Short', 'Increase Short', 'Open Short (Liquidation Taker)', 'Increase Short (Liquidation Taker)']
    short_close_directions = ['Close Short', 'Decrease Short', 'Close Short (Liquidation)', 'Decrease Short (Partial Liquidation)']
    combined_directions = ['Close Short + Open Long', 'Close Long + Open Short']
    
    new_totals = current_totals.copy()
    size = safe_float(size, 0.0)
    total_trade_fee = safe_float(exchange_fee, 0.0) + safe_float(bot_fee, 0.0)
    realized_fees = 0.0
    
    if size > 0:
        if direction in open_directions:
            new_totals['total_open_size_long'] += size
            # Накапливаем комиссии при открытии Long позиций
            new_totals['accumulated_fees_long'] += total_trade_fee
            
        elif direction in close_directions:
            new_totals['total_close_size_long'] += size
            # При закрытии Long позиции списываем пропорциональную часть накопленных комиссий
            current_remaining = new_totals['total_open_size_long'] - current_totals['total_close_size_long']
            if current_remaining > 0 and new_totals['accumulated_fees_long'] > 0:
                proportion = size / current_remaining
                proportion = min(proportion, 1.0)
                realized_fees = new_totals['accumulated_fees_long'] * proportion
                new_totals['accumulated_fees_long'] -= realized_fees
                
                # Проверка: если позиция закрыта полностью, accumulated_fees_long должно быть равно нулю
                remaining_after_close = new_totals['total_open_size_long'] - new_totals['total_close_size_long']
                if abs(remaining_after_close) < 1e-10 and abs(new_totals['accumulated_fees_long']) > 1e-6:
                    debug_logger.warning(f"!!! FEES WARNING: Long position fully closed but accumulated_fees_long is not zero: {new_totals['accumulated_fees_long']:.6f}")
                    new_totals['fees_warning_long'] = True
                    realized_fees += new_totals['accumulated_fees_long']
                    new_totals['accumulated_fees_long'] = 0.0
            
        elif direction in short_open_directions:
            new_totals['total_open_size_short'] += size
            # Накапливаем комиссии при открытии Short позиций
            new_totals['accumulated_fees_short'] += total_trade_fee
            
        elif direction in short_close_directions:
            new_totals['total_close_size_short'] += size
            # При закрытии Short позиции списываем пропорциональную часть накопленных комиссий
            current_remaining = new_totals['total_open_size_short'] - current_totals['total_close_size_short']
            if current_remaining > 0 and new_totals['accumulated_fees_short'] > 0:
                proportion = size / current_remaining
                proportion = min(proportion, 1.0)
                realized_fees = new_totals['accumulated_fees_short'] * proportion
                new_totals['accumulated_fees_short'] -= realized_fees
                
                # Проверка: если позиция закрыта полностью, accumulated_fees_short должно быть равно нулю
                remaining_after_close = new_totals['total_open_size_short'] - new_totals['total_close_size_short']
                if abs(remaining_after_close) < 1e-10 and abs(new_totals['accumulated_fees_short']) > 1e-6:
                    debug_logger.warning(f"!!! FEES WARNING: Short position fully closed but accumulated_fees_short is not zero: {new_totals['accumulated_fees_short']:.6f}")
                    new_totals['fees_warning_short'] = True
                    realized_fees += new_totals['accumulated_fees_short']
                    new_totals['accumulated_fees_short'] = 0.0
        
        # Обработка комбинированных направлений (новое) - исправлена проблема двойного учета комиссий
        elif direction in combined_directions:
            if direction == 'Close Short + Open Long':
                # Рассчитываем размеры для закрытия и открытия
                close_size = abs(current_totals['total_open_size_short'] - current_totals['total_close_size_short'])
                open_size = size - close_size
                
                # Разделяем комиссии пропорционально
                close_ratio = close_size / size if size > 0 else 0
                close_fee = total_trade_fee * close_ratio
                open_fee = total_trade_fee - close_fee
                
                # Закрываем short позицию
                new_totals['total_close_size_short'] += close_size
                if new_totals['accumulated_fees_short'] > 0:
                    realized_fees = new_totals['accumulated_fees_short']
                    new_totals['accumulated_fees_short'] = 0.0
                
                # Открываем long позицию
                if open_size > 0:
                    new_totals['total_open_size_long'] += open_size
                    new_totals['accumulated_fees_long'] += open_fee
                
                # Для Net PnL используем только комиссию от закрытия
                new_totals['close_fee_only'] = close_fee
                
            elif direction == 'Close Long + Open Short':
                # Рассчитываем размеры для закрытия и открытия
                close_size = abs(current_totals['total_open_size_long'] - current_totals['total_close_size_long'])
                open_size = size - close_size
                
                # Разделяем комиссии пропорционально
                close_ratio = close_size / size if size > 0 else 0
                close_fee = total_trade_fee * close_ratio
                open_fee = total_trade_fee - close_fee
                
                # Закрываем long позицию
                new_totals['total_close_size_long'] += close_size
                if new_totals['accumulated_fees_long'] > 0:
                    realized_fees = new_totals['accumulated_fees_long']
                    new_totals['accumulated_fees_long'] = 0.0
                
                # Открываем short позицию
                if open_size > 0:
                    new_totals['total_open_size_short'] += open_size
                    new_totals['accumulated_fees_short'] += open_fee
                
                # Для Net PnL используем только комиссию от закрытия
                new_totals['close_fee_only'] = close_fee
    
    # Рассчитываем remaining
    new_totals['remaining_open_size_long'] = new_totals['total_open_size_long'] - new_totals['total_close_size_long']
    new_totals['remaining_open_size_short'] = new_totals['total_open_size_short'] - new_totals['total_close_size_short']
    
    # ИСПРАВЛЕНИЕ: Избавляемся от отрицательного нуля (-0.0000)
    tolerance = 1e-10
    if abs(new_totals['remaining_open_size_long']) < tolerance:
        new_totals['remaining_open_size_long'] = 0.0
    if abs(new_totals['remaining_open_size_short']) < tolerance:
        new_totals['remaining_open_size_short'] = 0.0
    if abs(new_totals['accumulated_fees_long']) < tolerance:
        new_totals['accumulated_fees_long'] = 0.0
    if abs(new_totals['accumulated_fees_short']) < tolerance:
        new_totals['accumulated_fees_short'] = 0.0
    
    new_totals['realized_fees'] = realized_fees
    
    return new_totals

# НОВАЯ ФУНКЦИЯ: Расчет Net PnL с учетом всех комиссий
def calculate_net_pnl(closed_pnl, exchange_fee, bot_fee, realized_fees, direction, close_fee_only=None):
    """Рассчитывает чистый PnL с учетом всех комиссий
    
    Для операций открытия и увеличения позиций (Open/Increase) возвращает 0.0,
    независимо от комиссий, так как прибыль/убыток реализуется только при закрытии.
    """
    # Для операций открытия и увеличения позиций Net PnL = 0.0
    if direction and ('Open' in direction or 'Increase' in direction):
        return 0.0
    
    # Для операций закрытия и уменьшения рассчитываем как раньше
    if close_fee_only is not None:
        # Для комбинированных направлений используем только комиссию от закрытия
        total_fees = safe_float(close_fee_only, 0.0) + safe_float(realized_fees, 0.0)
    else:
        # Для обычных направлений используем все комиссии
        total_fees = safe_float(exchange_fee, 0.0) + safe_float(bot_fee, 0.0) + safe_float(realized_fees, 0.0)
    net_pnl = safe_float(closed_pnl, 0.0) - total_fees
    return net_pnl

# НОВАЯ ФУНКЦИЯ: Проверка и вывод предупреждения об отрицательном Net PnL
def check_negative_pnl(net_pnl, coin, oid, timestamp, wallet):
    """Проверяет Net PnL на отрицательность и выводит предупреждение"""
    # Добавляем толерантность чтобы избежать предупреждений о микроскопических отрицательных значениях
    if net_pnl < -1e-8:
        # Проверяем, не отправляли ли уже уведомление для этой сделки
        cursor.execute(f'''
            SELECT COUNT(*) FROM sent_trades_{wallet} WHERE oid = ?
        ''', (oid,))
        
        if cursor.fetchone()[0] > 0:
            return False  # Уведомление уже отправлено
        
        negative_pnl_msg = (f"⚠️ NEGATIVE NET PnL WARNING ⚠️\n"
                          f"Wallet: {wallet}\n"
                          f"Coin: {coin}\n"
                          f"OID: {oid}\n"
                          f"Net PnL: {net_pnl:.4f}\n"
                          f"Time: {format_timestamp(timestamp)}\n"
                          f"{'='*50}")
        
        print(negative_pnl_msg)
        debug_logger.warning(negative_pnl_msg.replace('\n', ' | '))
        
        # ВРЕМЕННО ОТКЛЮЧЕНО: Отправляем уведомление в Telegram
        try:
            # send_tg_message(negative_pnl_msg)  # ОТКЛЮЧЕНО НА ВРЕМЯ ОТЛАДКИ
            print("📵 Telegram notification disabled for debugging")
            # Помечаем как отправленное и сохраняем в БД
            cursor.execute(f'''
                INSERT OR IGNORE INTO sent_trades_{wallet} (oid) VALUES (?)
            ''', (oid,))
            conn.commit()  # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: сохраняем изменения
        except Exception as e:
            debug_logger.error(f"Ошибка сохранения sent_trades в БД: {e}")
        
        return True
    return False

# Функция для форматирования и вывода информации о сделке
def format_trade_info(wallet, trade_data):
    """Форматирует информацию о сделке для вывода"""
    bot_fee = safe_float(trade_data.get('bot_fee', 0.0)) or 0.0
    exchange_fee = safe_float(trade_data.get('fee', 0.0)) or 0.0
    realized_fees = safe_float(trade_data.get('realized_fees', 0.0)) or 0.0
    current_fees = exchange_fee + bot_fee
    total_fees = current_fees + realized_fees
    closed_pnl = safe_float(trade_data.get('closedPnl', 0.0)) or 0.0
    net_pnl = safe_float(trade_data.get('net_pnl', 0.0)) or 0.0
    size = safe_float(trade_data.get('size', 0.0)) or 0.0
    price = safe_float(trade_data.get('price', 0.0)) or 0.0
    
    return (f"Wallet: {wallet} | "
            f"OID: {trade_data.get('oid', 'N/A')} | "
            f"Coin: {trade_data.get('coin', 'N/A')} | "
            f"Direction: {trade_data.get('direction', 'N/A')} | "
            f"Size: {size:.6f} | "
            f"Price: {price:.4f} | "
            f"Exchange Fee: {exchange_fee:.6f} | "
            f"Bot Fee: {bot_fee:.6f} | "
            f"Realized Fees: {realized_fees:.6f} | "
            f"Total Fees: {total_fees:.6f} | "
            f"Closed PnL: {closed_pnl:.4f} | "
            f"Net PnL: {net_pnl:.4f} | "
            f"Time: {format_timestamp(trade_data.get('timestamp', 0))}")

# Функция для сохранения сделок в текстовые файлы по монетам
def save_trades_to_files():
    """Сохраняет сделки в текстовые файлы, сгруппированные по монетам, в порядке ID"""
    for wallet in WALLETS:
        # Получаем все сделки для кошелька, отсортированные по ID
        cursor.execute(f'''
            SELECT * FROM trades_{wallet} ORDER BY id ASC
        ''')
        
        trades = cursor.fetchall()
        if not trades:
            continue
            
        # Получаем названия колонок
        cursor.execute(f"PRAGMA table_info(trades_{wallet})")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Группируем по монетам
        coins_data = {}
        for trade in trades:
            trade_dict = dict(zip(columns, trade))
            coin = trade_dict['coin']
            if coin not in coins_data:
                coins_data[coin] = []
            coins_data[coin].append(trade_dict)
        
        # Сохраняем файл для каждой монеты
        for coin, coin_trades in coins_data.items():
            filename = f"trade_sequence_{coin}_{wallet}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Trade sequence for {coin} - Wallet: {wallet}\n")
                f.write("=" * 200 + "\n")
                
                # Заголовки таблицы с полными названиями
                header = (f"{'ID':<3} | {'OID':<12} | {'Direction':<18} | {'Size':<8} | {'Price':<8} | "
                         f"{'Exchange Fee':<11} | {'Bot Fee':<8} | {'Realized Fee':<12} | {'Total Fee':<9} | "
                         f"{'Closed PnL':<10} | {'Net PnL':<8} | {'Open Long':<9} | {'Close Long':<10} | {'Remain Long':<11} | "
                         f"{'Accum Fee Long':<13} | {'Open Short':<10} | {'Close Short':<11} | {'Remain Short':<12} | {'Accum Fee Short':<14} | "
                         f"{'Time':<19} | {'Warnings'}")
                f.write(header + "\n")
                f.write("-" * 200 + "\n")
                
                for trade in coin_trades:
                    # Рассчитываем комиссии для каждой сделки в файле
                    size = safe_float(trade.get('size', 0.0)) or 0.0
                    price = safe_float(trade.get('price', 0.0)) or 0.0
                    bot_fee_bps = config.get('bot_fee_bps', 5)
                    bot_fee = calculate_bot_fee(size, price, bot_fee_bps)
                    exchange_fee = safe_float(trade.get('fee', 0.0)) or 0.0
                    realized_fees = safe_float(trade.get('realized_fees', 0.0)) or 0.0
                    current_fees = exchange_fee + bot_fee
                    
                    # Безопасное извлечение всех накопленных значений
                    open_long = safe_float(trade.get('total_open_size_long', 0.0)) or 0.0
                    close_long = safe_float(trade.get('total_close_size_long', 0.0)) or 0.0
                    remain_long = safe_float(trade.get('remaining_open_size_long', 0.0)) or 0.0
                    accum_fee_long = safe_float(trade.get('accumulated_fees_long', 0.0)) or 0.0
                    open_short = safe_float(trade.get('total_open_size_short', 0.0)) or 0.0
                    close_short = safe_float(trade.get('total_close_size_short', 0.0)) or 0.0
                    remain_short = safe_float(trade.get('remaining_open_size_short', 0.0)) or 0.0
                    accum_fee_short = safe_float(trade.get('accumulated_fees_short', 0.0)) or 0.0
                    closed_pnl = safe_float(trade.get('closedPnl', 0.0)) or 0.0
                    net_pnl = safe_float(trade.get('net_pnl', 0.0)) or 0.0
                    total_fees = current_fees + realized_fees
                    
                    # Определяем направления закрытия/уменьшения позиций  
                    closing_directions = [
                        'Close Long', 'Close Short', 'Decrease Long', 'Decrease Short',
                        'Close Long + Open Short', 'Close Short + Open Long'
                    ]
                    
                    # Проверяем нужно ли добавить предупреждение
                    warning_text = ""
                    if net_pnl is not None and net_pnl < 0 and trade.get('direction') in closing_directions:
                        warning_text = f"⚠️ Negative Net PnL ({net_pnl:.4f})"
                    
                    # Формируем строку таблицы с правильным выравниванием под полные заголовки
                    line = (f"{trade.get('id', 0):<3} | {trade.get('oid', 'N/A'):<12} | {trade.get('direction', 'N/A'):<18} | "
                           f"{size:<8.4f} | {price:<8.4f} | "
                           f"{exchange_fee:<11.6f} | {bot_fee:<8.6f} | {realized_fees:<12.6f} | {total_fees:<9.6f} | "
                           f"{closed_pnl:<10.4f} | {net_pnl:<8.4f} | "
                           f"{open_long:<9.4f} | {close_long:<10.4f} | {remain_long:<11.4f} | {accum_fee_long:<13.6f} | "
                           f"{open_short:<10.4f} | {close_short:<11.4f} | {remain_short:<12.4f} | {accum_fee_short:<14.6f} | "
                           f"{format_timestamp(trade.get('timestamp', 0))} | {warning_text}")
                    
                    f.write(line.rstrip() + "\n")
                    
                    # Проверка на отрицательные значения только для значительных отклонений
                    tolerance = 1e-6
                    remaining_long = safe_float(trade.get('remaining_open_size_long', 0.0)) or 0.0
                    remaining_short = safe_float(trade.get('remaining_open_size_short', 0.0)) or 0.0
                    
                    if remaining_long < -tolerance:
                        f.write(f"!!! WARNING: Remaining Open Size became negative ({remaining_long:.4f}) "
                               f"at ID {trade.get('id', 0)}, OID {trade.get('oid', 'N/A')}, "
                               f"Timestamp {format_timestamp(trade.get('timestamp', 0))}\n")
                    if remaining_short < -tolerance:
                        f.write(f"!!! WARNING: Remaining Short Size became negative ({remaining_short:.4f}) "
                               f"at ID {trade.get('id', 0)}, OID {trade.get('oid', 'N/A')}, "
                               f"Timestamp {format_timestamp(trade.get('timestamp', 0))}\n")
                    
                    # Проверка накопленных комиссий при полном закрытии позиций
                    if abs(remaining_long) < 1e-10 and abs(safe_float(trade.get('accumulated_fees_long', 0.0))) > 1e-6:
                        f.write(f"!!! FEES WARNING: Long position fully closed but accumulated_fees_long is not zero: "
                               f"{safe_float(trade.get('accumulated_fees_long', 0.0)):.6f} "
                               f"at ID {trade['id']}, OID {trade['oid']}, "
                               f"Timestamp {format_timestamp(trade['timestamp'])}\n")
                    
                    if abs(remaining_short) < 1e-10 and abs(safe_float(trade.get('accumulated_fees_short', 0.0))) > 1e-6:
                        f.write(f"!!! FEES WARNING: Short position fully closed but accumulated_fees_short is not zero: "
                               f"{safe_float(trade.get('accumulated_fees_short', 0.0)):.6f} "
                               f"at ID {trade['id']}, OID {trade['oid']}, "
                               f"Timestamp {format_timestamp(trade['timestamp'])}\n")
                    
                    f.write("\n")
            
            print(f"Сохранен файл: {filename} ({len(coin_trades)} сделок)")

# Основная функция обработки новых сделок
def process_new_trades():
    """Обрабатывает новые сделки для всех кошельков"""
    
    for wallet in WALLETS:
        try:
            print(f"\n=== Обработка кошелька {wallet} ===")
            debug_logger.info(f"Начинаем обработку кошелька {wallet}")
            
            # Получаем данные о сделках из API
            user_fills = info.user_fills(wallet)
            
            if not user_fills:
                print(f"Нет данных о сделках для кошелька {wallet}")
                continue
            
            # ВАЖНО: API возвращает сделки от новых к старым
            # Сортируем правильно: сначала по timestamp, потом по OID для одинаковых timestamp
            user_fills_sorted = sorted(user_fills, key=lambda x: (x.get('time', 0), int(x.get('oid', 0))))
            
            print(f"Получено {len(user_fills)} сделок из API, обрабатываем от старых к новым (сортировка по timestamp+OID)")
            debug_logger.info(f"Получено {len(user_fills)} сделок из API, обрабатываем от старых к новым (сортировка по timestamp+OID)")
                
            new_trades_count = 0
            
            # Обрабатываем сделки от старых к новым (правильно отсортированные)
            for fill in user_fills_sorted:
                try:
                    # Извлекаем данные сделки
                    oid = str(fill.get('oid', ''))
                    coin = fill.get('coin', '')
                    timestamp = int(fill.get('time', 0))
                    price = safe_float(fill.get('px', 0.0))
                    size = safe_float(fill.get('sz', 0.0))
                    side = fill.get('side', '')
                    closed_pnl = safe_float(fill.get('closedPnl', 0.0))
                    fee = safe_float(fill.get('fee', 0.0))
                    start_position = safe_float(fill.get('startPosition', 0.0))
                    
                    # Рассчитываем комиссию бота с конфигурируемой ставкой
                    bot_fee_bps = config.get('bot_fee_bps', 5)  # По умолчанию 5 bps
                    bot_fee = calculate_bot_fee(size, price, bot_fee_bps)
                    
                    # Определяем направление сделки
                    direction = get_direction(fill, wallet)
                    
                    # Проверяем, есть ли уже эта сделка в базе
                    cursor.execute(f'''
                        SELECT COUNT(*) FROM trades_{wallet} 
                        WHERE oid = ? AND coin = ? AND timestamp = ? AND size = ? AND startPosition = ?
                    ''', (oid, coin, timestamp, size, start_position))
                    
                    if cursor.fetchone()[0] > 0:
                        continue  # Сделка уже существует
                    
                    # Получаем текущие накопленные значения для данной монеты
                    current_totals = get_current_totals(wallet, coin, cursor)
                    
                    # Рассчитываем новые накопленные значения (с комиссиями)
                    new_totals = calculate_new_totals(current_totals, direction, size, fee, bot_fee)
                    
                    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Рассчитываем чистый PnL с учетом всех комиссий
                    close_fee_only = new_totals.get('close_fee_only', None)
                    net_pnl = calculate_net_pnl(closed_pnl, fee, bot_fee, new_totals['realized_fees'], direction, close_fee_only)
                    
                    # Сохраняем сделку в базу данных с рассчитанными totals и Net PnL
                    cursor.execute(f'''
                        INSERT INTO trades_{wallet} (
                            timestamp, oid, coin, price, size, side, direction, 
                            closedPnl, fee, startPosition,
                            total_open_size_long, total_close_size_long, remaining_open_size_long,
                            total_open_size_short, total_close_size_short, remaining_open_size_short,
                            accumulated_fees_long, accumulated_fees_short, realized_fees, net_pnl
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, oid, coin, price, size, side, direction,
                        closed_pnl, fee, start_position,
                        new_totals['total_open_size_long'], new_totals['total_close_size_long'], new_totals['remaining_open_size_long'],
                        new_totals['total_open_size_short'], new_totals['total_close_size_short'], new_totals['remaining_open_size_short'],
                        new_totals['accumulated_fees_long'], new_totals['accumulated_fees_short'], new_totals['realized_fees'], net_pnl
                    ))
                    
                    # Выводим информацию о сделке в консоль с комиссиями и Net PnL
                    trade_data = {
                        'oid': oid,
                        'coin': coin,
                        'timestamp': timestamp,
                        'price': price,
                        'size': size,
                        'side': side,
                        'direction': direction,
                        'closedPnl': closed_pnl,
                        'fee': fee,
                        'bot_fee': bot_fee,
                        'realized_fees': new_totals['realized_fees'],
                        'net_pnl': net_pnl,
                        'startPosition': start_position,
                        **new_totals
                    }
                    
                    trade_info = format_trade_info(wallet, trade_data)
                    print(trade_info)
                    debug_logger.info(trade_info)
                    
                    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Проверка на отрицательный Net PnL
                    # ТОЛЬКО для сделок закрытия/уменьшения позиций (где есть реальный PnL)
                    closing_directions = [
                        'Close Long', 'Close Short', 'Decrease Long', 'Decrease Short',
                        'Close Long + Open Short', 'Close Short + Open Long'
                    ]
                    
                    if direction in closing_directions:
                        check_negative_pnl(net_pnl, coin, oid, timestamp, wallet)
                    else:
                        debug_logger.debug(f"Skipping Net PnL check for opening/increasing direction: {direction} (Net PnL: {net_pnl:.4f})")
                    
                    # Проверка предупреждений о комиссиях
                    if new_totals.get('fees_warning_long', False):
                        fees_warning_msg = (f"!!! FEES WARNING: Long position fully closed but accumulated fees were not zero "
                                          f"for {coin} at OID {oid}, Timestamp {format_timestamp(timestamp)}")
                        print(fees_warning_msg)
                        debug_logger.warning(fees_warning_msg)
                    
                    if new_totals.get('fees_warning_short', False):
                        fees_warning_msg = (f"!!! FEES WARNING: Short position fully closed but accumulated fees were not zero "
                                          f"for {coin} at OID {oid}, Timestamp {format_timestamp(timestamp)}")
                        print(fees_warning_msg)
                        debug_logger.warning(fees_warning_msg)
                    
                    # Проверяем на отрицательные значения только для значительных отклонений
                    tolerance = 1e-6
                    remaining_long = safe_float(new_totals.get('remaining_open_size_long', 0.0)) or 0.0
                    remaining_short = safe_float(new_totals.get('remaining_open_size_short', 0.0)) or 0.0
                    
                    if remaining_long < -tolerance:
                        warning_msg = (f"!!! WARNING: Remaining Open Long Size became negative "
                                     f"({remaining_long:.4f}) for {coin} "
                                     f"at OID {oid}, Timestamp {format_timestamp(timestamp)}")
                        print(warning_msg)
                        debug_logger.warning(warning_msg)
                    
                    if remaining_short < -tolerance:
                        warning_msg = (f"!!! WARNING: Remaining Open Short Size became negative "
                                     f"({remaining_short:.4f}) for {coin} "
                                     f"at OID {oid}, Timestamp {format_timestamp(timestamp)}")
                        print(warning_msg)
                        debug_logger.warning(warning_msg)
                    
                    new_trades_count += 1
                    
                except Exception as e:
                    error_msg = f"Ошибка обработки сделки {fill}: {e}"
                    print(error_msg)
                    debug_logger.error(error_msg)
                    continue
            
            if new_trades_count > 0:
                print(f"Добавлено {new_trades_count} новых сделок для кошелька {wallet}")
            else:
                print(f"Новых сделок для кошелька {wallet} не найдено")
                
        except Exception as e:
            error_msg = f"Ошибка обработки кошелька {wallet}: {e}"
            print(error_msg)
            debug_logger.error(error_msg)
            with open("app_errors.log", "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")

def display_wallet_summary():
    """Показывает сводку по всем кошелькам"""
    try:
        print("\n📊 Накопленные результаты по кошелькам:")
        print("=" * 60)
        
        for wallet in WALLETS:
            try:
                # Получаем все сделки кошелька из базы данных
                cursor.execute(f"""
                    SELECT SUM(closedPnl) as total_closed_pnl, SUM(net_pnl) as total_net_pnl 
                    FROM trades_{wallet}
                """)
                
                result = cursor.fetchone()
                if result and result[0] is not None:
                    total_closed_pnl = safe_float(result[0]) or 0.0
                    total_net_pnl = safe_float(result[1]) or 0.0
                    
                    # Определяем символ и цвет в зависимости от результата
                    if total_net_pnl is not None and total_net_pnl > 0:
                        symbol = "🟢"
                        status = f"Положительный результат: +{total_net_pnl:.4f}"
                    else:
                        symbol = "🔴"
                        status = f"Отрицательный результат: {total_net_pnl:.4f}"
                    
                    print(f"💼 Кошелек: {wallet}")
                    print(f"   📈 Накопленный Closed PnL:   {total_closed_pnl:10.4f}")
                    print(f"   💰 Накопленный Net PnL:     {total_net_pnl:10.4f}")
                    print(f"   {symbol} {status}")
                else:
                    print(f"💼 Кошелек: {wallet}")
                    print("   ❌ Нет данных о сделках")
            except Exception as e:
                print(f"💼 Кошелек: {wallet}")
                print(f"   ❌ Ошибка при получении данных: {e}")
                debug_logger.error(f"Ошибка display_wallet_summary для {wallet}: {e}")
    except Exception as e:
        debug_logger.error(f"Критическая ошибка display_wallet_summary: {e}")
        print("❌ Ошибка при отображении сводки по кошелькам")

def run_incremental_monitoring():
    """Запускает инкрементальное обновление данных"""
    try:
        print("\n🔄 Проверка новых транзакций...")
        debug_logger.info("🔄 Запуск инкрементального мониторинга")
        
        total_new_trades = 0
        wallets_with_new_trades = []
        
        for wallet in WALLETS:
            new_trades_count = process_incremental_trades_for_wallet(wallet)
            debug_logger.info(f"🔍 Кошелек {wallet}: process_incremental_trades_for_wallet вернул {new_trades_count}")
            total_new_trades += new_trades_count
            debug_logger.info(f"🔢 total_new_trades теперь равен {total_new_trades}")
            
            if new_trades_count > 0:
                wallets_with_new_trades.append(wallet)
                print(f"🆕 Кошелек {wallet[:8]}...{wallet[-6:]}: {new_trades_count} новых сделок")
        
        debug_logger.info(f"🎯 Итоговый total_new_trades: {total_new_trades}")
        debug_logger.info(f"🎯 Условие total_new_trades > 0: {total_new_trades > 0}")
        
        if total_new_trades > 0:
            print(f"\n📊 Всего найдено {total_new_trades} новых сделок")
            debug_logger.info(f"📊 Всего найдено {total_new_trades} новых сделок")
            
            # Регенерируем отчеты и графики только если есть новые сделки
            print("🔄 Обновление отчетов и графиков...")
            debug_logger.info("🔄 Обновление отчетов и графиков...")
            save_trades_to_files()
            create_cumulative_pnl_charts()
            create_per_wallet_pnl_charts()
            print("✅ Отчеты обновлены")
            debug_logger.info("✅ Отчеты обновлены")
            
            # Выводим краткую сводку по кошелькам
            display_wallet_summary()
            
            # Отправляем сводку об обновлении в Telegram
            send_incremental_summary(total_new_trades, wallets_with_new_trades)
            
        else:
            print("✅ Новых сделок не найдено")
        
        debug_logger.info(f"✅ Инкрементальное обновление завершено. Обработано сделок: {total_new_trades}")
        return total_new_trades
        
    except Exception as e:
        error_msg = f"❌ Ошибка при инкрементальном мониторинге: {e}"
        print(error_msg)
        debug_logger.error(error_msg)
        return 0

def continuous_monitoring():
    """Запускает непрерывный мониторинг каждые 5 минут"""
    
    # Обработчик сигналов для корректного завершения
    def signal_handler(sig, frame):
        print(f"\n🛑 Получен сигнал {sig}. Завершение мониторинга...")
        debug_logger.info(f"🛑 Получен сигнал {sig}. Завершение мониторинга...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🚀 Запуск непрерывного мониторинга каждые 5 минут...")
    print("📊 Первоначальный анализ данных...")
    debug_logger.info("🚀 Запуск непрерывного мониторинга каждые 5 минут")
    
    # Выполняем первоначальную полную синхронизацию
    try:
        print("🚀 Запуск мониторинга торговых сделок Hyperliquid с расчетом Net PnL")
        print("📊 Формула Net PnL: Closed PnL - (Exchange Fee + Bot Fee + Realized Fees)")
        print("⚠️  При отрицательном Net PnL будут отправлены уведомления в Telegram\n")
        
        # Обрабатываем новые сделки с расчетом totals и Net PnL на лету
        process_new_trades()
        
        # Сохраняем изменения в базе данных
        conn.commit()
        
        # Генерируем текстовые файлы по монетам с Net PnL информацией
        save_trades_to_files()
        
        # Создаем интерактивные графики кумулятивного PnL
        create_cumulative_pnl_charts()
        
        # Создаем графики с детализацией по кошелькам
        create_per_wallet_pnl_charts()
        
        print("\n✅ Обработка завершена успешно")
        print("📁 Проверьте файлы trade_sequence_[COIN]_[WALLET].txt для детальной информации")
        
        # Показываем накопленные PnL по каждому кошельку
        display_wallet_summary()
        
    except Exception as e:
        print(f"❌ Ошибка при первоначальной синхронизации: {e}")
        debug_logger.error(f"❌ Ошибка при первоначальной синхронизации: {e}")
    
    print("\n⏰ Переход в режим мониторинга каждые 5 минут...")
    print("💡 Для остановки используйте Ctrl+C")
    
    # Отправляем уведомление о запуске мониторинга
    send_monitoring_start_notification()
    
    # Отправляем начальный отчет по открытым позициям
    print("\n📊 Отправка начального отчета по открытым позициям...")
    send_hourly_positions_report()
    
    # Отправляем суточный отчет NET PnL за 24 часа
    print("\n📅 Отправка суточного отчета NET PnL за 24 часа...")
    send_startup_daily_summary()
    
    # Основной цикл мониторинга
    cycle_count = 0
    while True:
        try:
            cycle_count += 1
            next_check_time = datetime.now().strftime('%H:%M:%S')
            
            # Спим 5 минут (300 секунд)
            time.sleep(300)
            
            print(f"\n🕐 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Проверка #{cycle_count}")
            
            # Особая отладка для проблемного кошелька каждые 3 проверки
            if cycle_count % 3 == 1:
                debug_wallet_data('0xe04e82b16a9a418d50d8f9c358987d2ee735bded')
            
            # Часовой отчет по открытым позициям (каждые 12 циклов = 60 минут)
            if cycle_count % 12 == 0:
                print(f"\n⏰ Час прошел - отправка отчета по открытым позициям...")
                send_hourly_positions_report()
            
            run_incremental_monitoring()
            
        except KeyboardInterrupt:
            print(f"\n🛑 Мониторинг остановлен пользователем")
            debug_logger.info("🛑 Мониторинг остановлен пользователем")
            break
        except Exception as e:
            error_msg = f"❌ Ошибка в цикле мониторинга: {e}"
            print(error_msg)
            debug_logger.error(error_msg)
            print("🔄 Продолжение мониторинга через 5 минут...")
            time.sleep(300)
    
    print("🏁 Мониторинг завершен")
    debug_logger.info("🏁 Мониторинг завершен")

# Основной цикл выполнения
if __name__ == "__main__":
    try:
        # Запускаем непрерывный мониторинг
        continuous_monitoring()
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {e}"
        debug_logger.error(error_msg)
        print(error_msg)
        with open("app_errors.log", "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")
    finally:
        # Закрываем соединение с базой данных
        conn.close()

def main_analysis():
    """Выполняет основной анализ данных"""
    print("🚀 Запуск мониторинга торговых сделок Hyperliquid с расчетом Net PnL")
    print("📊 Формула Net PnL: Closed PnL - (Exchange Fee + Bot Fee + Realized Fees)")
    print("⚠️  При отрицательном Net PnL будут отправлены уведомления в Telegram\n")
    
    # Обрабатываем новые сделки с расчетом totals и Net PnL на лету
    process_new_trades()
    
    # Сохраняем изменения в базе данных
    conn.commit()
    
    # Генерируем текстовые файлы по монетам с Net PnL информацией
    save_trades_to_files()
    
    # Создаем интерактивные графики кумулятивного PnL
    create_cumulative_pnl_charts()
    
    # Создаем графики с детализацией по кошелькам
    create_per_wallet_pnl_charts()
    
    print("\n✅ Обработка завершена успешно")
    print("📁 Проверьте файлы trade_sequence_[COIN]_[WALLET].txt для детальной информации")
    
    # Показываем накопленные PnL по каждому кошельку
    display_wallet_summary()

if __name__ == "__main__":
    try:
        # Запускаем непрерывный мониторинг
        continuous_monitoring()
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {e}"
        debug_logger.error(error_msg)
        print(error_msg)
        with open("app_errors.log", "a") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")
    finally:
        # Закрываем соединение с базой данных
        conn.close()