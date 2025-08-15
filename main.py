import discord
import re
import gspread
import requests
import os
import json
import logging
import asyncio
import random
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
from isbnlib import to_isbn10, to_isbn13, canonical, is_isbn10, is_isbn13
from flask import Flask, request
import threading
import time

# ログ設定
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Flask app for health check
app = Flask(__name__)

# ヘルスチェック用の変数
health_status = {
    'status': 'running',
    'last_check': datetime.now().isoformat(),
    'bot_connected': False,
    'connection_attempts': 0,
    'last_error': None,
    'startup_attempts': 0,  # 新規追加：起動試行回数
    'first_startup_time': datetime.now().isoformat()  # 新規追加：初回起動時刻
}

# 起動制限の設定
MAX_STARTUP_ATTEMPTS = 10  # 最大起動試行回数
STARTUP_WINDOW_HOURS = 24   # 24時間以内での制限

@app.route('/')
def hello():
    return "Discord Bot is running!"

@app.route('/health')
def health():
    health_status['last_check'] = datetime.now().isoformat()
    return json.dumps(health_status), 200, {'Content-Type': 'application/json'}

@app.route('/ping')
def ping():
    return "pong"

@app.route('/status')
def status():
    user_agent = request.headers.get('User-Agent', 'Unknown')
    logger.info(f"Status check from: {user_agent}")
    
    if 'monitor' in user_agent.lower() or 'uptimerobot' in user_agent.lower():
        return "OK"
    
    return {
        'status': 'active',
        'timestamp': datetime.now().isoformat(),
        'bot_status': health_status['bot_connected'],
        'connection_attempts': health_status['connection_attempts'],
        'startup_attempts': health_status['startup_attempts'],  # 新規追加
        'max_startup_attempts': MAX_STARTUP_ATTEMPTS  # 新規追加
    }

@app.route('/robots.txt')
def robots():
    return """User-agent: *
Allow: /
Allow: /health
Allow: /ping
Allow: /status"""

def run_web():
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

# 環境変数からトークンを取得
try:
    TOKEN = os.environ.get('DISCORD_TOKEN')
    API_KEY = os.environ.get('GOOGLE_BOOKS_API_KEY')
    
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN environment variable is not set")
    
    logger.info("Environment variables loaded successfully")
except Exception as e:
    logger.error(f"Failed to load environment variables: {e}")
    raise

# Google Sheetsの設定
try:
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    google_creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if not google_creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is not set")

    creds_dict = json.loads(google_creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable is not set")
    
    sheet = client.open_by_key(SHEET_ID).sheet1
    logger.info("Google Sheets connection established successfully")
    
except Exception as e:
    logger.error(f"Failed to setup Google Sheets: {e}")
    raise

# ISBNの正規表現パターン
ISBN_PATTERN = r"\b(?:(?:[\d][\d\s-]*){9}[\dXx]|(?:[\d][\d\s-]*){12}\d)\b"

# Google Books APIの設定
GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"

# Discordクライアント設定
intents = discord.Intents.default()
intents.message_content = True
client_discord = discord.Client(intents=intents)

# 起動回数チェック関数
def check_startup_limits():
    """起動回数の制限をチェック"""
    try:
        # ファイルから前回の起動データを読み込み
        if os.path.exists('/tmp/startup_log.json'):
            with open('/tmp/startup_log.json', 'r') as f:
                startup_data = json.load(f)
            
            first_startup = datetime.fromisoformat(startup_data.get('first_startup_time'))
            startup_attempts = startup_data.get('startup_attempts', 0)
            
            # 24時間以内かチェック
            if datetime.now() - first_startup < timedelta(hours=STARTUP_WINDOW_HOURS):
                if startup_attempts >= MAX_STARTUP_ATTEMPTS:
                    logger.error(f"Maximum startup attempts ({MAX_STARTUP_ATTEMPTS}) reached within {STARTUP_WINDOW_HOURS} hours")
                    logger.error("Bot will sleep for 24 hours to avoid rate limiting")
                    return False, startup_attempts + 1
                else:
                    return True, startup_attempts + 1
            else:
                # 24時間経過したのでリセット
                logger.info("Startup attempt counter reset (24 hours passed)")
                return True, 1
        else:
            # 初回起動
            return True, 1
            
    except Exception as e:
        logger.error(f"Error checking startup limits: {e}")
        return True, 1

def save_startup_data(attempts):
    """起動データをファイルに保存"""
    try:
        startup_data = {
            'first_startup_time': health_status['first_startup_time'],
            'startup_attempts': attempts,
            'last_startup': datetime.now().isoformat()
        }
        
        with open('/tmp/startup_log.json', 'w') as f:
            json.dump(startup_data, f)
            
    except Exception as e:
        logger.error(f"Error saving startup data: {e}")

# Rate Limit対応の改善された接続関数
async def safe_connect_with_backoff():
    """指数関数的バックオフを使った安全な接続"""
    max_retries = 5
    base_delay = 60  # 基本待機時間を60秒に延長
    
    for attempt in range(max_retries):
        try:
            health_status['connection_attempts'] += 1
            logger.info(f"Discord connection attempt {attempt + 1}/{max_retries}")
            
            # ランダムな遅延を追加してスパイクを避ける
            random_delay = random.uniform(5, 15)
            await asyncio.sleep(random_delay)
            
            await client_discord.start(TOKEN)
            return True
            
        except discord.errors.HTTPException as e:
            if e.status == 429 or "rate limited" in str(e).lower():
                wait_time = base_delay * (2 ** attempt) + random.uniform(0, 30)
                logger.warning(f"Rate limited on attempt {attempt + 1}. Waiting {wait_time:.1f} seconds...")
                health_status['last_error'] = f"Rate limited: {str(e)}"
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"HTTP Exception on attempt {attempt + 1}: {e}")
                health_status['last_error'] = f"HTTP Error: {str(e)}"
                await asyncio.sleep(base_delay)
        except Exception as e:
            logger.error(f"Connection error on attempt {attempt + 1}: {e}")
            health_status['last_error'] = f"Connection Error: {str(e)}"
            await asyncio.sleep(base_delay * (attempt + 1))
    
    logger.error(f"Failed to connect after {max_retries} attempts")
    return False

# Rate Limit対応の安全な返信機能
async def safe_reply(message, content, max_retries=3):
    for attempt in range(max_retries):
        try:
            await message.reply(content)
            logger.info(f"Message sent successfully on attempt {attempt + 1}")
            return True
        except discord.errors.HTTPException as e:
            if e.status == 429:
                retry_after = getattr(e, 'retry_after', 2 ** attempt)
                logger.warning(f"Rate limited. Waiting {retry_after} seconds before retry {attempt + 1}/{max_retries}")
                await asyncio.sleep(retry_after)
            else:
                logger.error(f"HTTP Exception: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error in safe_reply: {e}")
            return False
    
    logger.error(f"Failed to send message after {max_retries} attempts")
    return False

# ISBNが有効かどうかをチェックする関数
def is_valid_isbn(isbn):
    try:
        clean_isbn = canonical(isbn)
        
        if is_isbn10(clean_isbn) or is_isbn13(clean_isbn):
            return True
        
        if len(clean_isbn) == 10:
            try:
                base_isbn = "978" + clean_isbn[:-1]
                
                sum_odd = 0
                sum_even = 0
                for i, digit in enumerate(base_isbn):
                    if i % 2 == 0:
                        sum_odd += int(digit)
                    else:
                        sum_even += int(digit) * 3
                
                total_sum = sum_odd + sum_even
                check_digit = (10 - (total_sum % 10)) % 10
                
                isbn13 = base_isbn + str(check_digit)
                
                if is_isbn13(isbn13):
                    logger.info(f"978変換で有効なISBN-13に変換: {isbn13}")
                    return True
            except Exception as e:
                logger.error(f"ISBN変換エラー: {e}")
        
        return False
    except Exception as e:
        logger.error(f"ISBN validation error: {e}")
        return False

# ISBNの変換を安全に行う関数
def safe_isbn_conversion(isbn):
    try:
        clean_isbn = canonical(isbn)
        
        result = {
            "original": isbn,
            "clean": clean_isbn,
            "isbn_10": None,
            "isbn_13": None
        }
        
        try:
            if len(clean_isbn) == 13:
                result["isbn_10"] = to_isbn10(clean_isbn)
            elif len(clean_isbn) == 10:
                result["isbn_10"] = clean_isbn
        except Exception as e:
            logger.error(f"ISBN-10変換エラー: {e}")
        
        try:
            if len(clean_isbn) == 10:
                result["isbn_13"] = to_isbn13(clean_isbn)
            elif len(clean_isbn) == 13:
                result["isbn_13"] = clean_isbn
        except Exception as e:
            logger.error(f"ISBN-13変換エラー: {e}")
        
        return result
    except Exception as e:
        logger.error(f"Safe ISBN conversion error: {e}")
        return None

# ヘルスステータス更新用のバックグラウンドタスク
async def update_health_status():
    while True:
        try:
            health_status['last_check'] = datetime.now().isoformat()
            health_status['bot_connected'] = client_discord.is_ready()
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Health status update error: {e}")
            await asyncio.sleep(60)

# Botが起動した際の処理
@client_discord.event
async def on_ready():
    logger.info(f'Successfully logged in as {client_discord.user}')
    health_status['bot_connected'] = True
    health_status['last_error'] = None
    asyncio.create_task(update_health_status())

# Botが切断された際の処理
@client_discord.event
async def on_disconnect():
    logger.warning('Bot disconnected')
    health_status['bot_connected'] = False

# エラーハンドリング
@client_discord.event
async def on_error(event, *args, **kwargs):
    logger.error(f'Discord event error: {event}', exc_info=True)

# メッセージを受け取ったときの処理
@client_discord.event
async def on_message(message):
    if message.author == client_discord.user or message.author.bot:
        return

    try:
        match = re.search(ISBN_PATTERN, message.content)
        if match:
            isbn_with_separators = match.group(0)
            isbn = re.sub(r'[\s-]', '', isbn_with_separators)
            logger.info(f"ISBN Found: {isbn} (元の形式: {isbn_with_separators})")

            if not is_valid_isbn(isbn):
                await safe_reply(message, "申し訳ありません。有効なISBN形式ではないようです。正しいISBNを入力してください。")
                return

            isbn_data = safe_isbn_conversion(isbn)
            if not isbn_data or (not isbn_data["isbn_10"] and not isbn_data["isbn_13"]):
                await safe_reply(message, "ISBN変換に失敗しました。正確なISBNを入力してください。")
                return
                
            isbn_10 = isbn_data["isbn_10"]
            isbn_13 = isbn_data["isbn_13"]
            logger.info(f"ISBN-10: {isbn_10}, ISBN-13: {isbn_13}")
            
            try:
                openbd_url = f'https://api.openbd.jp/v1/get?isbn={isbn_13}'
                hanmoto_url = f'https://www.hanmoto.com/bd/isbn/{isbn_13}'
                
                response = requests.get(openbd_url, timeout=10)
                response.raise_for_status()
                book_info = response.json()
                current_date = datetime.now().strftime("%Y/%m/%d")

                if book_info and book_info[0] is not None:
                    summary = book_info[0]['summary']
                    title = summary.get('title', 'タイトル不明')
                    publisher = summary.get('publisher', '出版社不明')

                    # 価格情報の取得
                    price = '定価不明'
                    try:
                        if 'onix' in book_info[0]:
                            onix = book_info[0]['onix']
                            if 'ProductSupply' in onix:
                                product_supply = onix['ProductSupply']
                                if 'SupplyDetail' in product_supply:
                                    supply_detail = product_supply['SupplyDetail']
                                    if 'Price' in supply_detail:
                                        price_list = supply_detail['Price']
                                        if isinstance(price_list, list) and len(price_list) > 0:
                                            price_info = price_list[0]
                                            if 'PriceAmount' in price_info:
                                                price = f"{price_info['PriceAmount']}円"
                    except Exception as e:
                        logger.error(f"Price extraction error: {e}")

                    # スプレッドシートへの情報書き込み
                    try:
                        new_row = [str(current_date), str(isbn_10), str(isbn_13), title, str(price), publisher, 2, '注文待ち', str(message.author.id), str(hanmoto_url)]
                        sheet.append_row(new_row)
                        logger.info(f"Data added to Google Sheet: {new_row}")

                        reply_message = f"ありがとうございます！\n『{title}』を2冊発注依頼しました！\n{hanmoto_url}"
                        await safe_reply(message, reply_message)
                    except Exception as e:
                        logger.error(f"Google Sheets write error: {e}")
                        await safe_reply(message, "書籍情報の保存に失敗しました。しばらく時間をおいて再度お試しください。")
                else:
                    # スプレッドシートへの情報書き込み
                    try:
                        new_row = [str(current_date), str(isbn_10), str(isbn_13), "", "", "", 2, '注文待ち', str(message.author.id), str(hanmoto_url)]
                        sheet.append_row(new_row)
                        logger.info(f"Data added to Google Sheet (no book info): {new_row}")
                        
                        error_message = f"ありがとうございます！\n注文された書籍を２冊発注依頼しました！（まだ知識が浅くて、書籍タイトルを持ってこれませんでした。ごめんなさい！）\nこちらの本を発注しています！\n{hanmoto_url}"
                        await safe_reply(message, error_message)
                    except Exception as e:
                        logger.error(f"Google Sheets write error (no book info): {e}")
                        await safe_reply(message, "注文処理に失敗しました。しばらく時間をおいて再度お試しください。")
                        
            except requests.exceptions.Timeout:
                logger.error("OpenBD API timeout")
                await safe_reply(message, "書籍情報の取得がタイムアウトしました。しばらく時間をおいて再度お試しください。")
            except requests.exceptions.RequestException as e:
                logger.error(f"OpenBD API request error: {e}")
                await safe_reply(message, "書籍情報の取得に失敗しました。しばらく時間をおいて再度お試しください。")
            except Exception as e:
                logger.error(f"Error processing ISBN: {e}")
                await safe_reply(message, "申し訳ありません。処理中にエラーが発生しました。しばらく時間をおいて再度お試しください。")
    except Exception as e:
        logger.error(f"Message processing error: {e}")

# 改良されたメイン処理
async def main():
    """Rate Limit対策を含むメイン処理"""
    try:
        # 起動回数制限のチェック
        can_start, attempt_count = check_startup_limits()
        health_status['startup_attempts'] = attempt_count
        
        if not can_start:
            # 24時間待機
            logger.info("Sleeping for 24 hours due to startup attempt limit")
            await asyncio.sleep(86400)  # 24時間 = 86400秒
            return
        
        # 起動データを保存
        save_startup_data(attempt_count)
        logger.info(f"Startup attempt {attempt_count}/{MAX_STARTUP_ATTEMPTS}")
        
        # Webサーバーを別スレッドで起動
        web_thread = threading.Thread(target=run_web, daemon=True)
        web_thread.start()
        logger.info("Web server started")
        
        # 初期遅延（Cloudflareのブロック回避）
        initial_delay = random.uniform(10, 30)
        logger.info(f"Initial delay: {initial_delay:.1f} seconds")
        await asyncio.sleep(initial_delay)
        
        # 安全な接続試行
        success = await safe_connect_with_backoff()
        
        if not success:
            logger.error("Failed to establish Discord connection after all retries")
            # 失敗時の待機時間を起動回数に応じて調整
            failure_delay = min(300 + (attempt_count * 60), 3600)  # 最大1時間
            logger.info(f"Waiting {failure_delay} seconds before exit")
            await asyncio.sleep(failure_delay)
            
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        health_status['last_error'] = str(e)
        # エラー時は長時間待機
        await asyncio.sleep(600)  # 10分待機

if __name__ == "__main__":
    try:
        # Rate Limit回避のため、すぐには接続しない
        logger.info("Discord Bot starting with Rate Limit protection...")
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        time.sleep(300)  # 5分待機してから終了
