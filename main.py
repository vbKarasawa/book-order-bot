import discord
import re
import gspread
import requests
import os
import json
import logging
import asyncio
import random
import time
import threading
from datetime import datetime, timedelta, timezone
from oauth2client.service_account import ServiceAccountCredentials
from isbnlib import to_isbn10, to_isbn13, canonical, is_isbn10, is_isbn13
from flask import Flask, request

# ログ設定（より詳細な設定）
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # コンソール出力
    ]
)
logger = logging.getLogger(__name__)

# Rate Limit対策の設定
RATE_LIMIT_DETECTED = False
RATE_LIMIT_START_TIME = None
MIN_WAIT_MINUTES = 30  # 最低30分待機
MAX_WAIT_MINUTES = 120  # 最大2時間待機

# Flask app for health check
app = Flask(__name__)

# シンプルなヘルスチェック用の変数
health_status = {
    'status': 'running',
    'last_check': datetime.now().isoformat(),
    'bot_connected': False,
    'total_messages': 0,
    'successful_messages': 0,
    'message_success_rate': 0.0
}

@app.route('/')
def hello():
    """メインのヘルスチェックエンドポイント"""
    return "Discord Bot is running!"

@app.route('/ping')
def ping():
    """UptimeRobot用のpingエンドポイント"""
    health_status['last_check'] = datetime.now().isoformat()
    return "pong"

@app.route('/status')
def status():
    """詳細なステータス情報"""
    return health_status

def run_web():
    """Flaskサーバーを実行"""
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

def get_server_ip():
    """サーバーIPを取得"""
    try:
        response = requests.get('https://api.ipify.org')
        return response.text
    except:
        return "Unknown"

def handle_rate_limit_error(error_message):
    """Rate Limit エラーの処理"""
    global RATE_LIMIT_DETECTED, RATE_LIMIT_START_TIME
    
    current_time = datetime.now()
    
    # Rate Limit検出
    if "429" in str(error_message) or "rate limit" in str(error_message).lower():
        RATE_LIMIT_DETECTED = True
        RATE_LIMIT_START_TIME = current_time
        
        # ランダムな待機時間 (30分～2時間)
        wait_minutes = random.randint(MIN_WAIT_MINUTES, MAX_WAIT_MINUTES)
        wait_seconds = wait_minutes * 60
        
        logger.error(f"Rate Limit検出: {current_time}")
        logger.error(f"緊急待機開始: {wait_minutes}分 ({wait_seconds}秒)")
        
        # エラーログの詳細保存
        error_log = f"""
=== RATE LIMIT ERROR DETECTED ===
Time: {current_time}
Wait Duration: {wait_minutes} minutes
Error Details: {error_message}
IP: {get_server_ip()}
Next Retry: {current_time + timedelta(minutes=wait_minutes)}
================================
        """
        
        # ログファイルに保存
        with open('/tmp/rate_limit_errors.txt', 'a') as f:
            f.write(error_log)
        
        logger.info(f"エラーログ保存完了。{wait_minutes}分待機中...")
        
        # 実際の待機
        time.sleep(wait_seconds)
        
        # 待機終了後もRate Limitフラグは維持
        logger.info(f"{wait_minutes}分の待機完了。慎重に再開します。")
        
        return True
    
    return False

# Discord bot setup with enhanced error handling
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_credentials_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if google_credentials_json:
    creds_dict = json.loads(google_credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_url(os.environ.get('GOOGLE_SHEET_URL')).sheet1
else:
    logger.error("Google認証情報が見つかりません")

def safe_reply(message, content, max_retries=5):
    """Enhanced safe reply with exponential backoff"""
    base_wait_time = 5.0  # 基本待機時間を5秒に増加
    
    for attempt in range(max_retries):
        try:
            # Discord指定の待機時間を確認
            wait_time = base_wait_time * (2 ** attempt)  # 指数関数的バックオフ
            
            logger.info(f"返信試行 {attempt + 1}/{max_retries} - {wait_time:.1f}秒待機後")
            time.sleep(wait_time)
            
            # 実際の返信を送信
            asyncio.create_task(message.reply(content))
            
            # 成功統計を更新
            health_status['successful_messages'] += 1
            health_status['total_messages'] += 1
            health_status['message_success_rate'] = health_status['successful_messages'] / health_status['total_messages']
            
            logger.info(f"返信成功 (試行 {attempt + 1})")
            return True
            
        except discord.HTTPException as e:
            error_str = str(e)
            logger.warning(f"返信試行 {attempt + 1} 失敗: {error_str}")
            
            # Rate Limitエラーの特別処理
            if "429" in error_str or "rate limit" in error_str.lower():
                if handle_rate_limit_error(error_str):
                    return False
            
            # Discord指定の待機時間があるかチェック
            if hasattr(e, 'retry_after') and e.retry_after:
                discord_wait_time = e.retry_after + 1  # +1秒のマージン
                logger.info(f"Discord指定待機時間: {discord_wait_time}秒")
                time.sleep(discord_wait_time)
            
            if attempt == max_retries - 1:
                logger.error(f"返信最終試行失敗: {error_str}")
                
        except Exception as e:
            logger.error(f"返信エラー (試行 {attempt + 1}): {str(e)}")
            
            # Rate Limitエラーの可能性をチェック
            if handle_rate_limit_error(str(e)):
                return False
    
    # 統計更新（失敗）
    health_status['total_messages'] += 1
    if health_status['total_messages'] > 0:
        health_status['message_success_rate'] = health_status['successful_messages'] / health_status['total_messages']
    
    return False

def get_openbd_info(isbn):
    """OpenBD APIから書籍情報を取得"""
    try:
        url = f"https://api.openbd.jp/v1/get?isbn={isbn}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data and data[0]:
            book_data = data[0]
            title = book_data.get('summary', {}).get('title', 'タイトル不明')
            publisher = book_data.get('summary', {}).get('publisher', '出版社不明')
            
            # 価格情報の取得
            price = None
            if 'onix' in book_data and 'ProductSupply' in book_data['onix']:
                supply_detail = book_data['onix']['ProductSupply'].get('SupplyDetail', {})
                if 'Price' in supply_detail:
                    price_data = supply_detail['Price']
                    if isinstance(price_data, list) and len(price_data) > 0:
                        price = price_data[0].get('PriceAmount', None)
                    elif isinstance(price_data, dict):
                        price = price_data.get('PriceAmount', None)
            
            return title, publisher, price
        
        return None, None, None
        
    except Exception as e:
        logger.error(f"OpenBD API エラー: {e}")
        return None, None, None

def get_hanmoto_url(isbn):
    """版元ドットコムのURLを生成"""
    return f"https://www.hanmoto.com/bd/isbn/{isbn}"

@client.event
async def on_ready():
    """Bot起動時の処理"""
    logger.info(f'{client.user} has landed!')
    health_status['bot_connected'] = True

@client.event
async def on_message(message):
    """メッセージ受信時の処理"""
    if message.author == client.user:
        return

    # ISBN検出の正規表現パターン
    isbn_pattern = r'(?:ISBN[:\s-]*)?(?:978[:\s-]*)?(\d{1}[:\s-]*\d{3,5}[:\s-]*\d{1,7}[:\s-]*\d{1}[:\s-]*\d{1}|\d{1}[:\s-]*\d{3,5}[:\s-]*\d{1,7}[:\s-]*\d{1})'
    
    content = message.content
    match = re.search(isbn_pattern, content)
    
    if match:
        isbn_raw = match.group(1)
        isbn_digits = re.sub(r'[:\s-]', '', isbn_raw)
        
        logger.info(f"ISBN検出: {isbn_digits}")
        
        try:
            # ISBN形式の確認と変換
            isbn_10 = None
            isbn_13 = None
            
            if len(isbn_digits) == 10 and is_isbn10(isbn_digits):
                isbn_10 = isbn_digits
                isbn_13 = to_isbn13(isbn_digits)
            elif len(isbn_digits) == 13 and is_isbn13(isbn_digits):
                isbn_13 = isbn_digits
                isbn_10 = to_isbn10(isbn_digits)
            else:
                safe_reply(message, f"無効なISBN形式です: {isbn_digits}")
                return
            
            # OpenBD APIから書籍情報を取得
            title, publisher, price = get_openbd_info(isbn_13 if isbn_13 else isbn_10)
            
            if title is None:
                safe_reply(message, f"ISBN {isbn_digits} の書籍情報が見つかりませんでした。")
                return
            
            # 現在の日付を取得
            current_date = datetime.now(timezone(timedelta(hours=9))).strftime('%Y/%m/%d')
            hanmoto_url = get_hanmoto_url(isbn_13 if isbn_13 else isbn_10)
            
            # スプレッドシートへの情報書き込み
            try:
                new_row = [str(current_date), str(isbn_10), str(isbn_13), title, str(price), publisher, 2, '注文待ち', str(message.author.id), str(hanmoto_url)]
                sheet.append_row(new_row)
                logger.info(f"Google Sheetにデータ追加: {new_row}")
                
                # 成功メッセージ
                reply_content = f"📚 書籍情報を登録しました！\n**タイトル**: {title}\n**出版社**: {publisher}"
                if price:
                    reply_content += f"\n**価格**: ¥{price}"
                reply_content += f"\n**詳細**: {hanmoto_url}"
                
                success = safe_reply(message, reply_content)
                if not success:
                    logger.warning(f"Reply failed but order processed successfully: {title}")
                
            except Exception as e:
                error_str = str(e)
                logger.error(f"Google Sheets書き込みエラー: {error_str}")
                
                # Rate Limitエラーの可能性をチェック
                if handle_rate_limit_error(error_str):
                    return
                
                safe_reply(message, f"書籍情報の保存でエラーが発生しました: {error_str}")
                
        except Exception as e:
            error_str = str(e)
            logger.error(f"ISBN処理エラー: {error_str}")
            
            # Rate Limitエラーの可能性をチェック
            if handle_rate_limit_error(error_str):
                return
            
            safe_reply(message, f"書籍情報の処理でエラーが発生しました: {error_str}")

def safe_discord_login():
    """安全なDiscordログイン"""
    global RATE_LIMIT_DETECTED, RATE_LIMIT_START_TIME
    
    # Rate Limit状態をチェック
    if RATE_LIMIT_DETECTED and RATE_LIMIT_START_TIME:
        time_since_rate_limit = datetime.now() - RATE_LIMIT_START_TIME
        
        # 最低1時間は慎重モード
        if time_since_rate_limit.total_seconds() < 3600:  # 1時間
            logger.warning("Rate Limit後の慎重期間中。ゆっくりと接続します...")
            time.sleep(30)  # 30秒追加待機
    
    try:
        # ログイン試行
        logger.info("Discordへの接続を試行中...")
        return client.run(os.environ['DISCORD_TOKEN'])
    
    except Exception as e:
        error_str = str(e)
        
        # Rate Limitエラーかチェック
        if handle_rate_limit_error(error_str):
            # Rate Limit処理が完了したら、プロセスを終了
            logger.info("Rate Limit対策完了。プロセスを安全に終了します。")
            exit(0)
        else:
            # 他のエラーの場合
            logger.error(f"Discord接続エラー: {error_str}")
            raise e

# メインの起動部分を修正
if __name__ == "__main__":
    try:
        # Flaskサーバーを別スレッドで開始
        threading.Thread(target=run_web, daemon=True).start()
        
        # 起動時の慎重チェック
        logger.info("Discord Bot開始前の安全チェック...")
        time.sleep(10)  # 10秒の初期待機
        
        # 安全なDiscordログイン
        safe_discord_login()
        
    except Exception as e:
        logger.error(f"起動エラー: {e}")
        # エラー時は長時間待機してから終了
        logger.info("エラー検出。60分待機後に終了します。")
        time.sleep(3600)  # 60分待機
        exit(1)
