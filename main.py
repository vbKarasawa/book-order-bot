import discord
import re
import gspread
import requests
import os
import json
import logging
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from isbnlib import to_isbn10, to_isbn13, canonical, is_isbn10, is_isbn13
from flask import Flask, request
import threading
import time

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Flask app for health check
app = Flask(__name__)

# シンプルなヘルスチェック用の変数
health_status = {
    'status': 'running',
    'last_check': datetime.now().isoformat(),
    'bot_connected': False
}

@app.route('/')
def hello():
    """メインのヘルスチェックエンドポイント"""
    return "Discord Bot is running!"

@app.route('/health')
def health():
    """詳細なヘルスチェック情報"""
    health_status['last_check'] = datetime.now().isoformat()
    return json.dumps(health_status), 200, {'Content-Type': 'application/json'}

@app.route('/ping')
def ping():
    """シンプルなping応答"""
    return "pong"

@app.route('/status')
def status():
    """ステータス確認用（User-Agentログ付き）"""
    user_agent = request.headers.get('User-Agent', 'Unknown')
    logging.info(f"Status check from: {user_agent}")
    
    # Monitor用の簡潔な応答
    if 'monitor' in user_agent.lower() or 'uptimerobot' in user_agent.lower():
        return "OK"
    
    return {
        'status': 'active',
        'timestamp': datetime.now().isoformat(),
        'bot_status': health_status['bot_connected']
    }

@app.route('/robots.txt')
def robots():
    """robots.txtでBot判定を軽減"""
    return """User-agent: *
Allow: /
Allow: /health
Allow: /ping
Allow: /status"""

def run_web():
    """Webサーバーを起動"""
    port = int(os.environ.get('PORT', 5000))
    # デバッグモードを無効にして本番環境用に最適化
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

# 環境変数からトークンを取得
TOKEN = os.environ.get('DISCORD_TOKEN')
API_KEY = os.environ.get('GOOGLE_BOOKS_API_KEY')

if not TOKEN:
    raise ValueError("DISCORD_TOKEN environment variable is not set")

# Google Sheetsの設定
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

# 環境変数からGoogle認証情報を取得
google_creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if not google_creds_json:
    raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is not set")

# JSON文字列を辞書に変換
creds_dict = json.loads(google_creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# スプレッドシートのIDを環境変数から取得
SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '1cZ9BGx2etjDQEHi_izTfbYHuzkLJLHx8UDucZMsbp6s')
sheet = client.open_by_key(SHEET_ID).sheet1

# 改良されたISBNの正規表現パターン - スペースやハイフンを含むISBN-10/13を検出
ISBN_PATTERN = r"\b(?:(?:[\d][\d\s-]*){9}[\dXx]|(?:[\d][\d\s-]*){12}\d)\b"

# Google Books APIの設定
GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"

# Discordクライアント設定
intents = discord.Intents.default()
intents.message_content = True
client_discord = discord.Client(intents=intents)

# Rate Limit対応の安全な返信機能
async def safe_reply(message, content, max_retries=3):
    """
    Rate Limitに対応した安全な返信機能
    """
    for attempt in range(max_retries):
        try:
            await message.reply(content)
            logging.info(f"Message sent successfully on attempt {attempt + 1}")
            return True
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate Limited
                wait_time = 2 ** attempt  # エクスポネンシャルバックオフ (2, 4, 8秒)
                logging.warning(f"Rate limited. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                await asyncio.sleep(wait_time)
            else:
                logging.error(f"HTTP Exception: {e}")
                return False
        except Exception as e:
            logging.error(f"Unexpected error in safe_reply: {e}")
            return False
    
    logging.error(f"Failed to send message after {max_retries} attempts")
    return False

# ISBNが有効かどうかをチェックする関数
def is_valid_isbn(isbn):
    """
    ISBNが有効かどうかを検証する
    ISBN-10で無効な場合は、先頭に978を付けてISBN-13としても検証する
    """
    # ハイフンやスペースを削除
    clean_isbn = canonical(isbn)
    
    # 通常のチェック: ISBN-10またはISBN-13として有効かどうか
    if is_isbn10(clean_isbn) or is_isbn13(clean_isbn):
        return True
    
    # ISBN-10フォーマット（10桁）だけど標準のチェックで無効だった場合
    if len(clean_isbn) == 10:
        try:
            # 978を先頭に付けて、正しいISBN-13を作成してみる
            base_isbn = "978" + clean_isbn[:-1]
            
            # 実際のチェックディジットを計算（1桁ずつ）
            sum_odd = 0
            sum_even = 0
            for i, digit in enumerate(base_isbn):
                if i % 2 == 0:  # 偶数位置（0始まりなので、実際は奇数桁）
                    sum_odd += int(digit)
                else:  # 奇数位置（0始まりなので、実際は偶数桁）
                    sum_even += int(digit) * 3
            
            total_sum = sum_odd + sum_even
            check_digit = (10 - (total_sum % 10)) % 10  # 10だと0に変換
            
            # 完全なISBN-13を作成
            isbn13 = base_isbn + str(check_digit)
            
            # 最終チェック
            if is_isbn13(isbn13):
                logging.info(f"978変換で有効なISBN-13に変換: {isbn13}")
                return True
        except Exception as e:
            logging.error(f"ISBN変換エラー: {e}")
    
    return False

# ISBNの変換を安全に行う関数
def safe_isbn_conversion(isbn):
    """
    ISBNを安全にISBN-10とISBN-13に変換する
    エラーが発生した場合はNoneを返す
    """
    # ハイフンやスペースを削除
    clean_isbn = canonical(isbn)
    
    # 変換結果を格納する辞書
    result = {
        "original": isbn,
        "clean": clean_isbn,
        "isbn_10": None,
        "isbn_13": None
    }
    
    # ISBN-10への変換を試みる
    try:
        if len(clean_isbn) == 13:
            result["isbn_10"] = to_isbn10(clean_isbn)
        elif len(clean_isbn) == 10:
            result["isbn_10"] = clean_isbn
    except Exception as e:
        logging.error(f"ISBN-10変換エラー: {e}")
        pass
    
    # ISBN-13への変換を試みる
    try:
        if len(clean_isbn) == 10:
            result["isbn_13"] = to_isbn13(clean_isbn)
        elif len(clean_isbn) == 13:
            result["isbn_13"] = clean_isbn
    except Exception as e:
        logging.error(f"ISBN-13変換エラー: {e}")
        pass
    
    return result

# Google Books APIを使ってISBNから書籍情報を取得する関数
def get_book_info(isbn):
    if not API_KEY:
        logging.warning("Google Books API key not found")
        return None
        
    params = {
        "q": f"isbn:{isbn}",
        "key": API_KEY
    }

    response = requests.get(GOOGLE_BOOKS_API_URL, params=params)

    if response.status_code == 200:
        data = response.json()

        if "items" in data and len(data["items"]) > 0:
            book_data = data["items"][0]["volumeInfo"]
            title = book_data.get("title", "タイトル情報なし")
            authors = ", ".join(book_data.get("authors", ["著者情報なし"]))

            # ISBNを取得
            isbn_identifiers = book_data.get("industryIdentifiers", [])
            isbn10 = None
            isbn13 = None

            for identifier in isbn_identifiers:
                if identifier["type"] == "ISBN_13":
                    isbn13 = identifier["identifier"]
                elif identifier["type"] == "ISBN_10":
                    isbn10 = identifier["identifier"]

            # ISBN-10がない場合、ISBN-13から変換を試みる
            if not isbn10 and isbn13:
                try:
                    isbn10 = to_isbn10(isbn13)
                except:
                    isbn10 = "ISBN-10情報なし"

            # ISBN-13がない場合、ISBN-10から変換を試みる
            if not isbn13 and isbn10:
                try:
                    isbn13 = to_isbn13(isbn10)
                except:
                    isbn13 = "ISBN-13情報なし"

            if not isbn10:
                isbn10 = "ISBN-10情報なし"
            if not isbn13:
                isbn13 = "ISBN-13情報なし"

            return {
                "title": title,
                "author": authors,
                "isbn10": isbn10,
                "isbn13": isbn13
            }
        else:
            return {
                "title": "該当する書籍情報が見つかりません",
                "author": "該当する著者情報が見つかりません",
                "isbn10": "該当するISBN-10情報が見つかりません",
                "isbn13": "該当するISBN-13情報が見つかりません"
            }
    else:
        return {
            "title": "APIリクエストエラー",
            "author": "APIリクエストエラー",
            "isbn10": "APIリクエストエラー",
            "isbn13": "APIリクエストエラー"
        }

# ヘルスステータス更新用のバックグラウンドタスク
async def update_health_status():
    """定期的にヘルスステータスを更新"""
    while True:
        try:
            health_status['last_check'] = datetime.now().isoformat()
            health_status['bot_connected'] = client_discord.is_ready()
            await asyncio.sleep(30)  # 30秒ごとに更新
        except Exception as e:
            logging.error(f"Health status update error: {e}")
            await asyncio.sleep(60)

# Botが起動した際の処理
@client_discord.event
async def on_ready():
    logging.info(f'Logged in as {client_discord.user}')
    health_status['bot_connected'] = True
    
    # バックグラウンドタスクを開始
    asyncio.create_task(update_health_status())

# Botが切断された際の処理
@client_discord.event
async def on_disconnect():
    logging.warning('Bot disconnected')
    health_status['bot_connected'] = False

# メッセージを受け取ったときの処理
@client_discord.event
async def on_message(message):
    if message.author == client_discord.user or message.author.bot:
        return

    # ISBNが含まれているか確認（ハイフンとスペース対応）
    match = re.search(ISBN_PATTERN, message.content)
    if match:
        # 取得したISBNからハイフンとスペースを削除
        isbn_with_separators = match.group(0)
        isbn = re.sub(r'[\s-]', '', isbn_with_separators)
        logging.info(f"ISBN Found: {isbn} (元の形式: {isbn_with_separators})")

        # ISBNの有効性を確認
        if not is_valid_isbn(isbn):
            await safe_reply(message, "申し訳ありません。有効なISBN形式ではないようです。正しいISBNを入力してください。")
            return

        # ISBNをISBN-10とISBN-13に変換
        isbn_data = safe_isbn_conversion(isbn)
        isbn_10 = isbn_data["isbn_10"]
        isbn_13 = isbn_data["isbn_13"]
        
        if not isbn_10 and not isbn_13:
            await safe_reply(message, "ISBN変換に失敗しました。正確なISBNを入力してください。")
            return
            
        logging.info(f"ISBN-10: {isbn_10}, ISBN-13: {isbn_13}")
        
        try:
            # OpenBD APIを使って書籍情報を取得
            openbd_url = f'https://api.openbd.jp/v1/get?isbn={isbn_13}'
            hanmoto_url = f'https://www.hanmoto.com/bd/isbn/{isbn_13}'
            response = requests.get(openbd_url)
            book_info = response.json()
            current_date = datetime.now().strftime("%Y/%m/%d")

            if book_info and book_info[0] is not None:
                summary = book_info[0]['summary']
                title = summary.get('title', 'タイトル不明')
                publisher = summary.get('publisher', '出版社不明')

                # 価格情報の取得（onixフィールドから取得）
                price = '定価不明'
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

                # スプレッドシートへの情報書き込み
                new_row = [str(current_date), str(isbn_10), str(isbn_13), title, str(price), publisher, 2, '注文待ち', str(message.author.id), str(hanmoto_url)]
                sheet.append_row(new_row)
                logging.info(f"Data added to Google Sheet: {new_row}")

                # 書籍情報が取得できた場合にメッセージを引用して返信
                reply_message = f"ありがとうございます！\n『{title}』を2冊発注依頼しました！\n{hanmoto_url}"
                await safe_reply(message, reply_message)

            else:
                # スプレッドシートへの情報書き込み
                new_row = [str(current_date), str(isbn_10), str(isbn_13), "", "", "", 2, '注文待ち', str(message.author.id), str(hanmoto_url)]
                sheet.append_row(new_row)
                logging.info(f"Data added to Google Sheet: {new_row}")
                # 書籍情報が取得できなかった場合にメッセージを引用して返信
                error_message = f"ありがとうございます！\n注文された書籍を２冊発注依頼しました！（まだ知識が浅くて、書籍タイトルを持ってこれませんでした。ごめんなさい！）\nこちらの本を発注しています！\n{hanmoto_url}"
                await safe_reply(message, error_message)
                
        except Exception as e:
            logging.error(f"Error processing ISBN: {e}")
            # エラー時も安全な返信を使用
            await safe_reply(message, "申し訳ありません。処理中にエラーが発生しました。しばらく時間をおいて再度お試しください。")

if __name__ == "__main__":
    # Webサーバーを別スレッドで起動
    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()
    
    # 少し待ってからDiscord Botを起動
    time.sleep(2)
    
    # Discord Bot起動
    try:
        client_discord.run(TOKEN)
    except KeyboardInterrupt:
        logging.info("Bot shutdown requested")
    except Exception as e:
        logging.error(f"Bot error: {e}")
        raise
