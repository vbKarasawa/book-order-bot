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

def calculate_isbn10_check_digit(isbn9):
    """ISBN10のチェックディジットを計算"""
    try:
        total = 0
        for i, digit in enumerate(isbn9):
            if not digit.isdigit():
                return None
            total += int(digit) * (10 - i)
        
        remainder = total % 11
        if remainder == 0:
            return '0'
        elif remainder == 1:
            return 'X'
        else:
            return str(11 - remainder)
    except:
        return None

def calculate_isbn13_check_digit(isbn12):
    """ISBN13のチェックディジットを計算"""
    try:
        total = 0
        for i, digit in enumerate(isbn12):
            if not digit.isdigit():
                return None
            multiplier = 1 if i % 2 == 0 else 3
            total += int(digit) * multiplier
        
        remainder = total % 10
        return str((10 - remainder) % 10)
    except:
        return None

def fix_common_isbn_errors(isbn_input):
    """一般的なISBN入力間違いを修正"""
    try:
        # 数字とXのみ抽出
        clean_isbn = re.sub(r'[^\dX]', '', isbn_input.upper())
        
        # ケース1: ISBN13の後ろ10桁をISBN10として間違えて入力
        if len(clean_isbn) == 10:
            # 通常のISBN10として検証
            if is_isbn10(clean_isbn):
                return clean_isbn, to_isbn13(clean_isbn)
            
            # ISBN13の後ろ10桁の可能性をチェック
            # 978 + 9桁 + チェックディジット の形で13桁にしてみる
            if clean_isbn[0].isdigit():  # Xで始まることはない
                test_isbn13 = '978' + clean_isbn
                if len(test_isbn13) == 13 and is_isbn13(test_isbn13):
                    corrected_isbn10 = to_isbn10(test_isbn13)
                    if corrected_isbn10:
                        logger.info(f"ISBN修正: 後ろ10桁パターン {clean_isbn} -> {corrected_isbn10}")
                        return corrected_isbn10, test_isbn13
            
            # 979プレフィックスも試す
            test_isbn13_979 = '979' + clean_isbn
            if len(test_isbn13_979) == 13 and is_isbn13(test_isbn13_979):
                corrected_isbn10 = to_isbn10(test_isbn13_979)
                if corrected_isbn10:
                    logger.info(f"ISBN修正: 979後ろ10桁パターン {clean_isbn} -> {corrected_isbn10}")
                    return corrected_isbn10, test_isbn13_979
            
            # チェックディジットが間違っている可能性
            isbn9 = clean_isbn[:9]
            correct_check = calculate_isbn10_check_digit(isbn9)
            if correct_check and correct_check != clean_isbn[9]:
                corrected_isbn10 = isbn9 + correct_check
                if is_isbn10(corrected_isbn10):
                    logger.info(f"ISBN修正: ISBN10チェックディジット {clean_isbn} -> {corrected_isbn10}")
                    return corrected_isbn10, to_isbn13(corrected_isbn10)
        
        # ケース2: ISBN10の前に978を付けただけの間違ったISBN13
        elif len(clean_isbn) == 13:
            # 通常のISBN13として検証
            if is_isbn13(clean_isbn):
                return to_isbn10(clean_isbn), clean_isbn
            
            # 978 + ISBN10 の形になっている可能性
            if clean_isbn.startswith('978'):
                potential_isbn10 = clean_isbn[3:]
                if len(potential_isbn10) == 10:
                    # ISBN10として正しいかチェック
                    if is_isbn10(potential_isbn10):
                        corrected_isbn13 = to_isbn13(potential_isbn10)
                        logger.info(f"ISBN修正: 978+ISBN10パターン {clean_isbn} -> {corrected_isbn13}")
                        return potential_isbn10, corrected_isbn13
                    
                    # ISBN10のチェックディジットを再計算
                    isbn9 = potential_isbn10[:9]
                    correct_check = calculate_isbn10_check_digit(isbn9)
                    if correct_check:
                        test_isbn10 = isbn9 + correct_check
                        if is_isbn10(test_isbn10):
                            corrected_isbn13 = to_isbn13(test_isbn10)
                            logger.info(f"ISBN修正: 978+ISBN10(チェックディジット修正) {clean_isbn} -> {corrected_isbn13}")
                            return test_isbn10, corrected_isbn13
            
            # チェックディジットが間違っている可能性
            isbn12 = clean_isbn[:12]
            correct_check = calculate_isbn13_check_digit(isbn12)
            if correct_check and correct_check != clean_isbn[12]:
                corrected_isbn13 = isbn12 + correct_check
                if is_isbn13(corrected_isbn13):
                    logger.info(f"ISBN修正: ISBN13チェックディジット {clean_isbn} -> {corrected_isbn13}")
                    return to_isbn10(corrected_isbn13), corrected_isbn13
        
        # ケース3: 9桁や12桁の不完全な入力
        elif len(clean_isbn) == 9:
            # ISBN10の最初の9桁の可能性
            correct_check = calculate_isbn10_check_digit(clean_isbn)
            if correct_check:
                test_isbn10 = clean_isbn + correct_check
                if is_isbn10(test_isbn10):
                    logger.info(f"ISBN修正: 9桁補完 {clean_isbn} -> {test_isbn10}")
                    return test_isbn10, to_isbn13(test_isbn10)
        
        elif len(clean_isbn) == 12:
            # ISBN13の最初の12桁の可能性
            correct_check = calculate_isbn13_check_digit(clean_isbn)
            if correct_check:
                test_isbn13 = clean_isbn + correct_check
                if is_isbn13(test_isbn13):
                    logger.info(f"ISBN修正: 12桁補完 {clean_isbn} -> {test_isbn13}")
                    return to_isbn10(test_isbn13), test_isbn13
        
        return None, None
        
    except Exception as e:
        logger.error(f"ISBN修正処理エラー: {e}")
        return None, None

def normalize_isbn_for_dedup(isbn_raw):
    """重複検出用のISBN正規化（ISBN13形式に統一）"""
    try:
        # まず標準的な方法で処理を試行
        isbn_digits = re.sub(r'[:\s-]', '', isbn_raw).upper()
        
        # 標準的な処理
        if len(isbn_digits) == 10 and is_isbn10(isbn_digits):
            return to_isbn13(isbn_digits)
        elif len(isbn_digits) == 13 and is_isbn13(isbn_digits):
            return isbn_digits
        else:
            # 修正機能を試行
            fixed_isbn10, fixed_isbn13 = fix_common_isbn_errors(isbn_raw)
            if fixed_isbn13:
                return fixed_isbn13
            
        return None
    except:
        return None

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

def process_single_isbn(isbn_raw, message_author_id):
    """単一ISBNの処理"""
    try:
        # まず標準的な方法で処理を試行
        isbn_digits = re.sub(r'[:\s-]', '', isbn_raw).upper()
        isbn_10 = None
        isbn_13 = None
        
        # 標準的な処理
        if len(isbn_digits) == 10 and is_isbn10(isbn_digits):
            isbn_10 = isbn_digits
            isbn_13 = to_isbn13(isbn_digits)
        elif len(isbn_digits) == 13 and is_isbn13(isbn_digits):
            isbn_13 = isbn_digits
            isbn_10 = to_isbn10(isbn_digits)
        else:
            # 標準処理で失敗した場合、修正機能を試行
            fixed_isbn10, fixed_isbn13 = fix_common_isbn_errors(isbn_raw)
            if fixed_isbn10 and fixed_isbn13:
                isbn_10 = fixed_isbn10
                isbn_13 = fixed_isbn13
            else:
                return None, None, f"無効なISBN形式です: {isbn_digits}"
        
        # 版元ドットコムのURL
        hanmoto_url = get_hanmoto_url(isbn_13 if isbn_13 else isbn_10)
        
        # OpenBD APIから書籍情報を取得
        title, publisher, price = get_openbd_info(isbn_13 if isbn_13 else isbn_10)
        
        # 現在の日付を取得
        current_date = datetime.now(timezone(timedelta(hours=9))).strftime('%Y/%m/%d')
        
        # スプレッドシートへの情報書き込み
        try:
            if title is None:
                # 書籍情報が取得できなかった場合
                new_row = [str(current_date), str(isbn_10), str(isbn_13), "", "", "", 2, '注文待ち', str(message_author_id), str(hanmoto_url)]
                sheet.append_row(new_row)
                logger.info(f"Google Sheetにデータ追加（書籍情報なし）: {new_row}")
                
                # 書籍情報なしの場合はURLを返す
                return None, hanmoto_url, None
            else:
                # 書籍情報が取得できた場合
                new_row = [str(current_date), str(isbn_10), str(isbn_13), title, str(price), publisher, 2, '注文待ち', str(message_author_id), str(hanmoto_url)]
                sheet.append_row(new_row)
                logger.info(f"Google Sheetにデータ追加: {new_row}")
                
                # 書籍情報ありの場合はタイトルのみ返す
                return title, None, None
            
        except Exception as e:
            error_str = str(e)
            logger.error(f"Google Sheets書き込みエラー: {error_str}")
            
            # Rate Limitエラーの可能性をチェック
            if handle_rate_limit_error(error_str):
                return None, None, "Rate Limit検出のため処理を中断しました"
            
            return None, None, f"書籍情報の保存でエラーが発生しました: {error_str}"
            
    except Exception as e:
        error_str = str(e)
        logger.error(f"ISBN処理エラー: {error_str}")
        
        # Rate Limitエラーの可能性をチェック
        if handle_rate_limit_error(error_str):
            return None, None, "Rate Limit検出のため処理を中断しました"
        
        return None, None, f"書籍情報の処理でエラーが発生しました: {error_str}"

@client.event
async def on_ready():
    """Bot起動時の処理"""
    logger.info(f'{client.user} has landed!')
    health_status['bot_connected'] = True

@client.event
async def on_message(message):
    """メッセージ受信時の処理"""
    if message.author == client.user or message.author.bot:
        return

    # ISBN検出の正規表現パターン（より柔軟に、複数対応）
    isbn_pattern = r'(?:ISBN[:\s-]*)?(?:978[:\s-]*|979[:\s-]*)?(\d{1}[:\s-]*\d{3,5}[:\s-]*\d{1,7}[:\s-]*\d{1}[:\s-]*[\dX]|\d{9,13}[\dX]?)'
    
    content = message.content
    matches = re.findall(isbn_pattern, content, re.IGNORECASE)
    
    if matches:
        logger.info(f"ISBN候補検出（{len(matches)}件）: {matches}")
        
        # 重複除去処理
        seen_isbns = set()
        unique_isbns = []
        duplicate_count = 0
        
        for isbn_raw in matches:
            normalized = normalize_isbn_for_dedup(isbn_raw)
            if normalized and normalized not in seen_isbns:
                seen_isbns.add(normalized)
                unique_isbns.append(isbn_raw)
            elif normalized:
                duplicate_count += 1
                logger.info(f"重複ISBN検出（スキップ）: {isbn_raw} -> {normalized}")
        
        if duplicate_count > 0:
            logger.info(f"重複除去: {len(matches)}件 -> {len(unique_isbns)}件 ({duplicate_count}件の重複を除去)")
        
        # 処理結果を保存するリスト
        successful_books = []
        books_without_info = []
        error_messages = []
        
        # 各ISBNを個別に処理（重複除去後）
        for isbn_raw in unique_isbns:
            logger.info(f"処理中: {isbn_raw}")
            
            # Rate Limit状態をチェック
            global RATE_LIMIT_DETECTED, RATE_LIMIT_START_TIME
            if RATE_LIMIT_DETECTED:
                logger.warning("Rate Limit検出中のため、処理をスキップします")
                break
            
            # 単一ISBNを処理
            result_title, result_url, error_msg = process_single_isbn(isbn_raw, message.author.id)
            
            if result_title:
                # 書籍情報が取得できた場合
                successful_books.append(result_title)
            elif result_url:
                # 書籍情報は取得できなかったがISBNは有効だった場合
                books_without_info.append(result_url)
            elif error_msg:
                error_messages.append(error_msg)
                # エラーがRate Limit関連の場合は処理を中断
                if "Rate Limit" in error_msg:
                    break
            
            # 複数処理時は少し間隔を空ける
            if len(unique_isbns) > 1:
                time.sleep(2)
        
        # 結果に応じて返信メッセージを作成
        reply_parts = []
        
        if successful_books:
            if len(successful_books) == 1:
                reply_parts.append(f"ありがとうございます！\n『{successful_books[0]}』を2冊発注依頼しました！")
            else:
                book_list = '\n'.join([f"・『{title}』" for title in successful_books])
                reply_parts.append(f"ありがとうございます！\n以下の書籍を各2冊ずつ発注依頼しました！\n{book_list}")
        
        if books_without_info:
            if len(books_without_info) == 1:
                reply_parts.append(f"ありがとうございます！\nこちらの書籍を2冊発注依頼しました！（書籍情報を取得できませんでした）\n{books_without_info[0]}")
            else:
                url_list = '\n'.join([f"・{url}" for url in books_without_info])
                reply_parts.append(f"以下の書籍も各2冊ずつ発注依頼しました！（書籍情報を取得できませんでした）\n{url_list}")
        
        # 成功した処理があれば返信
        if reply_parts:
            reply_content = '\n\n'.join(reply_parts)
            success = safe_reply(message, reply_content)
            if not success:
                total_books = len(successful_books) + len(books_without_info)
                logger.warning(f"Reply failed but {total_books} orders processed successfully")
        
        # エラーがある場合は追加でエラーメッセージを送信
        if error_messages:
            # Rate Limit以外のエラーのみ表示
            non_rate_limit_errors = [err for err in error_messages if "Rate Limit" not in err]
            if non_rate_limit_errors:
                error_content = "以下のISBNで問題が発生しました：\n" + '\n'.join([f"・{err}" for err in non_rate_limit_errors[:3]])  # 最大3件まで表示
                safe_reply(message, error_content)

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
