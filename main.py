# Rate Limit 緊急修正版コード (main.pyの該当部分)

import random
import time
from datetime import datetime, timedelta

# Rate Limit対策の設定
RATE_LIMIT_DETECTED = False
RATE_LIMIT_START_TIME = None
MIN_WAIT_MINUTES = 30  # 最低30分待機
MAX_WAIT_MINUTES = 120  # 最大2時間待機

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

def get_server_ip():
    """サーバーIPを取得"""
    try:
        import requests
        response = requests.get('https://api.ipify.org')
        return response.text
    except:
        return "Unknown"

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
