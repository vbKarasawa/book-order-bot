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
creds = ServiceAccountCredentials.from_json_k
