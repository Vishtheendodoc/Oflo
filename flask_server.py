from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
from datetime import datetime
from orderflow import OrderFlowAnalyzer, live_market_data, orderflow_history
import csv
from collections import defaultdict
import math
from dhanhq import DhanContext, MarketFeed
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import os

# --- CONFIG ---
API_BATCH_SIZE = 5          # Number of stocks per batch API call
BATCH_INTERVAL_SEC = 5      # Wait time between batches
RESET_TIME = "09:15"        # Clear delta_history daily at this time
STOCK_LIST_FILE = "stock_list.csv"
POSTGRES_URL = os.environ.get("DATABASE_URL", "postgres://user:password@host:5432/dbname")  # Replace fallback with your DB URL

# --- FLASK SETUP ---
app = Flask(__name__)
CORS(app)

# Global variables
analyzer = None
delta_history = defaultdict(lambda: defaultdict(lambda: {'buy': 0, 'sell': 0}))
last_reset_date = [None]  # For daily reset

# --- Initialize Dhan API Analyzer ---
CLIENT_ID = "1100244268"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzUzODUzOTQxLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDI0NDI2OCJ9.DsXutZuv9qIh4MNpMrfjIccyWg_-nwR9t5ldK0H14aGY23U7IaFW6cTmXp1MAr8zjk7eAIYNe-SVLv19Skw4MQ"
analyzer = OrderFlowAnalyzer(CLIENT_ID, ACCESS_TOKEN)

# PostgreSQL connection helper
def get_db_connection():
    return psycopg2.connect(POSTGRES_URL, cursor_factory=RealDictCursor)

# Prepare instrument list from your stock_list.csv
def get_instrument_list():
    stocks = []
    with open(STOCK_LIST_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            exch = row.get("exchange")
            seg = row.get("segment")
            instr = row.get("instrument")
            sec_id = str(row["security_id"])
            if exch == "NSE" and seg == "D":
                stocks.append((MarketFeed.NSE_FNO, sec_id, MarketFeed.Quote))
            elif exch == "MCX" and seg == "M":
                stocks.append(("MCX_COMM", sec_id, MarketFeed.Quote))
    return stocks

instrument_list = get_instrument_list()
dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
market_feed = MarketFeed(dhan_context, instrument_list, "v2")

# Initialize PostgreSQL table
def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS orderflow (
                    id SERIAL PRIMARY KEY,
                    security_id TEXT,
                    timestamp TIMESTAMP,
                    buy_volume REAL,
                    sell_volume REAL,
                    ltp REAL,
                    volume REAL
                )
            ''')
            conn.commit()
    print("✅ PostgreSQL table initialized")

init_db()

# Store quote data in PostgreSQL
def store_in_db(security_id, timestamp, buy, sell, ltp, volume):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO orderflow (security_id, timestamp, buy_volume, sell_volume, ltp, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (security_id, timestamp, buy, sell, ltp, volume))
                conn.commit()
    except Exception as e:
        print(f"[DB ERROR] Failed to insert data: {e}")

def marketfeed_thread():
    while True:
        try:
            market_feed.run_forever()
            while True:
                response = market_feed.get_data()
                if response and isinstance(response, dict):
                    security_id = str(response.get("security_id"))
                    if security_id:
                        live_market_data[security_id] = response
                        if response.get('type') == 'Quote Data':
                            ltt = response.get("LTT")
                            today = datetime.now().strftime('%Y-%m-%d')
                            timestamp = f"{today} {ltt}" if ltt else datetime.now().isoformat()
                            buy = response.get("total_buy_quantity", 0)
                            sell = response.get("total_sell_quantity", 0)
                            ltp = response.get("LTP", 0)
                            volume = response.get("volume", 0)
                            store_in_db(security_id, timestamp, buy, sell, ltp, volume)
                time.sleep(0.01)
        except Exception as e:
            print(f"[ERROR] Marketfeed thread crashed: {e}. Restarting...")
            time.sleep(2)

threading.Thread(target=marketfeed_thread, daemon=True).start()

def maybe_reset_history():
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    if last_reset_date[0] != today_str and now.strftime('%H:%M') >= RESET_TIME:
        delta_history.clear()
        last_reset_date[0] = today_str
        print(f"🧹 Cleared delta history at {now.strftime('%H:%M:%S')}")

def load_stock_list():
    stocks = []
    with open(STOCK_LIST_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            stocks.append((row["security_id"], row["symbol"]))
    print(f"📃 Loaded {len(stocks)} stocks from {STOCK_LIST_FILE}")
    return stocks

@app.route('/api/delta_data/<string:security_id>')
def get_delta_data(security_id):
    interval = int(request.args.get('interval', 5))
    try:
        with get_db_connection() as conn:
            df = pd.read_sql(
                "SELECT * FROM orderflow WHERE security_id = %s ORDER BY timestamp ASC",
                conn, params=(security_id,)
            )
        if df.empty:
            return jsonify([])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        buckets = []
        grouped = df.groupby(pd.Grouper(key='timestamp', freq=f'{interval}min'))
        for bucket_label, group in grouped:
            if not group.empty:
                buy_delta = group['buy_volume'].sum()
                sell_delta = group['sell_volume'].sum()
                delta = buy_delta - sell_delta
                buckets.append({
                    'timestamp': bucket_label.strftime('%H:%M'),
                    'buy_volume': buy_delta,
                    'sell_volume': sell_delta,
                    'delta': delta
                })
        return jsonify(buckets)
    except Exception as e:
        print(f"[API ERROR] Failed to fetch delta data: {e}")
        return jsonify([])

@app.route('/api/stocks')
def get_stock_list():
    try:
        stocks = load_stock_list()
        return jsonify([{"security_id": sid, "symbol": sym} for sid, sym in stocks])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/live_data/<string:security_id>')
def get_live_data(security_id):
    data = live_market_data.get(security_id)
    if data:
        return jsonify(data)
    else:
        return jsonify({"error": "No live data"}), 404

@app.route('/api/orderflow_history/<string:security_id>')
def get_orderflow_history(security_id):
    data = orderflow_history.get(security_id, [])
    return jsonify(data)

@app.route('/')
def dashboard():
    return "<h1>Order Flow Flask Server Running</h1>"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
