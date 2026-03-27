import os
import requests
import duckdb
import pandas as pd
import time
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

BACKFILL_START_DATE = datetime(2025, 1, 1)
TODAY_DATE = datetime.utcnow().date()

DB_NAME = "crypto_project"
TOKEN = os.getenv("MOTHERDUCK_TOKEN")
AV_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY") 
CONN_STR = f"md:{DB_NAME}?motherduck_token={TOKEN}"

# Standard Browser Headers to bypass simple bot filters
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# BINANCE WATERFALL SETUP
BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision/api/v3",  # Best for GitHub Actions (Public Data)
    "https://api.binance.us/api/v3",           # Fallback (Works in US data centers)
    "https://api3.binance.com/api/v3",
    "https://api-g.binance.com/api/v3"
]

def get_working_endpoint():
    """Tests multiple Binance mirrors to bypass regional/cloud geo-blocks."""
    logger.info("Testing Binance endpoints to bypass geo-blocks...")
    
    for url in BINANCE_ENDPOINTS:
        try:
            r = requests.get(f"{url}/ping", headers=HEADERS, timeout=10)
            if r.status_code == 200:
                logger.info(f"Connected successfully to: {url}")
                return url
        except Exception as e:
            logger.warning(f"Endpoint {url} failed: {e}")
            continue

    logger.error("All endpoints failed. Defaulting to Vision API.")
    return "https://data-api.binance.vision/api/v3"

# GLOBAL URL
BINANCE_URL = get_working_endpoint()
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# --- HELPER FUNCTIONS ---

def get_valid_binance_symbols():
    """Fetches symbols currently trading on the selected Binance mirror."""
    try:
        response = requests.get(f"{BINANCE_URL}/exchangeInfo", headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        symbols = {s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
        logger.info(f"Validated {len(symbols)} USDT pairs on {BINANCE_URL}.")
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch exchange info from {BINANCE_URL}: {e}")
        return set()

def update_target_coins(con, valid_binance_symbols):
    if not valid_binance_symbols:
        logger.warning("No Binance symbols available. Skipping Top 10 update.")
        return

    con.execute("""
        CREATE TABLE IF NOT EXISTS target_coins (
            coin_id VARCHAR, name VARCHAR, symbol VARCHAR, 
            binance_symbol VARCHAR PRIMARY KEY, rank INTEGER, updated_at TIMESTAMP
        );
    """)

    res = con.execute("SELECT MAX(updated_at) FROM target_coins").fetchone()
    if res[0] and (datetime.utcnow() - res[0]) < timedelta(hours=23):
        logger.info("Target coins recently updated. Skipping.")
        return

    logger.info("Fetching fresh Top 10 list from CoinGecko...")
    params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 25, 'page': 1}
    
    try:
        res = requests.get(COINGECKO_URL, params=params, timeout=15)
        res.raise_for_status()
        coins_json = res.json()
        
        targets = []
        for coin in coins_json:
            bsym = f"{coin['symbol'].upper()}USDT"
            if bsym in valid_binance_symbols:
                targets.append({
                    'coin_id': coin['id'], 'name': coin['name'], 'symbol': coin['symbol'].upper(),
                    'binance_symbol': bsym, 'rank': coin['market_cap_rank'], 'image_url': coin['image'], 'updated_at': datetime.utcnow()
                })
        
        if targets:
            df = pd.DataFrame(targets[:10])
            con.execute("INSERT OR REPLACE INTO target_coins SELECT * FROM df")
            logger.info(f"Top 10 tracked symbols: {df['symbol'].tolist()}")
    except Exception as e:
        logger.error(f"CoinGecko fetch failed: {e}")

def update_fx_rates(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_fx_rates (
            date DATE PRIMARY KEY, base_currency VARCHAR, target_currency VARCHAR, rate DOUBLE
        );
    """)
    
    res = con.execute("SELECT COUNT(*) FROM raw_fx_rates WHERE date = ?", [TODAY_DATE]).fetchone()
    if res[0] > 0:
        logger.info(f"FX Rate for {TODAY_DATE} is already in the database.")
    else:
        logger.info(f"FX Rate for {TODAY_DATE} missing. Calling AlphaVantage...")
        try:
            av_url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency=NGN&apikey={AV_API_KEY}"
            res_av = requests.get(av_url, timeout=15)
            data = res_av.json()
            rate_data = data.get("Realtime Currency Exchange Rate")
            if rate_data:
                rate = float(rate_data.get("5. Exchange Rate"))
                con.execute("INSERT OR REPLACE INTO raw_fx_rates VALUES (?, 'USD', 'NGN', ?)", [TODAY_DATE, rate])
                logger.info(f"Saved AlphaVantage FX: 1 USD = {rate} NGN")
            else:
                logger.warning(f"AlphaVantage error/limit: {data}")
        except Exception as e:
            logger.error(f"AlphaVantage Error: {e}")

    # History Sync
    all_history = con.execute("SELECT date FROM raw_fx_rates").fetchall()
    existing_dates = {r[0] for r in all_history}
    num_days = (TODAY_DATE - BACKFILL_START_DATE.date()).days
    missing = [BACKFILL_START_DATE.date() + timedelta(days=i) for i in range(num_days) 
               if BACKFILL_START_DATE.date() + timedelta(days=i) not in existing_dates]

    if missing:
        logger.info(f"Backfilling {len(missing)} FX dates from archive...")
        for d in missing:
            date_str = d.strftime('%Y-%m-%d')
            url = f"https://{date_str}.currency-api.pages.dev/v1/currencies/usd.json"
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    val = r.json().get('usd', {}).get('ngn')
                    if val:
                        con.execute("INSERT OR REPLACE INTO raw_fx_rates VALUES (?, 'USD', 'NGN', ?)", [d, float(val)])
            except: continue

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_binance_klines(symbol, start_ts):
    params = {'symbol': symbol, 'interval': '1h', 'startTime': start_ts, 'limit': 1000}
    res = requests.get(f"{BINANCE_URL}/klines", params=params, headers=HEADERS, timeout=15)
    res.raise_for_status()
    return res.json()

def process_single_symbol(symbol):
    thread_con = duckdb.connect(CONN_STR)
    try:
        res = thread_con.execute("SELECT MAX(open_time) FROM raw_klines WHERE symbol = ?", [symbol]).fetchone()
        start_ts = int(res[0].timestamp() * 1000) + 1 if res[0] else int(BACKFILL_START_DATE.timestamp() * 1000)

        data = fetch_binance_klines(symbol, start_ts)
        if not data: return 0
        
        df = pd.DataFrame(data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'q_vol', 'trades', 'tb_base', 'tb_quote', 'ignore'
        ])
        
        df['symbol'] = symbol
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])
        
        df_clean = df[['symbol', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']]
        thread_con.execute("INSERT OR REPLACE INTO raw_klines SELECT * FROM df_clean")
        return len(df)
    finally:
        thread_con.close()

def main():
    logger.info(f"=== Starting Pipeline Run | Mirror: {BINANCE_URL} ===")
    con = duckdb.connect(CONN_STR)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_klines (
            symbol VARCHAR, open_time TIMESTAMP, open DOUBLE, high DOUBLE, 
            low DOUBLE, close DOUBLE, volume DOUBLE, close_time TIMESTAMP,
            PRIMARY KEY (symbol, open_time)
        );
    """)

    try:
        valid_symbols = get_valid_binance_symbols()
        if not valid_symbols:
            logger.error("No connectivity to Binance mirrors. Aborting.")
            return

        update_target_coins(con, valid_symbols)
        update_fx_rates(con)
        
        target_df = con.execute("SELECT binance_symbol FROM target_coins").fetchdf()
        symbols = target_df['binance_symbol'].tolist()
        
        if not symbols:
            logger.warning("Target coins table empty.")
            return

        logger.info(f"Updating {len(symbols)} coins incrementally...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_single_symbol, s): s for s in symbols}
            for future in as_completed(futures):
                s = futures[future]
                try:
                    count = future.result()
                    logger.info(f"Completed {s}: {count} new rows.")
                except Exception as e:
                    logger.error(f"Error syncing {s}: {e}")

    except Exception as e:
        logger.error(f"Main Pipeline Failure: {e}")
        traceback.print_exc()
    finally:
        con.close()
        logger.info("=== Pipeline Run Finished ===")

if __name__ == "__main__":
    main()