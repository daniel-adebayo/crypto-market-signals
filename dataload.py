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

# CONFIG
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

# Dates
BACKFILL_START_DATE = datetime(2025, 1, 1)
TODAY_DATE = datetime.utcnow().date()

DB_NAME = "crypto_project"
TOKEN = os.getenv("MOTHERDUCK_TOKEN")
AV_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY") 
CONN_STR = f"md:{DB_NAME}?motherduck_token={TOKEN}"

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_URL = "https://api.binance.com/api/v3"

def get_valid_binance_symbols():
    """Fetches symbols currently trading on Binance to avoid mapping errors."""
    try:
        response = requests.get(f"{BINANCE_URL}/exchangeInfo", timeout=10)
        response.raise_for_status()
        data = response.json()
        return {s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT'}
    except Exception as e:
        logger.error(f"Failed to fetch Binance exchange info: {e}")
        return set()

def update_target_coins(con, valid_binance_symbols):
    con.execute("""
        CREATE TABLE IF NOT EXISTS target_coins (
            coin_id VARCHAR, name VARCHAR, symbol VARCHAR, 
            binance_symbol VARCHAR PRIMARY KEY, rank INTEGER, updated_at TIMESTAMP
        );
    """)

    result = con.execute("SELECT MAX(updated_at) FROM target_coins").fetchone()
    if result[0] and (datetime.utcnow() - result[0]) < timedelta(hours=24):
        logger.info("Gate 1 [Coins]: Updated within 24h. Skipping CoinGecko.")
        return

    logger.info("Gate 2 [Coins]: Data stale. Fetching from CoinGecko...")
    params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 15, 'page': 1}
    
    try:
        res = requests.get(COINGECKO_URL, params=params, timeout=10)
        res.raise_for_status()
        targets = []
        for coin in res.json():
            bsym = f"{coin['symbol'].upper()}USDT"
            if bsym in valid_binance_symbols:
                targets.append({
                    'coin_id': coin['id'], 'name': coin['name'], 'symbol': coin['symbol'].upper(),
                    'binance_symbol': bsym, 'rank': coin['market_cap_rank'], 'updated_at': datetime.utcnow()
                })
        
        if targets:
            df = pd.DataFrame(targets[:10])
            con.execute("INSERT OR REPLACE INTO target_coins SELECT * FROM df")
            logger.info(f"Top 10 Updated: {', '.join(df['symbol'].tolist())}")
    except Exception as e:
        logger.error(f"CoinGecko update failed: {e}")

def update_fx_rates(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_fx_rates (
            date DATE PRIMARY KEY, base_currency VARCHAR, target_currency VARCHAR, rate DOUBLE
        );
    """)
    
    res = con.execute("SELECT MAX(date) FROM raw_fx_rates").fetchone()
    last_db_date = res[0] if res[0] else None

    if last_db_date == TODAY_DATE:
        logger.info(f"Gate 1 [FX]: Rate for {TODAY_DATE} already in DB. Skipping API calls.")
        return

    num_days = (TODAY_DATE - BACKFILL_START_DATE.date()).days + 1
    required_dates = [BACKFILL_START_DATE.date() + timedelta(days=x) for x in range(num_days)]
    
    existing_rows = con.execute("SELECT date FROM raw_fx_rates").fetchall()
    existing_set = {r[0] for r in existing_rows}
    
    missing_dates = [d for d in required_dates if d not in existing_set]

    if not missing_dates:
        return

    logger.info(f"Gate 2 [FX]: Syncing {len(missing_dates)} missing dates.")
    
    for d in missing_dates:
        date_str = d.strftime('%Y-%m-%d')

        if d < TODAY_DATE:
            url = f"https://{date_str}.currency-api.pages.dev/v1/currencies/usd.json"
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    rate = r.json().get('usd', {}).get('ngn')
                    if rate:
                        con.execute("INSERT OR REPLACE INTO raw_fx_rates VALUES (?, 'USD', 'NGN', ?)", (d, float(rate)))
                        logger.info(f"Archive Sync: Found history for {date_str}")
            except: continue
        
        else:
            url = f"https://{date_str}.currency-api.pages.dev/v1/currencies/usd.json"
            found_today = False
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    rate = r.json().get('usd', {}).get('ngn')
                    if rate:
                        con.execute("INSERT OR REPLACE INTO raw_fx_rates VALUES (?, 'USD', 'NGN', ?)", (d, float(rate)))
                        logger.info(f"Archive Success: Found TODAY ({date_str}) in archive.")
                        found_today = True
            except: pass

            if not found_today:
                logger.info(f"Gate 3 [FX]: Today not in Archive. Calling AlphaVantage...")
                try:
                    av_url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency=NGN&apikey={AV_API_KEY}"
                    res_av = requests.get(av_url, timeout=10)
                    data = res_av.json()
                    rate_data = data.get("Realtime Currency Exchange Rate")
                    if rate_data:
                        rate = float(rate_data.get("5. Exchange Rate"))
                        con.execute("INSERT OR REPLACE INTO raw_fx_rates VALUES (?, 'USD', 'NGN', ?)", (d, rate))
                        logger.info(f"AlphaVantage Success: 1 USD = {rate} NGN")
                    else:
                        logger.warning(f"AlphaVantage Limit/Error: {data}")
                except Exception as e:
                    logger.error(f"AlphaVantage Connection Error: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_binance_klines(symbol, start_ts):
    params = {'symbol': symbol, 'interval': '1h', 'startTime': start_ts, 'limit': 1000}
    res = requests.get(f"{BINANCE_URL}/klines", params=params, timeout=10)
    res.raise_for_status()
    return res.json()

def process_single_symbol(symbol):
    """Incremental fetch for crypto prices based on last close_time."""
    thread_con = duckdb.connect(CONN_STR)
    try:
        res = thread_con.execute(f"SELECT MAX(open_time) FROM raw_klines WHERE symbol = '{symbol}'").fetchone()
        start_ts = int(res[0].timestamp() * 1000) + 1 if res[0] else int(BACKFILL_START_DATE.timestamp() * 1000)

        total_rows = 0
        while True:
            data = fetch_binance_klines(symbol, start_ts)
            if not data: break
            
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 
                'close_time', 'q_vol', 'trades', 'tb_base', 'tb_quote', 'ignore'
            ])
            
            df['symbol'] = symbol
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            
            df_to_load = df[['symbol', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']]
            thread_con.execute("INSERT OR REPLACE INTO raw_klines SELECT * FROM df_to_load")
            
            batch_size = len(df)
            total_rows += batch_size
            if batch_size < 1000: break
            
            start_ts = int(data[-1][0]) + 1
            if total_rows > 35000: break 

        return total_rows
    finally:
        thread_con.close()

def main():
    logger.info("=== Starting ELT Pipeline ===")
    con = duckdb.connect(CONN_STR)
    
    # Init Table
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_klines (
            symbol VARCHAR, open_time TIMESTAMP, open DOUBLE, high DOUBLE, 
            low DOUBLE, close DOUBLE, volume DOUBLE, close_time TIMESTAMP,
            PRIMARY KEY (symbol, open_time)
        );
    """)

    try:
        valid_symbols = get_valid_binance_symbols()
        update_target_coins(con, valid_symbols)
        update_fx_rates(con)
        targets = con.execute("SELECT binance_symbol FROM target_coins").fetchdf()
        symbol_list = targets['binance_symbol'].tolist()
        
        if not symbol_list:
            logger.warning("No tracking symbols found.")
            return

        logger.info(f"Syncing {len(symbol_list)} symbols incrementally...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_single_symbol, s): s for s in symbol_list}
            for future in as_completed(futures):
                s = futures[future]
                try:
                    count = future.result()
                    logger.info(f"Finished {s}: {count} new rows added.")
                except Exception as e:
                    logger.error(f"Error in {s}: {e}")

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        traceback.print_exc()
    finally:
        con.close()
        logger.info("=== Pipeline Run Finished ===")

if __name__ == "__main__":
    main()