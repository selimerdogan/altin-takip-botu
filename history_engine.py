import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from tefas import Crawler
import sys
import os
import json
import time

def init_firebase():
    if firebase_admin._apps:
        return firestore.client()
    firebase_key_str = os.environ.get('FIREBASE_KEY')
    if firebase_key_str:
        cred = credentials.Certificate(json.loads(firebase_key_str))
    elif os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
    else:
        cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)
    return firestore.client()

def save_to_firebase_batch(db, data_list, collection_name="market_grafigi"):
    if not data_list:
        return 0

    batch = db.batch()
    counter = 0
    total = 0
    
    print(f"💾 {len(data_list)} adet veri Firebase'e yazılıyor...")
    
    for item in data_list:
        safe_symbol = item['symbol'].replace("/", "_")
        doc_id = f"{item['prefix']}_{safe_symbol}"
        doc_ref = db.collection(collection_name).document(doc_id)
        
        payload = {
            "symbol": item['symbol'],
            "type": item['type'],
            "last_updated": firestore.SERVER_TIMESTAMP,
            "period": "1y",
            "history": item['history']
        }
        
        batch.set(doc_ref, payload, merge=True)
        counter += 1
        total += 1
        
        if counter >= 400:
            batch.commit()
            batch = db.batch()
            counter = 0
            time.sleep(1)

    if counter > 0:
        batch.commit()

    print(f"   -> ✅ Toplam {total} adet grafik serisi güncellendi.")
    return total

def get_tefas_history(db):
    print("--- 1. TEFAS Fon Geçmişi Çekiliyor ---")
    crawler = Crawler()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    s_str = start_date.strftime("%Y-%m-%d")
    e_str = end_date.strftime("%Y-%m-%d")
    
    try:
        df = crawler.fetch(start=s_str, end=e_str, columns=["code", "date", "price"])
        if df is None or df.empty:
            print("   ⚠️ TEFAS verisi boş geldi.")
            return 0

        df['date'] = pd.to_datetime(df['date'])
        grouped = df.groupby('code')
        results = []
        
        for code, group in grouped:
            history_data = []
            for _, row in group.iterrows():
                history_data.append({
                    "d": row['date'].strftime("%Y-%m-%d"),
                    "c": float(row['price'])
                })
            
            results.append({
                "prefix": "FUND",
                "symbol": code,
                "type": "fund",
                "history": history_data
            })
            
        return save_to_firebase_batch(db, results)
    except Exception as e:
        print(f"   ❌ TEFAS Hatası: {e}")
        return 0

def process_yfinance_tickers(db, ticker_list, prefix, asset_type, suffix=""):
    if not ticker_list:
        return 0

    clean_tickers = []
    original_map = {}

    for t in ticker_list:
        if "/" in t:
            yahoo_symbol = t.replace("/", "-") 
        else:
            yahoo_symbol = t.replace(".", "-")

        full_symbol = f"{yahoo_symbol}{suffix}"
        clean_tickers.append(full_symbol)
        original_map[full_symbol] = t

    print(f"--- {prefix} ({len(clean_tickers)} Adet) Geçmiş Veri İndiriliyor ---")
    if not clean_tickers:
        return 0

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        data = yf.download(clean_tickers, start=start_date, end=end_date, interval="1d", group_by='ticker', progress=False, threads=False) 
        
        results = []
        if data.empty:
            return 0

        if len(clean_tickers) == 1:
            iterator = [(clean_tickers[0], data)]
        else:
            iterator = data.items()

        for symbol_raw, df_symbol in iterator:
            try:
                if df_symbol.empty: continue
                if 'Close' not in df_symbol.columns: continue
                df_clean = df_symbol.dropna(subset=['Close'])
                if df_clean.empty: continue
                
                history_data = []
                for date, row in df_clean.iterrows():
                    val = row['Close']
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    history_data.append({
                        "d": date.strftime("%Y-%m-%d"),
                        "c": round(float(val), 4)
                    })
                
                if not history_data: continue
                real_symbol_name = original_map.get(symbol_raw, symbol_raw.replace(suffix, ""))
                results.append({
                    "prefix": prefix,
                    "symbol": real_symbol_name, 
                    "type": asset_type,
                    "history": history_data
                })
            except Exception:
                continue

        return save_to_firebase_batch(db, results)
    except Exception as e:
        print(f"   ❌ YFinance Hatası ({prefix}): {e}")
        return 0

def execute_history_engine():
    db = init_firebase()
    print("--- 📈 VARIO GEÇMİŞ GRAFİK MOTORU BAŞLIYOR ---")
    start_time = time.time()
    
    tefas_count = get_tefas_history(db)
    
    crypto_count = process_yfinance_tickers(
        db, ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX"],
        prefix="CRYPTO", asset_type="crypto", suffix="-USD"
    )
    
    bist_ornek = ["THYAO", "GARAN", "AKBNK", "EREGL", "ASELS", "SISE", "KCHOL", "BIMAS", "TUPRS", "FROTO", "SAHOL"] 
    bist_count = process_yfinance_tickers(
        db, bist_ornek, prefix="BIST", asset_type="stock", suffix=".IS"
    )
    
    us_ornek = ["AAPL", "MSFT", "TSLA", "NVDA", "GOOGL", "AMZN", "META"]
    us_count = process_yfinance_tickers(
        db, us_ornek, prefix="US", asset_type="stock", suffix=""
    )
    
    fx_count = process_yfinance_tickers(
        db, ["TRY=X", "EURTRY=X", "GC=F"], prefix="FX", asset_type="currency", suffix=""
    )

    elapsed = round(time.time() - start_time, 2)
    return {
        "status": "success",
        "elapsed_seconds": elapsed,
        "updated": {
            "funds": tefas_count,
            "crypto": crypto_count,
            "bist": bist_count,
            "us": us_count,
            "fx": fx_count
        }
    }
