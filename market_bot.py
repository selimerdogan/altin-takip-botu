import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd
import warnings
from bs4 import BeautifulSoup

warnings.simplefilter(action='ignore', category=FutureWarning)

headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

# Güvenlik Kontrolü: Dosya yoksa durdur.
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json bulunamadı!")
    sys.exit(1)

CMC_API_KEY = os.environ.get('CMC_API_KEY')

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase hatası: {e}")
    sys.exit(1)

def metni_sayiya_cevir(metin):
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# --- MODÜLLER ---

def get_doviz_yahoo():
    print("1. Döviz (Yahoo) çekiliyor...")
    liste = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X", "EURUSD=X", "GBPUSD=X", "DX-Y.NYB"]
    data = {}
    try:
        df = yf.download(liste, period="5d", progress=False, threads=False, auto_adjust=True, ignore_tz=True)['Close']
        if not df.empty:
            son = df.ffill().iloc[-1]
            for k in liste:
                try:
                    val = son.get(k)
                    if pd.notna(val):
                        key = k.replace("TRY=X", "").replace("=X", "").replace(".NYB", "")
                        if key.endswith("TRY"): key = key.replace("TRY", "")
                        data[key] = round(float(val), 4)
                except: continue
    except: pass
    return data

def get_altin_site():
    print("2. Altın (Doviz.com) çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        soup = BeautifulSoup(r.content, "html.parser")
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) > 2:
                isim = tds[0].get_text(strip=True)
                if "Ons" not in isim:
                    f = metni_sayiya_cevir(tds[2].get_text(strip=True))
                    if f > 0: data[isim] = f
    except: pass
    return data

def get_bist_tradingview():
    print("3. BIST (TV Scanner) çekiliyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}], "options": {"lang": "tr"}, "symbols": {"query": {"types": []}, "tickers": []}, "columns": ["name", "close"], "range": [0, 1000]}
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                d = h.get('d', [])
                if len(d) > 1: data[d[0]] = float(d[1])
    except: pass
    return data

def get_abd_tradingview():
    print("4. ABD (TV Scanner) çekiliyor...")
    url = "https://scanner.tradingview.com/america/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}], "options": {"lang": "en"}, "symbols": {"query": {"types": []}, "tickers": []}, "columns": ["name", "close", "market_cap_basic"], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}, "range": [0, 600]}
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                d = h.get('d', [])
                if len(d) > 1: data[d[0]] = float(d[1])
    except: pass
    return data

def get_crypto_cmc(limit=250):
    if not CMC_API_KEY: return {}
    print("5. Kripto (CMC) çekiliyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            for coin in r.json()['data']:
                data[f"{coin['symbol']}-USD"] = round(float(coin['quote']['USD']['price']), 4)
    except: pass
    return data

# --- KAYIT ---
try:
    print("--- PİYASA BOTU (HAFTA İÇİ) ---")
    
    final_paket = {
        "doviz_tl": get_doviz_yahoo(),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "timestamp": firestore.SERVER_TIMESTAMP
    }

    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        simdi = datetime.now()
        doc_id = simdi.strftime("%Y-%m-%d")
        saat = simdi.strftime("%H:%M")
        
        day_ref = db.collection(u'market_history').document(doc_id)
        day_ref.set({'date': doc_id}, merge=True)
        day_ref.collection(u'snapshots').document(saat).set(final_paket)
        
        print(f"🎉 [{saat}] Piyasa verileri başarıyla kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"HATA: {e}")
    sys.exit(1)
