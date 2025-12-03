import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd
import warnings

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

# --- KİMLİK KONTROLLERİ ---
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

# ==============================================================================
# 1. DÖVİZ & ALTIN (YAHOO FINANCE - GARANTİ)
# ==============================================================================
def get_doviz_altin_yahoo():
    print("1. Döviz ve Altın (Yahoo) çekiliyor...")
    
    # Hem Dövizleri hem Emtiaları (Altın, Gümüş, Platin) buradan alıyoruz
    semboller = [
        "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", 
        "JPYTRY=X", "AUDTRY=X", "CNYTRY=X", "RUBTRY=X",
        "EURUSD=X", "GBPUSD=X", "DX-Y.NYB",
        "XAUTRY=X", # Gram Altın
        "GC=F", "SI=F", "PL=F", "PA=F" # Ons, Gümüş, Platin, Paladyum
    ]
    
    data_doviz = {}
    data_altin = {}
    
    try:
        df = yf.download(semboller, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        if not df.empty:
            son = df.ffill().iloc[-1]
            for kod in semboller:
                try:
                    val = son.get(kod)
                    if pd.notna(val):
                        fiyat = round(float(val), 4)
                        
                        if kod == "XAUTRY=X": data_altin["Gram Altın"] = fiyat
                        elif kod == "GC=F": data_altin["Ons Altın"] = fiyat
                        elif kod == "SI=F": data_altin["Gümüş"] = fiyat
                        elif kod == "PL=F": data_altin["Platin"] = fiyat
                        elif kod == "PA=F": data_altin["Paladyum"] = fiyat
                        else:
                            key = kod.replace("TRY=X", "").replace("=X", "").replace(".NYB", "")
                            if key.endswith("TRY"): key = key.replace("TRY", "")
                            data_doviz[key] = fiyat
                except: continue
    except Exception as e:
        print(f"   -> ⚠️ Yahoo Hata: {e}")
        
    print(f"   -> ✅ Yahoo Bitti: {len(data_doviz)} Döviz, {len(data_altin)} Altın.")
    return data_doviz, data_altin

# ==============================================================================
# 2. BIST (TRADINGVIEW SCANNER)
# ==============================================================================
def get_bist_tradingview():
    print("2. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close"],
        "range": [0, 1000]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            # --- DÜZELTİLEN SATIR BURASI ---
            for h in r.json().get('data', []): 
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ BIST Başarılı: {len(data)} hisse.")
    except Exception as e:
        print(f"   -> ⚠️ BIST Hata: {e}")
    return data

# ==============================================================================
# 3. ABD BORSASI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_abd_tradingview():
    print("3. ABD Borsası (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "market_cap_basic"],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 600]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ ABD Başarılı: {len(data)} hisse.")
    except Exception as e:
        print(f"   -> ⚠️ ABD Hata: {e}")
    return data

# ==============================================================================
# 4. YATIRIM FONLARI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_fon_tradingview():
    print("4. Yatırım Fonları (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "equal", "right": "fund"}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close"],
        "range": [0, 2000]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            # --- DÜZELTİLEN SATIR BURASI ---
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ Fonlar Başarılı: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Fon Hata: {e}")
    return data

# ==============================================================================
# 5. KRİPTO (CMC API)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY:
        print("   -> ⚠️ CMC Key Yok.")
        return {}
    print(f"5. Kripto Piyasası (CMC Top {limit}) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            for coin in r.json()['data']:
                data[f"{coin['symbol']}-USD"] = round(float(coin['quote']['USD']['price']), 4)
            print(f"   -> ✅ CMC Başarılı: {len(data)} coin.")
    except: pass
    return data

# ==============================================================================
# KAYIT (SNAPSHOT MİMARİSİ)
# ==============================================================================
try:
    print("--- FİNANS BOTU (FULL STABLE) ---")
    
    # Döviz ve Altını al
    d_doviz, d_altin = get_doviz_altin_yahoo()

    final_paket = {
        "doviz_tl": d_doviz,
        "altin_tl": d_altin,
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "fon_tl": get_fon_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "timestamp": firestore.SERVER_TIMESTAMP
    }

    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        simdi = datetime.now()
        doc_id = simdi.strftime("%Y-%m-%d")
        saat = simdi.strftime("%H:%M")
        
        day_ref = db.collection(u'market_history').document(doc_id)
        day_ref.set({'date': doc_id}, merge=True)
        
        hour_ref = day_ref.collection(u'snapshots').document(saat)
        hour_ref.set(final_paket)
        
        total = sum(len(v) for k,v in final_paket.items() if isinstance(v, dict))
        print(f"🎉 BAŞARILI: [{doc_id} - {saat}] Toplam {total} veri kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
