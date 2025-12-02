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

def metni_sayiya_cevir(metin):
    try:
        return float(str(metin).replace(',', '.'))
    except:
        return 0.0

# ==============================================================================
# 1. DÖVİZ (BIGPARA API - JSON)
# ==============================================================================
def get_doviz_bigpara():
    """
    BigPara'nın resmi API'sinden döviz kurlarını çeker.
    HTML kazıma olmadığı için çok hızlı ve kararlıdır.
    """
    print("1. Döviz Kurları (BigPara API) taranıyor...")
    url = "https://bigpara.hurriyet.com.tr/api/v1/doviz/list"
    data = {}
    
    try:
        r = requests.get(url, headers=headers_general, timeout=20)
        if r.status_code == 200:
            items = r.json().get('data', [])
            # Önemli kurları filtreleyelim
            ONEMLI_KURLAR = ["USD", "EUR", "GBP", "CHF", "CAD", "JPY", "SAR", "AUD"]
            
            for item in items:
                kod = item.get('kod')
                fiyat = item.get('satis') # Satış fiyatını alıyoruz
                
                if kod in ONEMLI_KURLAR and fiyat:
                    data[kod] = float(fiyat)
            
            print(f"   -> ✅ Döviz Başarılı: {len(data)} adet.")
        else:
            print(f"   -> ⚠️ BigPara Hata: {r.status_code}")
    except Exception as e:
        print(f"   -> ⚠️ Döviz Bağlantı Hatası: {e}")
        
    return data

# ==============================================================================
# 2. ALTIN (BIGPARA API - JSON)
# ==============================================================================
def get_altin_bigpara():
    """
    Altın fiyatlarını da BigPara API'den çeker.
    """
    print("2. Altın Fiyatları (BigPara API) taranıyor...")
    url = "https://bigpara.hurriyet.com.tr/api/v1/altin/list"
    data = {}
    
    try:
        r = requests.get(url, headers=headers_general, timeout=20)
        if r.status_code == 200:
            items = r.json().get('data', [])
            for item in items:
                # Örn: 'GRAM ALTIN', 'ÇEYREK ALTIN'
                isim = item.get('ad').replace("i", "İ").upper() # Türkçe karakter düzeltmesi
                fiyat = item.get('satis')
                
                if isim and fiyat:
                    # İsimleri standart hale getirelim
                    if "GRAM" in isim: key = "Gram Altın"
                    elif "ÇEYREK" in isim: key = "Çeyrek Altın"
                    elif "YARIM" in isim: key = "Yarım Altın"
                    elif "TAM" in isim: key = "Tam Altın"
                    elif "CUMHURİYET" in isim: key = "Cumhuriyet A."
                    elif "ATA" in isim: key = "Ata Altın"
                    elif "ONS" in isim: key = "Ons Altın"
                    elif "22 AYAR" in isim: key = "22 Ayar Bilezik"
                    elif "14 AYAR" in isim: key = "14 Ayar Altın"
                    elif "18 AYAR" in isim: key = "18 Ayar Altın"
                    elif "GREMSE" in isim: key = "Gremse Altın"
                    elif "REŞAT" in isim: key = "Reşat Altın"
                    elif "HAMİT" in isim: key = "Hamit Altın"
                    elif "GÜMÜŞ" in isim: key = "Gümüş"
                    else: key = isim.title()
                    
                    data[key] = float(fiyat)
            
            print(f"   -> ✅ Altın Başarılı: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Altın Bağlantı Hatası: {e}")
        
    return data

# ==============================================================================
# 3. BIST (TRADINGVIEW SCANNER)
# ==============================================================================
def get_bist_tradingview():
    print("3. Borsa İstanbul (TV Scanner) taranıyor...")
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
# 5. ABD BORSASI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_abd_tradingview():
    print("5. ABD Borsası (TV Scanner) taranıyor...")
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
# 6. KRİPTO (CMC API)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY:
        print("   -> ⚠️ CMC Key Yok.")
        return {}
    print(f"6. Kripto Piyasası (CMC Top {limit}) taranıyor...")
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
    print("--- FİNANS BOTU (BIGPARA + TRADINGVIEW + CMC) ---")
    
    final_paket = {
        "doviz_tl": get_doviz_bigpara(),        # BigPara
        "altin_tl": get_altin_bigpara(),        # BigPara
        "borsa_tr_tl": get_bist_tradingview(),  # TradingView
        "fon_tl": get_fon_tradingview(),        # TradingView
        "borsa_abd_usd": get_abd_tradingview(), # TradingView
        "kripto_usd": get_crypto_cmc(250),      # CMC
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
