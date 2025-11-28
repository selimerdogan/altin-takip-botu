import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd
import io
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
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# ==============================================================================
# 1. BIST (TRADINGVIEW SCANNER)
# ==============================================================================
def get_bist_tradingview():
    print("1. Borsa İstanbul (TradingView) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close"],
        "range": [0, 1000]
    }
    data_bist = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data_bist[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ TradingView Hisse: {len(data_bist)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ TV Hisse Hatası: {e}")
    return data_bist

# ==============================================================================
# 2. YATIRIM FONLARI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_fon_tradingview():
    print("2. Yatırım Fonları (TradingView) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "equal", "right": "fund"}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close"],
        "range": [0, 2000]
    }
    data_fon = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data_fon[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ TradingView Fon: {len(data_fon)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ TV Fon Bağlantı Hatası: {e}")
    return data_fon

# ==============================================================================
# 3. ABD BORSASI (GITHUB CSV + YAHOO)
# ==============================================================================
def get_sp500_dynamic():
    print("3. ABD Borsası (S&P 500 CSV) taranıyor...")
    url_csv = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
    data_abd = {}
    try:
        s = requests.get(url_csv).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        liste_sp500 = [x.replace('.', '-') for x in df['Symbol'].tolist()]
        
        black_list = ['WBA', 'DISCA', 'DISCK']
        liste_sp500 = [x for x in liste_sp500 if x not in black_list]

        df_yahoo = yf.download(liste_sp500, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        
        if not df_yahoo.empty:
            son = df_yahoo.ffill().iloc[-1]
            for sembol in liste_sp500:
                try:
                    val = son.get(sembol)
                    if pd.notna(val): data_abd[sembol] = round(float(val), 2)
                except: continue
        print(f"   -> ✅ S&P 500 Başarılı: {len(data_abd)} hisse.")
    except Exception as e:
        print(f"   -> ⚠️ ABD Hata: {e}")
    return data_abd

# ==============================================================================
# 4. KRİPTO (CMC API)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY:
        print("   -> ⚠️ CMC Key Yok.")
        return {}
    print(f"4. Kripto Piyasası (CMC Top {limit}) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data_kripto = {}
    try:
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 200:
            for coin in r.json()['data']:
                data_kripto[f"{coin['symbol']}-USD"] = round(float(coin['quote']['USD']['price']), 4)
            print(f"   -> ✅ CMC Başarılı: {len(data_kripto)} coin.")
    except: pass
    return data_kripto

# ==============================================================================
# 5. DÖVİZ & ALTIN
# ==============================================================================
def get_doviz_yahoo():
    print("5. Döviz Kurları çekiliyor...")
    liste = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X", "EURUSD=X", "GBPUSD=X", "JPY=X", "DX-Y.NYB"]
    data = {}
    try:
        df = yf.download(liste, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        if not df.empty:
            son = df.ffill().iloc[-1]
            for kur in liste:
                try:
                    val = son.get(kur)
                    if pd.notna(val): data[kur.replace("TRY=X", "").replace("=X", "")] = round(float(val), 4)
                except: continue
    except: pass
    print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    return data

def get_altin_site():
    print("6. Altın verileri çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.content, "html.parser")
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) > 2:
                isim = tds[0].get_text(strip=True)
                if "Ons" not in isim:
                    f = metni_sayiya_cevir(tds[2].get_text(strip=True))
                    if f > 0: data[isim] = f
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# KAYIT (SUBCOLLECTION MİMARİSİ - HATA ÖNLEYİCİ)
# ==============================================================================
try:
    print("--- FİNANS BOTU (SUBCOLLECTION MİMARİSİ) ---")
    
    final_paket = {
        "borsa_tr_tl": get_bist_tradingview(),
        "fon_tl": get_fon_tradingview(),
        "borsa_abd_usd": get_sp500_dynamic(),
        "kripto_usd": get_crypto_cmc(250),
        "doviz_tl": get_doviz_yahoo(),
        "altin_tl": get_altin_site(),
        "timestamp": firestore.SERVER_TIMESTAMP # Sunucu saati
    }

    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat = simdi.strftime("%H:%M")
        
        # 1. Ana Gün Dokümanı (Varlığını garantiye al)
        day_ref = db.collection(u'market_history').document(bugun_tarih)
        day_ref.set({'date': bugun_tarih}, merge=True)
        
        # 2. Saati ALT KOLEKSİYON olarak ekle (LIMITSİZ YAPI)
        # Yol: market_history -> 2025-11-28 -> snapshots -> 14:00
        hour_ref = day_ref.collection(u'snapshots').document(su_an_saat)
        hour_ref.set(final_paket)
        
        total = sum(len(v) for k,v in final_paket.items() if isinstance(v, dict))
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat}] Toplam {total} veri 'snapshots' altına kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
