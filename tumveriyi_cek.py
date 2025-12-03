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

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
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
# 1. DÖVİZ (EN POPÜLER 10 - YAHOO)
# ==============================================================================
def get_doviz_top10():
    print("1. Top 10 Döviz Kuru (TL) çekiliyor...")
    
    # Türkiye'de en çok işlem gören 10 para birimi
    liste = [
        "USDTRY=X", # Dolar
        "EURTRY=X", # Euro
        "GBPTRY=X", # Sterlin
        "CHFTRY=X", # İsviçre Frangı
        "CADTRY=X", # Kanada Doları
        "JPYTRY=X", # Japon Yeni
        "AUDTRY=X", # Avustralya Doları
        "SEKTRY=X", # İsveç Kronu
        "DKKTRY=X", # Danimarka Kronu
        "SARTRY=X", # Suudi Riyali (Yahoo bazen vermeyebilir, gelmezse sorun değil)
        "NOKTRY=X"  # Norveç Kronu (Yedek)
    ]
    
    data = {}
    try:
        # threads=False: Veritabanı kilidini önler
        df = yf.download(liste, period="5d", progress=False, threads=False, auto_adjust=True, ignore_tz=True)['Close']
        
        if not df.empty:
            son = df.ffill().iloc[-1]
            for kod in liste:
                try:
                    val = son.get(kod)
                    if pd.notna(val):
                        # İsim temizliği: USDTRY=X -> USD
                        key = kod.replace("TRY=X", "").replace("=X", "")
                        
                        # Sadece ilk 10 taneyi alalım (Liste sırasına göre)
                        if len(data) < 10:
                            data[key] = round(float(val), 4)
                except: continue
                
        print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Döviz Hata: {e}")
        
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - KAZIMA)
# ==============================================================================
def get_altin_site():
    print("2. Altın Fiyatları (Doviz.com) çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) > 2:
                    try:
                        isim = tds[0].get_text(strip=True)
                        # Ons Altın genellikle Dolar'dır, ama listede olsun.
                        # TL olanlar (Gram, Çeyrek vs.) öncelikli.
                        fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                        if fiyat > 0: data[isim] = fiyat
                    except: continue
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
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
                    if len(d) > 1: data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ BIST Başarılı: {len(data)} hisse.")
    except: pass
    return data

# ==============================================================================
# 4. ABD BORSASI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_abd_tradingview():
    print("4. ABD Borsası (TV Scanner) taranıyor...")
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
                    if len(d) > 1: data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ ABD Başarılı: {len(data)} hisse.")
    except: pass
    return data

# ==============================================================================
# 5. YATIRIM FONLARI (TRADINGVIEW SCANNER)
# ==============================================================================
def get_fon_tradingview():
    print("5. Yatırım Fonları (TV Scanner) taranıyor...")
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
                    if len(d) >
