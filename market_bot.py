import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json
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
firebase_key_str = os.environ.get('FIREBASE_KEY')
CMC_API_KEY = os.environ.get('CMC_API_KEY')

if firebase_key_str:
    cred = credentials.Certificate(json.loads(firebase_key_str))
elif os.path.exists("serviceAccountKey.json"):
    cred = credentials.Certificate("serviceAccountKey.json")
else:
    print("HATA: Firebase anahtarı bulunamadı!")
    sys.exit(1)

try:
    if not firebase_admin._apps:
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
# 1. DÖVİZ (YAHOO - SAĞLAM İLK 10 + DEĞİŞİM ORANI)
# ==============================================================================
def get_doviz_yahoo():
    print("1. Top 10 Döviz (Yahoo) çekiliyor...")
    
    liste = [
        "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", 
        "JPYTRY=X", "AUDTRY=X", "EURUSD=X", "GBPUSD=X", "DX-Y.NYB"
    ]
    
    data = {}
    try:
        # Fiyat ve Değişim için son 2 günü alıyoruz
        df = yf.download(liste, period="5d", progress=False, threads=False, auto_adjust=True, ignore_tz=True)['Close']
        
        if not df.empty:
            df = df.ffill()
            bugun = df.iloc[-1]
            dun = df.iloc[-2] if len(df) > 1 else df.iloc[-1]

            for kod in liste:
                try:
                    val = bugun.get(kod)
                    val_prev = dun.get(kod)
                    
                    if pd.notna(val):
                        key = kod.replace("TRY=X", "").replace("=X", "").replace(".NYB", "")
                        if key.endswith("TRY"): key = key.replace("TRY", "")
                        
                        fiyat = float(val)
                        eski = float(val_prev)
                        degisim = ((fiyat - eski) / eski) * 100
                        
                        data[key] = {"price": round(fiyat, 4), "change": round(degisim, 2)}
                except: continue
                
        print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Yahoo Hata: {e}")
        
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - KAZIMA + DEĞİŞİM)
# ==============================================================================
def get_altin_site():
    print("2. Altın Fiyatları (Doviz.com) çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            table = soup.find("table")
            if table:
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) > 3:
                        try:
                            isim = tds[0].get_text(strip=True)
                            if "Ons" not in isim:
                                fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                                degisim_txt = tds[3].get_text(strip=True)
                                degisim = metni_sayiya_cevir(degisim_txt)
                                
                                if fiyat > 0: 
                                    data[isim] = {"price": fiyat, "change": degisim}
                        except: continue
    except Exception as e:
        print(f"   -> ⚠️ Altın Hata: {e}")
        
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# 3. BIST (TRADINGVIEW SCANNER + DEĞİŞİM)
# ==============================================================================
def get_bist_tradingview():
    print("3. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    
    # HATA VEREN KISIM DÜZELTİLDİ
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change"], # change = % değişim
        "range": [0, 1000]
    }
    
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code ==
