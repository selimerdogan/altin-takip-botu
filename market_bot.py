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
# 1. DÖVİZ (YAHOO - SADECE TL - FİYAT + DEĞİŞİM)
# ==============================================================================
def get_doviz_yahoo():
    print("1. Döviz Kurları (Fiyat + Değişim) çekiliyor...")
    
    # SADECE TL PARALAR (Parite Yok)
    liste = [
        "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", 
        "JPYTRY=X", "AUDTRY=X", "SARTRY=X", "DKKTRY=X", "SEKTRY=X", "NOKTRY=X"
    ]
    
    data = {}
    try:
        # Son 5 günü çekiyoruz ki bugünkü değişimi hesaplayabilelim
        df = yf.download(liste, period="5d", progress=False, threads=False, auto_adjust=True, ignore_tz=True)['Close']
        
        if not df.empty:
            # Son gün ve bir önceki gün
            df_dolu = df.ffill()
            bugun = df_dolu.iloc[-1]
            dun = df_dolu.iloc[-2]
            
            for kod in liste:
                try:
                    fiyat_bugun = bugun.get(kod)
                    fiyat_dun = dun.get(kod)
                    
                    if pd.notna(fiyat_bugun) and pd.notna(fiyat_dun):
                        val_now = float(fiyat_bugun)
                        val_prev = float(fiyat_dun)
                        
                        # Yüzde Değişim Hesabı
                        degisim = ((val_now - val_prev) / val_prev) * 100
                        
                        # İsim Temizliği
                        key = kod.replace("TRY=X", "").replace("=X", "")
                        if key.endswith("TRY"): key = key.replace("TRY", "")
                        
                        # YENİ YAPI: { price: 34.5, change: 0.15 }
                        data[key] = {
                            "price": round(val_now, 4),
                            "change": round(degisim, 2)
                        }
                except: continue
                
        print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Yahoo Hata: {e}")
        
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - FİYAT + DEĞİŞİM)
# ==============================================================================
def get_altin_site():
    print("2. Altın Verileri (Fiyat + Değişim) çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            # Tabloyu bul
            table = soup.find('table')
            if table:
                rows = table.find_all("tr")
                for tr in rows:
                    tds = tr.find_all("td")
                    # Yapı: İsim | Alış | Satış | % Değişim | Saat
                    if len(tds) > 3:
                        try:
                            isim = tds[0].get_text(strip=True)
                            if "Ons" not in isim:
                                fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                                degisim = metni_sayiya_cevir(tds[3].get_text(strip=True))
                                
                                if fiyat > 0: 
                                    data[isim] = {"price": fiyat, "change": degisim}
                        except: continue
    except Exception as e:
        print(f"   -> ⚠️ Altın Hata: {e}")
        
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# 3. TRADINGVIEW FONKSİYONU (ORTAK - FİYAT + DEĞİŞİM)
# ==============================================================================
def fetch_tradingview(market_type, range_limit=1000):
    """BIST, ABD ve FONLAR için ortak fonksiyon. Fiyat ve Değişim çeker."""
    
    if market_type == "bist":
        url = "https://scanner.tradingview.com/turkey/scan"
        filters = [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}]
        lang = "tr"
    elif market_type == "abd":
        url = "https://scanner.tradingview.com/america/scan"
        filters = [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}]
        lang = "en"
    elif market_type == "fon":
        url = "https://scanner.tradingview.com/turkey/scan"
        filters = [{"left": "type", "operation": "equal", "right": "fund"}]
        lang = "tr"

    # "change" sütununu ekliyoruz (% Değişim)
    payload = {
        "filter": filters,
        "options": {"lang": lang},
        "symbols": {"query": {"types": []}, "
