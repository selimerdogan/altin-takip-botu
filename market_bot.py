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

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

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
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('%', '').strip()
        if "," in temiz: temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except: return 0.0

# ==============================================================================
# 1. DÖVİZ (YAHOO - FİYAT + DEĞİŞİM HESABI)
# ==============================================================================
def get_doviz_yahoo():
    print("1. Döviz Kurları ve Değişimleri çekiliyor...")
    # Sadece TL Dövizler (Parite Yok)
    liste = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X"]
    data = {}
    try:
        # Son 2 günün verisini al ki değişim hesaplayalım
        df = yf.download(liste, period="5d", progress=False, auto_adjust=True)['Close']
        
        if not df.empty:
            df = df.ffill() # Boşlukları doldur
            son_fiyatlar = df.iloc[-1]
            
            # Bir önceki kapanışı bul (Değişim hesabı için)
            # Eğer 5 günlük veri varsa, sondan bir önceki
            onceki_fiyatlar = df.iloc[-2] if len(df) > 1 else df.iloc[-1]

            for kod in liste:
                try:
                    fiyat = float(son_fiyatlar.get(kod))
                    eski_fiyat = float(onceki_fiyatlar.get(kod))
                    
                    if fiyat > 0:
                        # Yüzde Değişim Hesabı: ((Yeni - Eski) / Eski) * 100
                        degisim = ((fiyat - eski_fiyat) / eski_fiyat) * 100
                        
                        key = kod.replace("TRY=X", "").replace("=X", "")
                        
                        # YENİ YAPI: { "fiyat": 34.5, "degisim": 0.12 }
                        data[key] = {"fiyat": round(fiyat, 4), "degisim": round(degisim, 2)}
                except: continue
    except Exception as e: print(f"   -> ⚠️ Yahoo Hata: {e}")
    
    print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - FİYAT + DEĞİŞİM)
# ==============================================================================
def get_altin_site():
    print("2. Altın Fiyatları ve Değişimleri çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            # Tabloyu bul
            table = soup.find("table")
            if table:
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) > 3:
                        try:
                            isim = tds[0].get_text(strip=True)
                            if "Ons" not in isim:
                                fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True)) # Satış
                                degisim_txt = tds[3].get_text(strip=True) # % Değişim Sütunu
                                degisim = metni_sayiya_cevir(degisim_txt)
                                
                                if fiyat > 0:
                                    data[isim] = {"fiyat": fiyat, "degisim": degisim}
                        except: continue
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# 3. BIST & ABD & FON (TRADINGVIEW - HAZIR DEĞİŞİM VERİSİ)
# ==============================================================================
def get_tradingview_data(market, filter_type, range_limit):
    url = f"https://scanner.tradingview.com/{market}/scan"
    payload = {
        "filter": [{"left": "type", "operation": filter_type[0], "right": filter_type[1]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change"], # change = % Değişim
        "range": [0, range_limit]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                d = h.get('d', []) # [isim, fiyat, değişim]
                if len(d) > 2:
                    isim = d[0]
                    fiyat = float(d[1])
                    degisim = float(d[2]) # TV direkt % değişim verir
                    
                    data[isim] = {"fiyat": fiyat, "degisim": round(degisim, 2)}
    except: pass
    return data

# ==============================================================================
# 4. KRİPTO (CMC API - HAZIR DEĞİŞİM)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY: return {}
    print(f"6. Kripto Piyasası taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            for coin in r.json()['data']:
                quote = coin['quote']['USD']
                fiyat = quote['price']
                degisim = quote['percent_change_24h'] # 24s Değişim
                
                data[f"{coin['symbol']}-USD"] = {
                    "fiyat": round(float(fiyat), 4),
                    "degisim": round(float(degisim), 2)
                }
            print(f"   -> ✅ CMC Başarılı: {len(data)} coin.")
    except: pass
    return data

# ==============================================================================
# KAYIT
# ==============================================================================
try:
    print("--- FİNANS BOTU (FİYAT + DEĞİŞİM %) ---")
    
    # TradingView Çağrıları
    bist_data = get_tradingview_data("turkey", ["in_range", ["stock", "dr"]], 1000)
    print(f"   -> ✅ BIST: {len(bist_data)} hisse.")
    
    abd_data = get_tradingview_data("america", ["in_range", ["stock", "dr"]], 600)
    print(f"   -> ✅ ABD: {len(abd_data)} hisse.")
    
    fon_data = get_tradingview_data("turkey", ["equal", "fund"], 2000)
    print(f"   -> ✅ Fon: {len(fon_data)} adet.")

    final_paket = {
        "doviz_tl": get_doviz_yahoo(),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": bist_data,
        "borsa_abd_usd": abd_data,
        "fon_tl": fon_data,
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
        
        print(f"🎉 BAŞARILI: [{doc_id} - {saat}] Veriler ve Değişim Oranları Kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
