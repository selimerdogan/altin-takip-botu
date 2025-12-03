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
# 1. DÖVİZ & GLOBAL EMTİA (YAHOO FINANCE)
# ==============================================================================
def get_doviz_emtia_yahoo():
    print("1. Döviz ve Global Emtia (Yahoo) çekiliyor...")
    
    # Hatalı olanlar (CNY, RUB, XAUTRY) çıkarıldı. Sadece çalışanlar kaldı.
    semboller = [
        "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", 
        "JPYTRY=X", "AUDTRY=X", # Ana Kurlar
        "EURUSD=X", "GBPUSD=X", "DX-Y.NYB", # Pariteler
        "GC=F", "SI=F", "PL=F", "PA=F" # Ons Altın, Gümüş, Platin, Paladyum
    ]
    
    data_doviz = {}
    data_emtia = {}
    
    try:
        df = yf.download(semboller, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        if not df.empty:
            son = df.ffill().iloc[-1]
            for kod in semboller:
                try:
                    val = son.get(kod)
                    if pd.notna(val):
                        fiyat = round(float(val), 4)
                        
                        # Emtiaları Ayır
                        if kod == "GC=F": data_emtia["Ons Altın"] = fiyat
                        elif kod == "SI=F": data_emtia["Gümüş (Ons)"] = fiyat
                        elif kod == "PL=F": data_emtia["Platin"] = fiyat
                        elif kod == "PA=F": data_emtia["Paladyum"] = fiyat
                        else:
                            # Döviz İsim Temizliği
                            key = kod.replace("TRY=X", "").replace("=X", "").replace(".NYB", "")
                            if key.endswith("TRY"): key = key.replace("TRY", "")
                            data_doviz[key] = fiyat
                except: continue
    except Exception as e:
        print(f"   -> ⚠️ Yahoo Hata: {e}")
        
    print(f"   -> ✅ Yahoo Bitti: {len(data_doviz)} Döviz, {len(data_emtia)} Emtia.")
    return data_doviz, data_emtia

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - KAZIMA - GARANTİ YÖNTEM)
# ==============================================================================
def get_altin_site():
    print("2. Gram/Çeyrek Altın (Site) çekiliyor...")
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
                        # Ons'u Yahoo'dan alıyoruz, buradan sadece TL olanları alalım
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                            if fiyat > 0: data[isim] = fiyat
                    except: continue
    except Exception as e:
        print(f"   -> ⚠️ Altın Sitesi Hata: {e}")
        
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
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
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
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
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
                    if len(d) > 1:
                        data[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ Fonlar Başarılı: {len(data)} adet.")
    except: pass
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
    print("--- FİNANS BOTU (YAHOO + TV + CMC + DOVIZ.COM) ---")
    
    # 1. Yahoo'dan Döviz ve Emtia
    d_doviz, d_emtia = get_doviz_emtia_yahoo()
    
    # 2. Siteden Altın (TL)
    d_altin_tl = get_altin_site()
    
    # Altın ve Emtia verilerini birleştirelim (Hepsi 'altin_tl' altında görünsün veya ayrılabilir)
    # Kullanıcı tercihine göre 'altin_tl' içine hem TL altınları hem Global emtiaları koyuyoruz.
    tum_altin_emtia = {**d_altin_tl, **d_emtia}

    final_paket = {
        "doviz_tl": d_doviz,
        "altin_tl": tum_altin_emtia, # Gram Altın + Ons + Gümüş...
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
