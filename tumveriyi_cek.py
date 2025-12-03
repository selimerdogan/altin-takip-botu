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
        # "34,5020" -> 34.5020
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# ==============================================================================
# 1. DÖVİZ (DOVIZ.COM - İLK 10) - YENİ MODÜL!
# ==============================================================================
def get_doviz_site():
    print("1. Döviz Kurları (doviz.com) taranıyor...")
    url = "https://www.doviz.com/"
    data = {}
    
    try:
        r = requests.get(url, headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            
            # Doviz.com ana sayfasındaki "market-data" veya ana tabloyu buluyoruz.
            # Genellikle <div class="market-data"> içindeki item'lardır.
            # Veya basitçe 'item' class'ına sahip divleri gezebiliriz.
            
            # En garanti yöntem: data-socket-key özelliğine sahip satırları bulmak
            # Örn: <div class="item" data-socket-key="USD" ...>
            
            items = soup.find_all("div", class_="item")
            
            count = 0
            limit = 10 # İlk 10 tanesi yeterli
            
            for item in items:
                if count >= limit: break
                
                try:
                    # Kod (USD, EUR)
                    kod_tag = item.find("span", class_="name")
                    if not kod_tag: continue
                    kod = kod_tag.get_text(strip=True)
                    
                    # Fiyat (Satış fiyatını alıyoruz)
                    fiyat_tag = item.find("span", class_="value")
                    if not fiyat_tag: continue
                    fiyat_text = fiyat_tag.get_text(strip=True)
                    
                    # Veriyi temizle
                    fiyat = metni_sayiya_cevir(fiyat_text)
                    
                    # Sadece 3 harfli standart kurları al (ALTIN vs. karışmasın)
                    if len(kod) == 3 and kod.isalpha():
                        data[kod] = fiyat
                        count += 1
                        
                except: continue
            
            # Eğer yukarıdaki yöntem çalışmazsa (Site tasarımı değişirse) YEDEK PLAN:
            if len(data) == 0:
                 # Tablo yapısını dene
                 rows = soup.find_all("tr")
                 for row in rows:
                     cols = row.find_all("td")
                     if len(cols) >= 3:
                         kod = cols[0].get_text(strip=True).split()[0] # "ABD Doları" -> "ABD" (Riskli)
                         # Bu kısım riskli olduğu için Yahoo yedeğini aşağıda çağıracağız.
                         pass

            print(f"   -> ✅ Döviz Başarılı: {len(data)} adet kur (doviz.com).")
            
    except Exception as e:
        print(f"   -> ⚠️ Döviz.com Hatası: {e}")
    
    # Eğer siteden çekemezsek boş dönmesin, Yahoo'dan tamamlasın
    if len(data) < 3:
        print("   -> ⚠️ Site verisi eksik, Yahoo devreye giriyor...")
        return get_doviz_yahoo_yedek()
        
    return data

# --- YEDEK DÖVİZ FONKSİYONU (YAHOO) ---
def get_doviz_yahoo_yedek():
    liste = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X"]
    data = {}
    try:
        df = yf.download(liste, period="1d", progress=False)['Close']
        if not df.empty:
            son = df.iloc[-1]
            for k in liste:
                val = son.get(k)
                if pd.notna(val):
                    clean = k.replace("TRY=X", "")
                    data[clean] = round(float(val), 4)
    except: pass
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM - KAZIMA)
# ==============================================================================
def get_altin_site():
    print("2. Altın verileri çekiliyor...")
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
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                            if fiyat > 0: data[isim] = fiyat
                    except: continue
        else:
            print(f"   -> ⚠️ Altın Sitesi Hata: {r.status_code}")
    except Exception as e:
        print(f"   -> ⚠️ Altın Hata: {e}")
        
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
    except: pass
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
    print("--- FİNANS BOTU (DOVIZ.COM + TV + CMC) ---")
    
    final_paket = {
        "doviz_tl": get_doviz_site(),           # Yeni Kaynak!
        "altin_tl": get_altin_site(),
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
