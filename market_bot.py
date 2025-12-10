import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import sys
import os
import json
import warnings
from bs4 import BeautifulSoup
import time
import pandas as pd

# --- YENİ EKLENEN KÜTÜPHANE (TEFAS) ---
from tefas import Crawler

# --- SELENIUM KÜTÜPHANELERİ ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
# 1. DÖVİZ (KAYNAK: FOREKS.COM - SELENIUM İLE)
# ==============================================================================
def get_doviz_foreks():
    print("1. Döviz Kurları (Foreks.com - Selenium) çekiliyor...")
    data = {}
    
    isim_map = {
        "Dolar": "USD", "Euro": "EUR", "Sterlin": "GBP", "İsviçre Frangı": "CHF",
        "Kanada Doları": "CAD", "Japon Yeni": "JPY", "Rus Rublesi": "RUB",
        "Çin Yuanı": "CNY", "BAE Dirhemi": "BAE"
    }

    url = "https://www.foreks.com/doviz/"
    
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent={headers_general['User-Agent']}")

    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(url)
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.find_all("tr")
        
        for row in rows:
            text_row = row.get_text()
            found_key = None
            for tr_name, kod in isim_map.items():
                if tr_name in text_row:
                    found_key = kod
                    break
            
            if found_key:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    try:
                        fiyat_raw = cols[1].get_text(strip=True)
                        degisim_raw = cols[2].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(fiyat_raw)
                        degisim = metni_sayiya_cevir(degisim_raw)
                        
                        if fiyat == 0 and len(cols) > 5:
                             fiyat_raw = cols[5].get_text(strip=True)
                             fiyat = metni_sayiya_cevir(fiyat_raw)

                        if fiyat > 0:
                            data[found_key] = {"price": fiyat, "change": degisim}
                    except: continue

        print(f"   -> ✅ Foreks Döviz Bitti: {len(data)} adet.")
    except Exception as e:
        print(f"   -> ⚠️ Foreks Selenium Hatası: {e}")
    finally:
        if driver: driver.quit()
        
    return data

# ==============================================================================
# 2. ALTIN (DOVIZ.COM)
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
                                degisim = metni_sayiya_cevir(tds[3].get_text(strip=True))
                                if fiyat > 0: 
                                    data[isim] = {"price": fiyat, "change": degisim}
                        except: continue
    except Exception as e:
        print(f"   -> ⚠️ Altın Hata: {e}")
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# 3. BIST (TRADINGVIEW)
# ==============================================================================
def get_bist_tradingview():
    print("3. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    
    # DÜZELTİLDİ: Yıldız karakterleri temizlendi
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change"],
        "range": [0, 1000]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 2:
                        data[d[0]] = {"price": float(d[1]), "change": round(float(d[2]), 2)}
                except: continue
            print(f"   -> ✅ BIST Başarılı: {len(data)} hisse.")
    except: pass
    return data

# ==============================================================================
# 4. ABD BORSASI (TRADINGVIEW)
# ==============================================================================
def get_abd_tradingview():
    print("4. ABD Borsası (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/america/scan"
    
    # DÜZELTİLDİ: Yıldız karakterleri temizlendi
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "market_cap_basic"],
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
                    if len(d) > 2:
                        data[d[0]] = {"price": float(d[1]), "change": round(float(d[2]), 2)}
                except: continue
            print(f"   -> ✅ ABD Başarılı: {len(data)} hisse.")
    except: pass
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
                quote = coin['quote']['USD']
                data[f"{coin['symbol']}-USD"] = {
                    "price": round(float(quote['price']), 4),
                    "change": round(float(quote['percent_change_24h']), 2)
                }
            print(f"   -> ✅ CMC Başarılı: {len(data)} coin.")
    except: pass
    return data

# ==============================================================================
# 6. TEFAS FONLARI (KÜTÜPHANE İLE)
# ==============================================================================
def get_tefas_lib():
    print("6. TEFAS Fonları (Kütüphane) çekiliyor...")
    
    try:
        crawler = Crawler()
        
        # Son 5 günü çekelim ki hafta sonuna denk gelse bile veri bulabilelim
        bugun = datetime.now()
        baslangic = bugun - timedelta(days=5) 
        
        # Veriyi çek
        df = crawler.fetch(
            start=baslangic.strftime("%Y-%m-%d"), 
            end=bugun.strftime("%Y-%m-%d"),
            columns=["code", "date", "price", "title"]
        )
        
        if df is None or df.empty:
            print("   -> ⚠️ TEFAS verisi boş geldi.")
            return {}

        # İşlemler
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['code', 'date'])
        
        # Değişim Hesabı
        df['onceki_fiyat'] = df.groupby('code')['price'].shift(1)
        df['degisim'] = ((df['price'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['degisim'] = df['degisim'].fillna(0.0)
        
        # Sadece son veriyi al
        df_latest = df.groupby('code').tail(1)
        
        data = {}
        for item in df_latest.to_dict('records'):
            kod = item['code']
            data[kod] = {
                "price": float(item['price']),
                "change": round(float(item['degisim']), 2),
                "name": item.get('title', '')
            }
            
        print(f"   -> ✅ TEFAS Başarılı: {len(data)} fon çekildi.")
        return data

    except Exception as e:
        print(f"   -> ⚠️ TEFAS Hatası: {e}")
        return {}

# ==============================================================================
# KAYIT (SADECE KAPANIŞ YEDEĞİ - TOLERANSLI)
# ==============================================================================
try:
    print("--- PİYASA BOTU BAŞLIYOR ---")
    
    # 1. Veri Paketini Oluştur
    final_paket = {
        "doviz_tl": get_doviz_foreks(),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "fon_tl": get_tefas_lib(),
        "last_updated": firestore.SERVER_TIMESTAMP
    }

    # Eğer en az bir veri grubu doluysa işlemlere başla
    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        
        # GitHub sunucusu UTC çalışır. Biz Türkiye saatini (UTC+3) hesaplıyoruz.
        # Bu sayede sunucunun saati ne olursa olsun bizim saatimiz doğru olur.
        tr_saat = datetime.utcnow() + timedelta(hours=3)
        
        # -------------------------------------------------------------
        # ADIM 1: CANLI VERİYİ GÜNCELLE (Her zaman çalışır)
        # -------------------------------------------------------------
        # Bu kısım uygulamanın anlık gördüğü veridir.
        try:
            db.collection(u'market_data').document(u'LIVE_PRICES').set(final_paket)
            print(f"✅ [{tr_saat.strftime('%H:%M:%S')}] CANLI Fiyatlar güncellendi.")
        except Exception as e:
            print(f"❌ Canlı veri yazma hatası: {e}")

        # -------------------------------------------------------------
        # ADIM 2: KAPANIŞI ARŞİVLE (Sadece 18:30 Seansı)
        # -------------------------------------------------------------
        # Mantık: Saat 18 olsun VE Dakika 20'den büyük olsun.
        # - Saat 18:00'deki çalışmada dakika 00-10 arasıdır -> KAYDETMEZ.
        # - Saat 18:30'daki çalışmada dakika 30'dur -> KAYDEDER.
        # - Gecikme olsa bile (örn: 18:45) -> 45 > 20 -> YİNE KAYDEDER.
        
        if tr_saat.hour == 18 and tr_saat.minute >= 20:
            
            doc_id = tr_saat.strftime("%Y-%m-%d")
            snapshot_name = "18:30_Kapanis" 
            
            day_ref = db.collection(u'market_history').document(doc_id)
            day_ref.set({'date': doc_id}, merge=True)
            day_ref.collection(u'snapshots').document(snapshot_name).set(final_paket, merge=True)
            
            print(f"💾 [{doc_id}] GÜN SONU KAPANIŞI Arşivlendi (Saat: {tr_saat.strftime('%H:%M')}).")
            
        else:
            print(f"⏩ [{tr_saat.strftime('%H:%M')}] Tarihçe atlandı (Sadece 18:30 sonrası kapanış alınır).")

    else:
        print("❌ HATA: Hiçbir veri çekilemedi!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
