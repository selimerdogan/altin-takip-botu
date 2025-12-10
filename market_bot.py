import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json
import warnings
from bs4 import BeautifulSoup
import time

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
# 1. DÖVİZ (KAYNAK: FOREKS.COM - SELENIUM İLE - GÖRSEL DOĞRULAMALI)
# ==============================================================================
def get_doviz_foreks():
    print("1. Döviz Kurları (Foreks.com - Selenium) çekiliyor...")
    data = {}
    
    # Sitedeki isimler ile senin veritabanı kodların arasındaki eşleştirme
    isim_map = {
        "Dolar": "USD",          # Görselde "Dolar" olarak geçiyor
        "Euro": "EUR",           # Görselde "Euro" olarak geçiyor
        "Sterlin": "GBP",        # Görselde "Sterlin" olarak geçiyor
        "İsviçre Frangı": "CHF",
        "Kanada Doları": "CAD",
        "Japon Yeni": "JPY",        
        "Rus Rublesi": "RUB",    # Görselde var, ekledim (İstersen kaldırabilirsin)
        "Çin Yuanı": "CNY",       # Görselde var, ekledim
        "BAE Dirhemi": "BAE"       # Görselde var, ekledim
    }

    url = "https://www.foreks.com/doviz/"
    
    # --- Tarayıcı Ayarları ---
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
        time.sleep(5) # Sayfanın yüklenmesi için bekleme süresi
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Tablo satırlarını bul
        # Foreks'te veriler genelde 'tbody' içindeki 'tr'lerde olur
        rows = soup.find_all("tr")
        
        for row in rows:
            text_row = row.get_text()
            
            # 1. İsim Eşleşmesi Kontrolü
            found_key = None
            for tr_name, kod in isim_map.items():
                # "Amerikan Doları" veya sadece "Dolar" geçebilir, görselde "Dolar" başlıkta büyük yazıyor
                if tr_name in text_row:
                    found_key = kod
                    break
            
            if found_key:
                cols = row.find_all("td")
                
                # GÖRSELE GÖRE SÜTUN ANALİZİ:
                # cols[0] -> Sembol (İsim/Bayrak)
                # cols[1] -> Son (FİYAT) -> Örn: 42,5273
                # cols[2] -> Fark % (DEĞİŞİM) -> Örn: %0,07
                # cols[3] -> Fark
                # cols[4] -> Alış
                # cols[5] -> Satış
                
                if len(cols) >= 3:
                    try:
                        # Fiyat için 'Son' sütununu (index 1) alıyoruz
                        fiyat_raw = cols[1].get_text(strip=True)
                        
                        # Değişim için 'Fark %' sütununu (index 2) alıyoruz
                        degisim_raw = cols[2].get_text(strip=True)
                        
                        fiyat = metni_sayiya_cevir(fiyat_raw)
                        degisim = metni_sayiya_cevir(degisim_raw)
                        
                        # Eğer fiyat 0 geldiyse (bazen Son boş olabilir), Satış'ı (index 5) dene
                        if fiyat == 0 and len(cols) > 5:
                             fiyat_raw = cols[5].get_text(strip=True)
                             fiyat = metni_sayiya_cevir(fiyat_raw)

                        if fiyat > 0:
                            data[found_key] = {
                                "price": fiyat,
                                "change": degisim
                            }
                    except Exception as inner_e:
                        continue

        print(f"   -> ✅ Foreks Döviz Bitti: {len(data)} adet.")

    except Exception as e:
        print(f"   -> ⚠️ Foreks Selenium Hatası: {e}")
    finally:
        if driver:
            driver.quit()
        
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
# 3. BIST (TRADINGVIEW)
# ==============================================================================
def get_bist_tradingview():
    print("3. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
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
                fiyat = quote['price']
                degisim = quote['percent_change_24h']
                data[f"{coin['symbol']}-USD"] = {
                    "price": round(float(fiyat), 4),
                    "change": round(float(degisim), 2)
                }
            print(f"   -> ✅ CMC Başarılı: {len(data)} coin.")
    except: pass
    return data

# ==============================================================================
# KAYIT (REVİZE EDİLMİŞ - HİBRİT YAPI)
# ==============================================================================
try:
    print("--- PİYASA BOTU (DEĞİŞİM ORANLI) - FOREKS SELENIUM ---")
    
    # Veri Paketini Oluştur
    final_paket = {
        "doviz_tl": get_doviz_foreks(),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "last_updated": firestore.SERVER_TIMESTAMP # "timestamp" yerine last_updated daha anlaşılır
    }

    # Eğer veri doluysa işlemlere başla
    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        
        simdi = datetime.now()

        # -------------------------------------------------------------
        # ADIM 1: CANLI VERİYİ GÜNCELLE (Uygulamanın okuyacağı yer)
        # -------------------------------------------------------------
        # Bu işlem her çalışmada yapılır. Eski veriyi ezer, yenisini yazar.
        # Bu sayede veritabanı şişmez ve uygulaman her zaman tek doküman okur.
        try:
            db.collection(u'market_data').document(u'LIVE_PRICES').set(final_paket)
            print(f"✅ [{simdi.strftime('%H:%M:%S')}] CANLI Fiyatlar 'LIVE_PRICES' dosyasına yazıldı.")
        except Exception as e:
            print(f"❌ Canlı veri yazma hatası: {e}")

        # -------------------------------------------------------------
        # ADIM 2: GEÇMİŞİ ARŞİVLE (Grafikler için)
        # -------------------------------------------------------------
        # Sadece saat başlarında ve buçuklarda (00 ve 30 geçe) kayıt alıyoruz.
        # İstersen bu aralığı değiştirebilirsin (örn: simdi.minute == 0 -> Sadece saat başı)
        
        if simdi.minute % 30 == 0:
            doc_id = simdi.strftime("%Y-%m-%d") # 2025-12-10
            saat_str = simdi.strftime("%H:%M")  # 14:30
            
            day_ref = db.collection(u'market_history').document(doc_id)
            # Günlük dokümanı oluştur (yoksa)
            day_ref.set({'date': doc_id}, merge=True)
            # O saatin snapshot'ını kaydet
            day_ref.collection(u'snapshots').document(saat_str).set(final_paket, merge=True)
            
            print(f"💾 [{doc_id} - {saat_str}] TARİHÇE Arşivlendi (Grafik Verisi).")
        else:
            print(f"⏩ [{simdi.strftime('%H:%M')}] Tarihçe atlandı (Tasarruf Modu).")

    else:
        print("❌ HATA: Veri çekilemedi, paket boş!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)



