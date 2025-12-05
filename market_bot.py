import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json
import pandas as pd
import warnings
from bs4 import BeautifulSoup

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
# Foreks.com gibi siteler için tarayıcı gibi davranan güçlü header
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"
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
    """
    Örnek Girdi: "34,50 TL" -> Çıktı: 34.50
    """
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# ==============================================================================
# 1. DÖVİZ (KAYNAK: FOREKS.COM - YENİ)
# ==============================================================================
def get_doviz_foreks():
    print("1. Döviz Kurları (Foreks.com) çekiliyor...")
    data = {}
    
    # Foreks'teki görünen adları senin ID'lerine eşliyoruz
    isim_map = {
        "ABD Doları": "USD",
        "Euro": "EUR",
        "İngiliz Sterlini": "GBP",
        "İsviçre Frangı": "CHF",
        "Kanada Doları": "CAD",
        "Japon Yeni": "JPY",        
        "Avustralya Doları": "AUD"
    }

    url = "https://www.foreks.com/doviz/"
    
    try:
        r = requests.get(url, headers=headers_general, timeout=20)
        
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            
            # Foreks'te veriler tablo satırları (tr) içindedir
            rows = soup.find_all("tr")
            
            for row in rows:
                text_row = row.get_text() # Satırın tamamını metin olarak al
                
                # Bu satırda bizim istediğimiz paralardan biri var mı?
                found_key = None
                for tr_name, kod in isim_map.items():
                    if tr_name in text_row:
                        found_key = kod
                        break
                
                if found_key:
                    cols = row.find_all("td")
                    # Foreks tablosu genelde: [İsim, Sembol, Alış, Satış, %Fark, ...]
                    # Sayısal değerlerin olduğu sütunları hedefliyoruz.
                    if len(cols) >= 5:
                        try:
                            # 3. index genelde Satış Fiyatıdır (Piyasa değeri)
                            satis_raw = cols[3].get_text(strip=True)
                            # 4. veya 5. index % Değişimdir (Sütun yapısına göre değişebilir, genelde yan yanadır)
                            degisim_raw = cols[4].get_text(strip=True)
                            
                            fiyat = metni_sayiya_cevir(satis_raw)
                            degisim = metni_sayiya_cevir(degisim_raw)
                            
                            if fiyat > 0:
                                data[found_key] = {
                                    "price": fiyat,
                                    "change": degisim
                                }
                        except:
                            continue

            print(f"   -> ✅ Foreks Döviz Bitti: {len(data)} adet.")
        else:
            print(f"   -> ⚠️ Bağlantı Hatası: {r.status_code}")

    except Exception as e:
        print(f"   -> ⚠️ Foreks Hata: {e}")
        
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
                        # d[0]=İsim, d[1]=Fiyat, d[2]=Değişim
                        data[d[0]] = {"price": float(d[1]), "change": round(float(d[2]), 2)}
                except: continue
            print(f"   -> ✅ BIST Başarılı: {len(data)} hisse.")
    except: pass
    return data

# ==============================================================================
# 4. ABD BORSASI (TRADINGVIEW SCANNER + DEĞİŞİM)
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
# 5. KRİPTO (CMC API + DEĞİŞİM)
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
# KAYIT (SNAPSHOT MİMARİSİ)
# ==============================================================================
try:
    print("--- PİYASA BOTU (DEĞİŞİM ORANLI) - FOREKS ENTEGRASYONU ---")
    
    final_paket = {
        "doviz_tl": get_doviz_foreks(),    # YENİ FOREKS FONKSİYONU
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "timestamp": firestore.SERVER_TIMESTAMP
    }

    # Eğer en az bir sözlük doluysa kaydet
    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        simdi = datetime.now()
        doc_id = simdi.strftime("%Y-%m-%d")
        saat = simdi.strftime("%H:%M")
        
        day_ref = db.collection(u'market_history').document(doc_id)
        day_ref.set({'date': doc_id}, merge=True)
        
        # Merge=True: Fon verisini silmez, üstüne yazar
        day_ref.collection(u'snapshots').document(saat).set(final_paket, merge=True)
        
        total = sum(len(v) for k,v in final_paket.items() if isinstance(v, dict))
        print(f"🎉 BAŞARILI: [{doc_id} - {saat}] Toplam {total} veri kaydedildi.")
    else:
        print("❌ HATA: Veri çekilemedi (Tüm kaynaklar boş)!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
