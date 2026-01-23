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
# YARDIMCI FONKSİYON: DÜNKÜ KAPANIŞI GETİR (Firebase'den)
# ==============================================================================
def get_yesterday_prices_from_db():
    try:
        dun = datetime.now() - timedelta(days=1)
        doc_id = dun.strftime("%Y-%m-%d")
        doc_ref = db.collection(u'market_history').document(doc_id).collection(u'snapshots').document(u'18:30_Kapanis')
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            return data.get('doviz_tl', {})
        return {}
    except Exception as e:
        print(f"    ⚠️ Geçmiş veri okuma hatası: {e}")
        return {}

# ==============================================================================
# 0. BIST 100 ENDEKSİ (TRADINGVIEW) - YENİ! 📈
# ==============================================================================
def get_bist100_index():
    print("0. BIST 100 Endeksi çekiliyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "symbols": {"tickers": ["BIST:XU100"], "query": {"types": []}},
        "columns": ["name", "close", "change", "description"]
    }
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            res_data = r.json().get('data', [])
            if res_data:
                d = res_data[0].get('d', [])
                return {
                    "XU100": {
                        "price": float(d[1]),
                        "change": round(float(d[2]), 2),
                        "name": "BIST 100",
                        "type": "index"
                    }
                }
    except Exception as e:
        print(f"    ⚠️ BIST100 Hatası: {e}")
    return {}

# ==============================================================================
# 1. DÖVİZ (EXCHANGERATE-API)
# ==============================================================================
def get_doviz_exchangerate():
    print("1. Döviz Kurları (ExchangeRate-API) çekiliyor...")
    api_key = os.environ.get('EXCHANGERATE_API_KEY')
    if not api_key:
        print("    ⚠️ ExchangeRate API Key eksik!")
        return {}

    target_currencies = {
        "EUR": "Euro", "GBP": "İngiliz Sterlini", "CHF": "İsviçre Frangı",
        "JPY": "Japon Yeni", "CAD": "Kanada Doları", "AUD": "Avustralya Doları",
        "CNY": "Çin Yuanı", "HKD": "Hong Kong Doları", "SEK": "İsveç Kronu",
        "NOK": "Norveç Kronu", "DKK": "Danimarka Kronu", "PLN": "Polonya Zlotisi",
        "HUF": "Macar Forinti", "CZK": "Çek Korunası", "RON": "Rumen Leyi",
        "BGN": "Bulgar Levası", "ISK": "İzlanda Kronu", "UAH": "Ukrayna Grivnası",
        "RUB": "Rus Rublesi", "SAR": "Suudi Arabistan Riyali", "AED": "BAE Dirhemi",
        "QAR": "Katar Riyali", "KWD": "Kuveyt Dinarı", "BHD": "Bahreyn Dinarı",
        "OMR": "Umman Riyali", "JOD": "Ürdün Dinarı", "ILS": "İsrail Şekeli",
        "EGP": "Mısır Lirası", "KRW": "Güney Kore Wonu", "SGD": "Singapur Doları",
        "INR": "Hindistan Rupisi", "IDR": "Endonezya Rupiahı", "MYR": "Malezya Ringgiti",
        "PHP": "Filipin Pesosu", "THB": "Tayland Bahtı", "VND": "Vietnam Dongu",
        "PKR": "Pakistan Rupisi", "AZN": "Azerbaycan Manatı", "GEL": "Gürcistan Larisi",
        "KZT": "Kazakistan Tengesi", "MXN": "Meksika Pesosu", "BRL": "Brezilya Reali",
        "ARS": "Arjantin Pesosu", "CLP": "Şili Pesosu", "COP": "Kolombiya Pesosu",
        "PEN": "Peru Solü", "ZAR": "Güney Afrika Randı", "MAD": "Fas Dirhemi"
    }

    try:
        url_today = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
        resp_today = requests.get(url_today, timeout=10)
        rates_yesterday_db = get_yesterday_prices_from_db()
        history_source = "DB" if rates_yesterday_db else "NONE"

        if resp_today.status_code == 200:
            rates_today = resp_today.json().get('conversion_rates', {})
            data = {}
            dolar_tl_today = rates_today.get('TRY', 0)
            dolar_tl_yesterday = 0
            if history_source == "DB" and "USD" in rates_yesterday_db:
                dolar_tl_yesterday = rates_yesterday_db["USD"].get("price", 0)

            if dolar_tl_today > 0:
                degisim_usd = 0.0
                if dolar_tl_yesterday > 0:
                    degisim_usd = ((dolar_tl_today - dolar_tl_yesterday) / dolar_tl_yesterday) * 100
                
                data["USD"] = {
                    "price": round(float(dolar_tl_today), 4),
                    "change": round(degisim_usd, 2),
                    "name": "ABD Doları",
                    "type": "currency"
                }

                for kod, isim in target_currencies.items():
                    try:
                        rate_usd_today = rates_today.get(kod, 0)
                        if rate_usd_today > 0:
                            tl_today = dolar_tl_today / rate_usd_today
                            degisim_val = 0.0
                            if history_source == "DB" and kod in rates_yesterday_db:
                                tl_yesterday = rates_yesterday_db[kod].get("price", 0)
                                if tl_yesterday > 0:
                                    degisim_val = ((tl_today - tl_yesterday) / tl_yesterday) * 100
                            
                            data[kod] = {
                                "price": round(float(tl_today), 4),
                                "change": round(degisim_val, 2),
                                "name": isim,
                                "type": "currency"
                            }
                    except: continue
            return data
        return {}
    except Exception as e:
        print(f"    -> ⚠️ Hata: {e}")
        return {}

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
                                    data[isim] = {
                                        "price": fiyat, 
                                        "change": degisim, 
                                        "name": isim,
                                        "type": "gold"
                                    }
                        except: continue
    except Exception as e:
        print(f"    -> ⚠️ Altın Hata: {e}")
    return data

# ==============================================================================
# 3. BIST HİSSELERİ (TRADINGVIEW)
# ==============================================================================
def get_bist_tradingview():
    print("3. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "description"],
        "range": [0, 1000]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 3:
                        data[d[0]] = {
                            "price": float(d[1]), 
                            "change": round(float(d[2]), 2),
                            "name": d[3],
                            "type": "stock"
                        }
                except: continue
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
        "columns": ["name", "close", "change", "market_cap_basic", "description"],
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
                    if len(d) > 4:
                        data[d[0]] = {
                            "price": float(d[1]), 
                            "change": round(float(d[2]), 2),
                            "name": d[4],
                            "type": "stock"
                        }
                except: continue
    except: pass
    return data

# ==============================================================================
# 5. KRİPTO (CMC API) - LİMİT 500'E ÇIKARILDI
# ==============================================================================
def get_crypto_cmc(limit=500): # Varsayılan değer 500 oldu
    if not CMC_API_KEY:
        print("    -> ⚠️ CMC Key Yok.")
        return {}
    print(f"5. Kripto Piyasası (CMC Top {limit}) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30) # Timeout biraz arttırıldı
        if r.status_code == 200:
            for coin in r.json()['data']:
                quote = coin['quote']['USD']
                symbol = coin['symbol']
                data[f"{symbol}-USD"] = {
                    "price": round(float(quote['price']), 4),
                    "change": round(float(quote['percent_change_24h']), 2),
                    "name": coin['name'],
                    "type": "crypto"
                }
    except Exception as e: 
        print(f"    -> ⚠️ Kripto Hatası: {e}")
    return data

# ==============================================================================
# 6. TEFAS FONLARI (KÜTÜPHANE İLE)
# ==============================================================================
def get_tefas_lib():
    print("6. TEFAS Fonları (Kütüphane) çekiliyor...")
    try:
        crawler = Crawler()
        bugun = datetime.now()
        baslangic = bugun - timedelta(days=5) 
        df = crawler.fetch(
            start=baslangic.strftime("%Y-%m-%d"), 
            end=bugun.strftime("%Y-%m-%d"),
            columns=["code", "date", "price", "title"]
        )
        if df is None or df.empty:
            return {}
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['code', 'date'])
        df['onceki_fiyat'] = df.groupby('code')['price'].shift(1)
        df['degisim'] = ((df['price'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['degisim'] = df['degisim'].fillna(0.0)
        df_latest = df.groupby('code').tail(1)
        data = {}
        for item in df_latest.to_dict('records'):
            kod = item['code']
            data[kod] = {
                "price": float(item['price']),
                "change": round(float(item['degisim']), 2),
                "name": item.get('title', ''),
                "type": "fund"
            }
        return data
    except Exception as e:
        print(f"    -> ⚠️ TEFAS Hatası: {e}")
        return {}

# ==============================================================================
# KAYIT VE ÇALIŞTIRICI
# ==============================================================================
try:
    print("--- PİYASA BOTU BAŞLIYOR ---")
    
    final_paket = {
        "endeks_tr": get_bist100_index(), # BIST 100 Eklendi
        "doviz_tl": get_doviz_exchangerate(),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(250),
        "fon_tl": get_tefas_lib(),
        "last_updated": firestore.SERVER_TIMESTAMP
    }

    if any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        tr_saat = datetime.utcnow() + timedelta(hours=3)
        # 1. CANLI VERİ
        db.collection(u'market_data').document(u'LIVE_PRICES').set(final_paket)
        print(f"✅ [{tr_saat.strftime('%H:%M:%S')}] CANLI Fiyatlar güncellendi.")

        # 2. KAPANIŞ ARŞİVİ
        if tr_saat.hour == 18 and tr_saat.minute >= 20:
            doc_id = tr_saat.strftime("%Y-%m-%d")
            day_ref = db.collection(u'market_history').document(doc_id)
            day_ref.set({'date': doc_id}, merge=True)
            day_ref.collection(u'snapshots').document("18:30_Kapanis").set(final_paket, merge=True)
            print(f"💾 [{doc_id}] GÜN SONU KAPANIŞI Arşivlendi.")
    else:
        print("❌ HATA: Veri çekilemedi!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)

