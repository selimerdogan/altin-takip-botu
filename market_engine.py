import requests
import re
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
from tefas import Crawler
from concurrent.futures import ThreadPoolExecutor
from alarm_engine import check_and_trigger_alarms

warnings.simplefilter(action='ignore', category=FutureWarning)

headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def init_firebase():
    if firebase_admin._apps:
        return firestore.client()
        
    firebase_key_str = os.environ.get('FIREBASE_KEY')
    if firebase_key_str:
        cred = credentials.Certificate(json.loads(firebase_key_str))
    elif os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
    else:
        # Default application credentials (e.g., when running on Google Cloud Run directly!)
        cred = credentials.ApplicationDefault()
        
    firebase_admin.initialize_app(cred)
    return firestore.client()

def metni_sayiya_cevir(metin):
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

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

def get_doviz_exchangerate(db):
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
        
        rates_yesterday_db = {}
        try:
            dun = datetime.now() - timedelta(days=1)
            doc_id = dun.strftime("%Y-%m-%d")
            doc_ref = db.collection('market_history').document(doc_id).collection('snapshots').document('18:30_Kapanis')
            doc = doc_ref.get()
            if doc.exists:
                rates_yesterday_db = (doc.to_dict() or {}).get('doviz_tl', {})
        except Exception as e:
            print(f"    ⚠️ Geçmiş veri okuma hatası: {e}")

        if resp_today.status_code == 200:
            rates_today = resp_today.json().get('conversion_rates', {})
            data = {}
            dolar_tl_today = rates_today.get('TRY', 0)
            dolar_tl_yesterday = rates_yesterday_db.get("USD", {}).get("price", 0) if rates_yesterday_db else 0

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
                            if rates_yesterday_db and kod in rates_yesterday_db:
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
                            a_tag = tds[0].find("a")
                            if a_tag:
                                raw_isim = a_tag.get_text(strip=True)
                            else:
                                raw_isim = tds[0].get_text(strip=True)
                            # Saat etiketini (örn: 15:07, 15:06) temizle
                            isim = re.sub(r'\s*\d{1,2}:\d{2}$', '', raw_isim).strip()
                            
                            if "Ons" not in isim and isim:
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
            
            # Vario ticker uyumluluğu için alias ekle (Gram Gümüş -> Gümüş, Gram Altın -> Altın)
            if "Gram Gümüş" in data and "Gümüş" not in data:
                data["Gümüş"] = {
                    "price": data["Gram Gümüş"]["price"],
                    "change": data["Gram Gümüş"]["change"],
                    "name": "Gümüş",
                    "type": "gold"
                }
            if "Gram Altın" in data and "Altın" not in data:
                data["Altın"] = {
                    "price": data["Gram Altın"]["price"],
                    "change": data["Gram Altın"]["change"],
                    "name": "Altın",
                    "type": "gold"
                }
    except Exception as e:
        print(f"    -> ⚠️ Altın Hata: {e}")
    return data

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

def get_abd_tradingview():
    print("4. ABD Borsası (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "market_cap_basic", "description"],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 750]
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

def get_crypto_cmc(limit=500):
    cmc_key = os.environ.get('CMC_API_KEY')
    if not cmc_key:
        print("    -> ⚠️ CMC Key Yok.")
        return {}
    print(f"5. Kripto Piyasası (CMC Top {limit}) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': cmc_key}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            for coin in r.json().get('data', []):
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

def get_tefas_lib():
    print("6. TEFAS Fonları (Doğrudan Yüksek Hızlı API) çekiliyor...")
    list_url = 'https://www.tefas.gov.tr/api/funds/fonGetiriBazliBilgiGetir'
    detail_url = 'https://www.tefas.gov.tr/api/funds/fonBilgiGetir'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Content-Type': 'application/json'
    }

    all_funds = {}
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=1)
    session.mount('https://', adapter)

    for kind in ['YAT', 'EMK', 'BYF']:
        p = {
            'dil': 'TR', 'fonTipi': kind, 'kurucuKodu': None, 'sfonTurKod': None, 'fonTurAciklama': None,
            'islem': 1, 'fonTurKod': None, 'fonGrubu': None, 'donemGetiri1a': '1', 'donemGetiri3a': '1',
            'donemGetiri6a': '1', 'donemGetiri1y': '1', 'donemGetiriyb': '1', 'donemGetiri3y': '1',
            'donemGetiri5y': '1', 'basTarih': None, 'bitTarih': None, 'calismaTipi': 2, 'getiriOrani': '1'
        }
        try:
            r = session.post(list_url, json=p, headers=headers, timeout=8)
            if r.status_code == 200:
                for row in (r.json() or {}).get('resultList', []):
                    code = row.get('fonKodu')
                    if code and code not in all_funds:
                        all_funds[code] = row.get('fonUnvan', '')
        except Exception as e:
            print(f"    ⚠️ TEFAS {kind} liste hatası: {e}")

    codes = list(all_funds.keys())
    print(f"   -> Toplam {len(codes)} adet aktif TEFAS fonu listelendi, detaylar çekiliyor...")

    if not codes:
        try:
            crawler = Crawler(fund_limit=2000)
            bugun = datetime.now()
            baslangic = bugun - timedelta(days=5) 
            df = crawler.fetch(start=baslangic.strftime("%Y-%m-%d"), end=bugun.strftime("%Y-%m-%d"), columns=["code", "date", "price", "title"])
            if df is not None and not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(by=['code', 'date'])
                df['onceki_fiyat'] = df.groupby('code')['price'].shift(1)
                df['degisim'] = ((df['price'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
                df_latest = df.groupby('code').tail(1)
                fallback_data = {}
                for item in df_latest.to_dict('records'):
                    fallback_data[item['code']] = {
                        "price": float(item['price']),
                        "change": round(float(item.get('degisim', 0.0) or 0.0), 2),
                        "name": item.get('title', ''),
                        "type": "fund"
                    }
                return fallback_data
        except Exception as fb_err:
            print(f"    ⚠️ Crawler fallback hatası: {fb_err}")
        return {}

    def fetch_fund(code):
        for _ in range(2):
            try:
                res = session.post(detail_url, json={'fonKodu': code, 'dil': 'TR'}, headers=headers, timeout=4)
                if res.status_code == 200:
                    lst = (res.json() or {}).get('resultList', [])
                    return lst[0] if lst else None
            except Exception:
                pass
        return None

    with ThreadPoolExecutor(max_workers=40) as executor:
        results = list(executor.map(fetch_fund, codes))

    data = {}
    for item in results:
        if not item or item.get('sonFiyat') is None:
            continue
        kod = item.get('fonKodu')
        price = float(item.get('sonFiyat', 0.0))
        change = float(item.get('gunlukGetiri', 0.0) or 0.0)
        title = item.get('fonUnvan') or all_funds.get(kod, '')
        data[kod] = {
            "price": price,
            "change": round(change, 2),
            "name": title,
            "type": "fund"
        }

    print(f"   -> ✅ Toplam {len(data)} adet TEFAS fonu fiyat ve getirisiyle hazırlandı.")
    return data

def execute_engine():
    db = init_firebase()
    print("--- 🚀 VARIO CLOUD RUN MARKET MOTORU BAŞLIYOR ---")
    start_time = time.time()
    
    final_paket = {
        "endeks_tr": get_bist100_index(),
        "doviz_tl": get_doviz_exchangerate(db),
        "altin_tl": get_altin_site(),
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_abd_tradingview(),
        "kripto_usd": get_crypto_cmc(500),
        "fon_tl": get_tefas_lib(),
        "last_updated": firestore.SERVER_TIMESTAMP
    }

    if not any(len(v) > 0 for k,v in final_paket.items() if isinstance(v, dict)):
        raise RuntimeError("Hiçbir kaynaktan piyasa verisi çekilemedi!")

    tr_saat = datetime.utcnow() + timedelta(hours=3)
    
    # 1. CANLI VERİ (Mevcut uygulamalar için LIVE_PRICES)
    db.collection('market_data').document('LIVE_PRICES').set(final_paket)
    
    # 2. KOMPAKT KATEGORİK KAYITLAR (Yeni nesil hızlı okuma için)
    db.collection('market_data').document('bist').set({"items": final_paket["borsa_tr_tl"], "updated": firestore.SERVER_TIMESTAMP})
    db.collection('market_data').document('doviz').set({"items": final_paket["doviz_tl"], "updated": firestore.SERVER_TIMESTAMP})
    db.collection('market_data').document('altin').set({"items": final_paket["altin_tl"], "updated": firestore.SERVER_TIMESTAMP})
    db.collection('market_data').document('kripto').set({"items": final_paket["kripto_usd"], "updated": firestore.SERVER_TIMESTAMP})
    db.collection('market_data').document('fon').set({"items": final_paket["fon_tl"], "updated": firestore.SERVER_TIMESTAMP})
    
    print(f"✅ [{tr_saat.strftime('%H:%M:%S')}] CANLI Fiyatlar güncellendi.")

    # 3. ALARM KONTROLÜ & ANINDA PUSH BİLDİRİM
    alarm_results = check_and_trigger_alarms(final_paket, db)

    # 4. GÜN SONU KAPANIK ARŞİVİ
    if tr_saat.hour == 18 and tr_saat.minute >= 20:
        doc_id = tr_saat.strftime("%Y-%m-%d")
        day_ref = db.collection('market_history').document(doc_id)
        day_ref.set({'date': doc_id}, merge=True)
        day_ref.collection('snapshots').document("18:30_Kapanis").set(final_paket, merge=True)
        print(f"💾 [{doc_id}] GÜN SONU KAPANIŞI Arşivlendi.")

    elapsed = round(time.time() - start_time, 2)
    return {
        "status": "success",
        "elapsed_seconds": elapsed,
        "timestamp": tr_saat.isoformat(),
        "counts": {
            "bist": len(final_paket["borsa_tr_tl"]),
            "abd": len(final_paket["borsa_abd_usd"]),
            "crypto": len(final_paket["kripto_usd"]),
            "gold": len(final_paket["altin_tl"]),
            "currency": len(final_paket["doviz_tl"]),
            "funds": len(final_paket["fon_tl"])
        },
        "alarms": alarm_results
    }
