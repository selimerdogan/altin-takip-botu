import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import sys
import os
import yfinance as yf
import pandas as pd
import io
import warnings
import json

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
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
# 1. BIST (TRADINGVIEW SCANNER API - EN SAĞLAM KAYNAK)
# ==============================================================================
def get_bist_tradingview():
    """
    TradingView'in gizli tarayıcı API'sini kullanır.
    Tüm BIST hisselerini (Halka arzlar dahil) tek seferde JSON olarak çeker.
    Engellenme riski çok düşüktür.
    """
    print("1. Borsa İstanbul (TradingView Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    
    # TradingView'a "Bana BIST'teki tüm hisseleri ver" diyen sorgu
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr", "fund"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "volume"],
        "sort": {"sortBy": "name", "sortOrder": "asc"},
        "range": [0, 600] # İlk 600 hisseyi getir (BIST'in tamamı sığar)
    }
    
    data_bist = {}
    
    try:
        # JSON formatında POST isteği atıyoruz
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        
        if r.status_code == 200:
            json_data = r.json()
            # Yapı: {"data": [{"d": ["THYAO", 274.5, ...]}, ...]}
            hisseler = json_data.get('data', [])
            
            for h in hisseler:
                try:
                    # TradingView verisi liste olarak gelir: [isim, fiyat, değişim, hacim]
                    raw_data = h.get('d', [])
                    if len(raw_data) > 1:
                        kod = raw_data[0]
                        fiyat = raw_data[1]
                        
                        if kod and fiyat:
                            data_bist[kod] = float(fiyat)
                except: continue
                
            print(f"   -> ✅ TradingView Başarılı: {len(data_bist)} hisse çekildi.")
        else:
            print(f"   -> ⚠️ TradingView Hatası: {r.status_code}")
            
    except Exception as e:
        print(f"   -> ⚠️ TradingView Bağlantı Hatası: {e}")
        
    return data_bist

# ==============================================================================
# 2. ABD BORSASI (GITHUB CSV - S&P 500)
# ==============================================================================
def get_sp500_dynamic():
    print("2. ABD Borsası (S&P 500 - Dinamik CSV) taranıyor...")
    url_csv = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
    data_abd = {}
    
    try:
        s = requests.get(url_csv).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        liste_sp500 = [x.replace('.', '-') for x in df['Symbol'].tolist()]
        
        # Hatalı olanları çıkar
        black_list = ['WBA', 'DISCA', 'DISCK']
        liste_sp500 = [x for x in liste_sp500 if x not in black_list]
        
        print(f"   -> CSV'den {len(liste_sp500)} şirket okundu. Fiyatlar Yahoo'dan çekiliyor...")
        
        df_yahoo = yf.download(liste_sp500, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        
        if not df_yahoo.empty:
            son_fiyatlar = df_yahoo.ffill().iloc[-1]
            for sembol in liste_sp500:
                try:
                    fiyat = son_fiyatlar.get(sembol)
                    if pd.notna(fiyat):
                        data_abd[sembol] = round(float(fiyat), 2)
                except: continue
                
        print(f"   -> ✅ S&P 500 Başarılı: {len(data_abd)} hisse.")
        
    except Exception as e:
        print(f"   -> ⚠️ ABD Hata: {e}")
        
    return data_abd

# ==============================================================================
# 3. KRİPTO (CMC API)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY:
        print("   -> ⚠️ CMC Key Yok, atlanıyor.")
        return {}
    
    print(f"3. Kripto Piyasası (CMC Top {limit}) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data_kripto = {}

    try:
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 200:
            for coin in r.json()['data']:
                try:
                    symbol = coin['symbol']
                    price = coin['quote']['USD']['price']
                    data_kripto[f"{symbol}-USD"] = round(float(price), 4)
                except: continue
            print(f"   -> ✅ CMC Başarılı: {len(data_kripto)} coin.")
    except: pass
    return data_kripto

# ==============================================================================
# 4. FONLAR (TEFAS AKILLI MOD)
# ==============================================================================
def get_tefas_data():
    print("4. Yatırım Fonları (TEFAS) taranıyor...")
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReturns"
    tefas_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "Origin": "https://www.tefas.gov.tr", "Content-Type": "application/json; charset=UTF-8"
    }
    session = requests.Session()
    try:
        session.get("https://www.tefas.gov.tr/FonKarsilastirma.aspx", headers=tefas_headers, timeout=10)
    except: pass
    
    simdi = datetime.now()
    for i in range(7):
        tarih = (simdi - timedelta(days=i))
        if tarih.year > datetime.now().year: tarih = tarih.replace(year=datetime.now().year)
        tarih_str = tarih.strftime("%d.%m.%Y")
        try:
            payload = {"calismatipi": "2", "fontip": "YAT", "bastarih": tarih_str, "bittarih": tarih_str}
            r = session.post(url, json=payload, headers=tefas_headers, timeout=30)
            if r.status_code == 200:
                d = r.json().get('data', [])
                if len(d) > 50:
                    fonlar = {}
                    for f in d:
                        try:
                            raw_fiyat = str(f['FIYAT']).replace(',', '.')
                            fonlar[f['FONKODU']] = float(raw_fiyat)
                        except: continue
                    print(f"   -> ✅ TEFAS Başarılı ({tarih_str}): {len(fonlar)} fon.")
                    return fonlar
        except: continue
    return {}

# ==============================================================================
# 5. DÖVİZ & ALTIN
# ==============================================================================
def get_doviz_yahoo():
    print("5. Döviz Kurları çekiliyor...")
    liste = ["USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X", "EURUSD=X", "GBPUSD=X", "JPY=X", "DX-Y.NYB"]
    data = {}
    try:
        df = yf.download(liste, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        if not df.empty:
            son = df.ffill().iloc[-1]
            for kur in liste:
                try:
                    f = son.get(kur)
                    if pd.notna(f):
                        data[kur.replace("TRY=X", "").replace("=X", "")] = round(float(f), 4)
                except: continue
    except: pass
    print(f"   -> ✅ Döviz Bitti: {len(data)} adet.")
    return data

def get_altin_site():
    print("6. Altın verileri çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.content, "html.parser")
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) > 2:
                isim = tds[0].get_text(strip=True)
                if "Ons" not in isim:
                    f = metni_sayiya_cevir(tds[2].get_text(strip=True))
                    if f > 0: data[isim] = f
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data)} adet.")
    return data

# ==============================================================================
# ANA ÇALIŞMA ALANI
# ==============================================================================
try:
    print("--- FİNAL FİNANS BOTU (TRADINGVIEW + CSV + CMC) ---")
    
    d_bist = get_bist_tradingview() # TradingView Motoru
    d_abd = get_sp500_dynamic()     # CSV -> Yahoo
    d_kripto = get_crypto_cmc(250)  # CMC API
    d_fon = get_tefas_data()        # TEFAS API
    d_doviz = get_doviz_yahoo()     # Yahoo
    d_altin = get_altin_site()      # Scraping

    final_paket = {
        "borsa_tr_tl": d_bist,
        "borsa_abd_usd": d_abd,
        "kripto_usd": d_kripto,
        "fon_tl": d_fon,
        "doviz_tl": d_doviz,
        "altin_tl": d_altin
    }

    if any(final_paket.values()):
        simdi = datetime.now()
        doc_id = simdi.strftime("%Y-%m-%d")
        saat = simdi.strftime("%H:%M")
        
        db.collection(u'market_history').document(doc_id).set(
            {u'hourly': {saat: final_paket}}, merge=True
        )
        
        toplam = sum(len(v) for v in final_paket.values())
        print(f"🎉 BAŞARILI: [{doc_id} - {saat}] Toplam {toplam} veri kaydedildi.")
    else:
        print("❌ HATA: Veri toplanamadı.")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
