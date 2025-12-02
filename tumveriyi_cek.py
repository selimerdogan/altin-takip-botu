import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import sys
import os
import yfinance as yf
import pandas as pd
import warnings

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
        # 1.250,50 -> 1250.50 formatına çevir
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# ==============================================================================
# 1. DÖVİZ (KUR.DOVIZ.COM - YENİ!)
# ==============================================================================
def get_doviz_site():
    print("1. Döviz Kurları (kur.doviz.com) taranıyor...")
    url = "https://kur.doviz.com/"
    data = {}
    
    try:
        r = requests.get(url, headers=headers_general, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            
            # Tablodaki satırları bul
            # Genellikle tablo yapısı: İsim | Kod | Alış | Satış
            satirlar = soup.find_all("tr")
            
            for row in satirlar:
                cols = row.find_all("td")
                # Satırda yeterli sütun var mı?
                if len(cols) >= 4:
                    try:
                        # Sütun yapıları siteden siteye değişebilir ama genelde:
                        # 0: İsim (Img + Text)
                        # 1: Kod (USD)
                        # 2: Alış
                        # 3: Satış
                        
                        kod = cols[1].get_text(strip=True) # USD, EUR
                        satis_fiyati = cols[3].get_text(strip=True) # Satış fiyatını alalım
                        
                        # Filtre: Sadece 3 harfli standart kodları al (Gereksizleri ele)
                        if len(kod) == 3 and kod.isalpha():
                            fiyat = metni_sayiya_cevir(satis_fiyati)
                            if fiyat > 0:
                                data[kod] = fiyat
                    except: continue
            
            print(f"   -> ✅ Döviz Başarılı: {len(data)} adet kur çekildi.")
        else:
            print(f"   -> ⚠️ Site Hatası: {r.status_code}")
            
    except Exception as e:
        print(f"   -> ⚠️ Döviz Bağlantı Hatası: {e}")
        
    return data

# ==============================================================================
# 2. BIST (TRADINGVIEW SCANNER)
# ==============================================================================
def get_bist_tradingview():
    print("2. Borsa İstanbul (TV Scanner) taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close"],
        "range": [0, 1000]
    }
    data_bist = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        if r.status_code == 200:
            for h in r.json().get('data', []):
                try:
                    d = h.get('d', [])
                    if len(d) > 1:
                        data_bist[d[0]] = float(d[1])
                except: continue
            print(f"   -> ✅ BIST Başarılı: {len(data_bist)} hisse.")
    except: pass
    return data_bist

# ==============================================================================
# 3. YATIRIM FONLARI (TEFAS - AKILLI MOD)
# ==============================================================================
def get_tefas_data():
    print("3. Yatırım Fonları (TEFAS) taranıyor...")
    url_api = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReturns"
    session = requests.Session()
    tefas_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "Origin": "https://www.tefas.gov.tr", "Content-Type": "application/json; charset=UTF-8"
    }
    
    try: session.get("https://www.tefas.gov.tr/FonKarsilastirma.aspx", headers=tefas_headers, timeout=10)
    except: pass

    simdi = datetime.now()
    for i in range(7):
        tarih = (simdi - timedelta(days=i))
        if tarih.year > datetime.now().year: tarih = tarih.replace(year=datetime.now().year)
        tarih_str = tarih.strftime("%d.%m.%Y")
        
        try:
            payload = {"calismatipi": "2", "fontip": "YAT", "bastarih": tarih_str, "bittarih": tarih_str}
            r = session.post(url_api, json=payload, headers=tefas_headers, timeout=30)
            if r.status_code == 200:
                d = r.json().get('data', [])
                if len(d) > 50:
                    fonlar = {}
                    for f in d:
                        try:
                            val = float(str(f['FIYAT']).replace(',', '.'))
                            fonlar[f['FONKODU']] = val
                        except: continue
                    print(f"   -> ✅ TEFAS Başarılı ({tarih_str}): {len(fonlar)} fon.")
                    return fonlar
        except: continue
    print("   -> ❌ TEFAS verisi bulunamadı.")
    return {}

# ==============================================================================
# 4. ABD BORSASI (S&P 500 - CSV + YAHOO)
# ==============================================================================
def get_sp500_dynamic():
    print("4. ABD Borsası (S&P 500) taranıyor...")
    url_csv = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
    data_abd = {}
    try:
        s = requests.get(url_csv).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        liste_sp500 = [x.replace('.', '-') for x in df['Symbol'].tolist()]
        black_list = ['WBA', 'DISCA', 'DISCK']
        liste_sp500 = [x for x in liste_sp500 if x not in black_list]

        df_yahoo = yf.download(liste_sp500, period="5d", progress=False, threads=True, auto_adjust=True, ignore_tz=True)['Close']
        if not df_yahoo.empty:
            son = df_yahoo.ffill().iloc[-1]
            for sembol in liste_sp500:
                try:
                    val = son.get(sembol)
                    if pd.notna(val): data_abd[sembol] = round(float(val), 2)
                except: continue
        print(f"   -> ✅ S&P 500 Başarılı: {len(data_abd)} hisse.")
    except: pass
    return data_abd

# ==============================================================================
# 5. KRİPTO (CMC API)
# ==============================================================================
def get_crypto_cmc(limit=250):
    if not CMC_API_KEY: return {}
    print(f"5. Kripto Piyasası (CMC) taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data_kripto = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            for coin in r.json()['data']:
                data_kripto[f"{coin['symbol']}-USD"] = round(float(coin['quote']['USD']['price']), 4)
            print(f"   -> ✅ CMC Başarılı: {len(data_kripto)} coin.")
    except: pass
    return data_kripto

# ==============================================================================
# 6. ALTIN (SİTEDEN)
# ==============================================================================
def get_altin_site():
    print("6. Altın verileri çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
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
# KAYIT
# ==============================================================================
try:
    print("--- MEGA FİNANS BOTU (DOVIZ.COM ENTEGRELİ) ---")
    
    final_paket = {
        "doviz_tl": get_doviz_site(),          # Yeni Kaynak!
        "borsa_tr_tl": get_bist_tradingview(),
        "borsa_abd_usd": get_sp500_dynamic(),
        "kripto_usd": get_crypto_cmc(250),
        "fon_tl": get_tefas_data(),
        "altin_tl": get_altin_site(),
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
