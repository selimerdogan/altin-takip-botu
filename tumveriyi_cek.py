import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd

# --- AYARLAR ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
}

# --- FIREBASE BAĞLANTISI ---
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json bulunamadı!")
    sys.exit(1)

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
        # TL, $, %, harfler ve boşlukları temizle
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        # Türkçe formatı (1.250,50) -> İngilizce formata (1250.50) çevir
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# ==============================================================================
# 1. BIST İÇİN ÖZEL KAZIYICI (SCRAFER)
# ==============================================================================
def get_bist_from_web():
    """
    borsa.doviz.com/hisseler adresindeki tabloyu canlı okur.
    Böylece liste elle yazılmaz, yeni halka arzlar otomatik gelir.
    """
    url = "https://borsa.doviz.com/hisseler"
    bist_data = {}
    
    try:
        print("   -> Borsa İstanbul verileri siteden kazınıyor...")
        session = requests.Session()
        resp = session.get(url, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            
            # Tablodaki tüm satırları bul
            satirlar = soup.find_all("tr")
            
            for satir in satirlar:
                cols = satir.find_all("td")
                # Tablo Yapısı: [0] İsim/Kod, [1] Son Fiyat, [2] Değişim...
                if len(cols) > 1:
                    try:
                        # İsim sütunu genellikle "THYAO\nTurk Hava Yollari" şeklindedir.
                        # Biz sadece ilk kelimeyi (Kodu) alacağız.
                        ham_isim = cols[0].get_text(strip=True)
                        kod = ham_isim.split()[0] # İlk kelimeyi al (Örn: THYAO)
                        
                        fiyat_txt = cols[1].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(fiyat_txt)
                        
                        # Kod uzunluğu mantıklıysa (3-6 karakter) ve fiyat varsa ekle
                        if fiyat > 0 and 2 < len(kod) < 10:
                            bist_data[kod] = fiyat
                    except:
                        continue
            print(f"   -> ✅ Siteden {len(bist_data)} adet Türk hissesi çekildi.")
        else:
            print(f"   -> ⚠️ Siteye erişilemedi (Kod: {resp.status_code})")
            
    except Exception as e:
        print(f"   -> ⚠️ BIST Kazıma Hatası: {e}")
        
    return bist_data

# ==============================================================================
# 2. SABİT LİSTELER (ABD, KRİPTO, DÖVİZ - Yahoo Finance)
# ==============================================================================

LISTE_ABD = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK-B", "LLY", "AVGO", "V", "JPM", "XOM", "WMT", "UNH", "MA", "PG", "JNJ", "HD", "MRK", "COST", "ABBV", "CVX", "CRM", "BAC", "AMD", "PEP", "KO", "NFLX", "ADBE", "DIS", "MCD", "CSCO", "TMUS", "ABT", "INTC", "INTU", "CMCSA", "PFE", "NKE", "WFC", "QCOM", "TXN", "DHR", "PM", "UNP", "IBM", "AMGN", "GE", "HON", "BA", "SPY", "QQQ", "UBER", "PLTR",
    "LIN", "ACN", "RTX", "VZ", "T", "CAT", "LOW", "BKNG", "NEE", "GS", "MS", "BMY", "DE", "MDT", "SCHW", "BLK", "TJX", "PGR", "COP", "ISRG", "LMT", "ADP", "AXP", "MMC", "GILD", "VRTX", "C", "MDLZ", "ADI", "REGN", "LRCX", "CI", "CVS", "BSX", "ZTS", "AMT", "ETN", "SLB", "FI", "BDX", "SYK", "CB", "EOG", "TM", "SO", "CME", "MU", "KLAC", "PANW", "MO", "SHW", "SNPS", "EQIX", "CDNS", "ITW", "DUK", "CL", "APH", "PYPL", "CSX", "PH", "TGT", "USB", "ICE", "NOC", "WM", "FCX", "GD", "NXPI", "ORLY", "HCA", "MCK", "EMR", "MAR", "PNC", "PSX", "BDX", "ROP", "NSC", "GM", "FDX", "MCO", "AFL", "CARR", "ECL", "APD", "AJG", "MSI", "AZO", "TT", "WMB", "TFC", "COF", "PCAR", "D", "SRE", "AEP", "HLT", "O", "TRV", "MET", "PSA", "PAYX", "ROST", "KMB", "JCI", "URI", "ALL", "PEG", "ED", "XEL", "GWW", "YUM", "FAST", "WELL", "AMP", "DLR", "VLO", "AME", "CMI", "FIS", "ILMN", "AIG", "KR", "PPG", "KMI", "DFS", "EXC", "LUV", "DAL"
]

LISTE_KRIPTO = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD", "DOGE-USD",
    "TRX-USD", "DOT-USD", "LINK-USD", "LTC-USD", "SHIB-USD", "ATOM-USD",
    "XLM-USD", "NEAR-USD", "INJ-USD", "FIL-USD", "HBAR-USD", "LDO-USD", "ARB-USD",
    "ALGO-USD", "SAND-USD", "QNT-USD", "VET-USD", "OP-USD", "EGLD-USD", "AAVE-USD",
    "THETA-USD", "AXS-USD", "MANA-USD", "EOS-USD", "FLOW-USD", "XTZ-USD",
    "MKR-USD", "SNX-USD", "NEO-USD", "JASMY-USD", "KLAY-USD", "GALA-USD", "CFX-USD",
    "CHZ-USD", "CRV-USD", "ZEC-USD", "XEC-USD", "IOTA-USD",
    "LUNC-USD", "BTT-USD", "MINA-USD", "DASH-USD", "CAKE-USD", "RUNE-USD", "KAVA-USD",
    "ENJ-USD", "ZIL-USD", "BAT-USD", "TWT-USD", "QTUM-USD", "CELO-USD", "RVN-USD",
    "LRC-USD", "ENS-USD", "CVX-USD", "YFI-USD", "ANKR-USD", "1INCH-USD", "HOT-USD"
]

LISTE_DOVIZ = [
    "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X",
    "EURUSD=X", "GBPUSD=X"
]

# ==============================================================================
# ANA PROGRAM
# ==============================================================================

try:
    print("--- ULTRA FİNANS BOTU (BIST SİTEDEN ÇEKİLİYOR) ---")
    
    # 1. BIST VERİSİNİ SİTEDEN ÇEK
    data_borsa_tr = get_bist_from_web()
    
    # 2. DİĞERLERİNİ YAHOO'DAN TOPLU ÇEK
    # BIST listesini buraya eklemiyoruz çünkü onu yukarıda hallettik.
    tum_semboller = LISTE_ABD + LISTE_KRIPTO + LISTE_DOVIZ
    print(f"2. Global Piyasalar (ABD, Kripto, Döviz) Yahoo'dan çekiliyor... ({len(tum_semboller)} adet)")
    
    df = yf.download(tum_semboller, period="5d", progress=False, threads=True, auto_adjust=True)['Close']
    
    data_borsa_abd = {}
    data_kripto = {}
    data_doviz = {}
    
    if not df.empty:
        df_dolu = df.ffill()
        son_fiyatlar = df_dolu.iloc[-1]
        
        for sembol in tum_semboller:
            try:
                fiyat = son_fiyatlar.get(sembol)
                if pd.notna(fiyat):
                    fiyat = round(float(fiyat), 2)
                    
                    if sembol in LISTE_ABD:
                        data_borsa_abd[sembol] = fiyat
                    elif sembol in LISTE_KRIPTO:
                        data_kripto[sembol.replace("-USD", "")] = fiyat
                    elif sembol in LISTE_DOVIZ:
                        data_doviz[sembol.replace("TRY=X", "").replace("=X", "")] = fiyat
            except: continue
    
    print(f"   -> ✅ Yahoo Bitti: ABD({len(data_borsa_abd)}), Kripto({len(data_kripto)}), Döviz({len(data_doviz)})")

    # 3. ALTIN VERİSİNİ ÇEK
    print("3. Altın verileri siteden çekiliyor...")
    data_altin = {}
    try:
        session = requests.Session()
        r = session.get("https://altin.doviz.com/", headers=headers, timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(cols[2].get_text(strip=True))
                            if fiyat > 0: data_altin[isim] = fiyat
                    except: continue
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data_altin)} adet")

    # 4. KAYIT
    final_paket = {
        "borsa_tr_tl": data_borsa_tr,
        "borsa_abd_usd": data_borsa_abd,
        "kripto_usd": data_kripto,
        "doviz_tl": data_doviz,
        "altin_tl": data_altin
    }

    # BIST dahil herhangi bir veri varsa kaydet
    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        db.collection(u'market_history').document(bugun_tarih).set(
            {u'hourly': {su_an_saat_dakika: final_paket}}, merge=True
        )
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Tüm veriler kaydedildi.")
    else:
        print("❌ HATA: Hiçbir veri toplanamadı!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
