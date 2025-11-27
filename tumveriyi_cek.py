import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf

# --- 1. LİSTELER (HEPSİNİ TEK YERDE YÖNETİYORUZ) ---

# ABD Borsası (Dolar döner)
LISTE_BORSA_ABD = ["AAPL", "TSLA", "NVDA", "AMZN", "GOOGL"]

# Türk Borsası (TL döner)
LISTE_BORSA_TR  = ["THYAO.IS", "GARAN.IS", "SISE.IS", "EREGL.IS"]

# Kripto Paralar (Dolar döner)
LISTE_KRIPTO    = ["BTC-USD", "ETH-USD", "SOL-USD"]

# Döviz (TL döner) - YENİ EKLENDİ (Yahoo Kodları)
# USDTRY=X -> Dolar/TL
# EURTRY=X -> Euro/TL
LISTE_DOVIZ     = ["USDTRY=X", "EURTRY=X"] 

url_altin = "https://altin.doviz.com/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.google.com/"
}

# --- FIREBASE BAĞLANTISI ---
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json yok!")
    sys.exit(1)

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase hatası: {e}")
    sys.exit(1)

# --- YARDIMCI FONKSİYONLAR ---
def metni_sayiya_cevir(metin):
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').strip()
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# --- ANA İŞLEM ---
try:
    print("--- FULL PAKET FİNANS BOTU BAŞLADI ---")
    
    # VERİ KUTULARI
    data_altin = {}
    data_doviz = {}
    data_kripto = {}
    data_borsa_abd = {}
    data_borsa_tr = {}

    # ---------------------------------------------------------
    # 1. YAHOO FINANCE (BORSA + KRİPTO + DÖVİZ) - HEPSİ BİR ARADA
    # ---------------------------------------------------------
    print("1. Yahoo Finance verileri (Borsa, Kripto, Döviz) çekiliyor...")
    
    # Tüm listeleri birleştir
    tum_semboller = LISTE_BORSA_ABD + LISTE_BORSA_TR + LISTE_KRIPTO + LISTE_DOVIZ
    
    # Tek seferde internete sor (Hız için)
    tickers = yf.Tickers(' '.join(tum_semboller))
    
    for sembol in tum_semboller:
        try:
            bilgi = tickers.tickers[sembol].info
            # Fiyatı bulmaya çalış (Farklı isimlerde gelebiliyor)
            fiyat = bilgi.get('currentPrice') or bilgi.get('regularMarketPrice') or bilgi.get('previousClose')
            
            if fiyat:
                fiyat = round(fiyat, 2)
                
                # --- KUTULARA DAĞITIM ---
                
                # A) DÖVİZ
                if sembol in LISTE_DOVIZ:
                    if sembol == "USDTRY=X":
                        data_doviz["DOLAR"] = fiyat
                    elif sembol == "EURTRY=X":
                        data_doviz["EURO"] = fiyat
                
                # B) KRİPTO
                elif sembol in LISTE_KRIPTO:
                    temiz_isim = sembol.replace("-USD", "")
                    data_kripto[temiz_isim] = fiyat
                
                # C) ABD BORSASI
                elif sembol in LISTE_BORSA_ABD:
                    data_borsa_abd[sembol] = fiyat
                
                # D) TÜRK BORSASI
                elif sembol in LISTE_BORSA_TR:
                    temiz_isim = sembol.replace(".IS", "")
                    data_borsa_tr[temiz_isim] = fiyat
                    
        except Exception as e:
            print(f"⚠️ {sembol} alınamadı.")


    # ---------------------------------------------------------
    # 2. ALTIN (altin.doviz.com) - Kazımaya Devam
    # ---------------------------------------------------------
    print("2. Altın verileri çekiliyor...")
    try:
        session = requests.Session()
        r_altin = session.get(url_altin, headers=headers, timeout=20)
        
        if r_altin.status_code == 200:
            soup = BeautifulSoup(r_altin.content, "html.parser")
            satirlar = soup.find_all("tr")
            
            for satir in satirlar:
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        fiyat_txt = cols[2].get_text(strip=True)
                        
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(fiyat_txt)
                            if fiyat > 0:
                                data_altin[isim] = fiyat
                    except:
                        continue
    except Exception as e:
        print(f"⚠️ Altın Hatası: {e}")

    # ---------------------------------------------------------
    # PAKETLEME VE KAYIT
    # ---------------------------------------------------------
    
    final_paket = {
        "altin": data_altin,
        "doviz": data_doviz,
        "kripto_usd": data_kripto,
        "borsa_abd_usd": data_borsa_abd,
        "borsa_tr_tl": data_borsa_tr
    }

    # Herhangi bir veri varsa kaydet
    # (recursive check: içerdeki sözlüklerden en az biri dolu mu?)
    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        doc_ref = db.collection(u'market_history').document(bugun_tarih)
        
        kayit = {
            u'hourly': {
                su_an_saat_dakika: final_paket
            }
        }
        
        doc_ref.set(kayit, merge=True)
        
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler kaydedildi.")
        print("KONTROL:")
        print(f"💵 Döviz: {data_doviz}") # Loglarda Doları görelim
        print(f"🟡 Altın: {len(data_altin)} adet")
        print(f"📈 Borsa TR: {len(data_borsa_tr)} adet")
        
    else:
        print("❌ HATA: Hiçbir veri gelmedi!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
