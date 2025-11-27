import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf

# --- 1. AYARLAR & LİSTELER ---

# Kategorilere göre takip listesi
LISTE_BORSA_ABD = ["AAPL", "TSLA", "NVDA", "AMZN", "GOOGL"] # Dolar
LISTE_BORSA_TR  = ["THYAO.IS", "GARAN.IS", "SISE.IS", "EREGL.IS"] # TL
LISTE_KRIPTO    = ["BTC-USD", "ETH-USD", "SOL-USD"] # Dolar

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
    print("--- KATEGORİLİ FİNANS BOTU BAŞLADI ---")
    
    # ANA KUTULAR (Kategoriler)
    data_altin = {}
    data_doviz = {}
    data_kripto = {}
    data_borsa_abd = {}
    data_borsa_tr = {}

    # ---------------------------------------------------------
    # 1. YAHOO FINANCE (Kripto, ABD ve TR Borsası)
    # ---------------------------------------------------------
    print("1. Borsa ve Kripto verileri çekiliyor...")
    
    # Tüm sembolleri birleştirip tek seferde çekelim (Performans için)
    tum_semboller = LISTE_BORSA_ABD + LISTE_BORSA_TR + LISTE_KRIPTO
    tickers = yf.Tickers(' '.join(tum_semboller))
    
    for sembol in tum_semboller:
        try:
            bilgi = tickers.tickers[sembol].info
            fiyat = bilgi.get('currentPrice') or bilgi.get('regularMarketPrice') or bilgi.get('previousClose')
            
            if fiyat:
                # Hangi kategoriye aitse oraya koy
                if sembol in LISTE_KRIPTO:
                    temiz_isim = sembol.replace("-USD", "")
                    data_kripto[temiz_isim] = round(fiyat, 2)
                    
                elif sembol in LISTE_BORSA_ABD:
                    data_borsa_abd[sembol] = round(fiyat, 2)
                    
                elif sembol in LISTE_BORSA_TR:
                    temiz_isim = sembol.replace(".IS", "")
                    data_borsa_tr[temiz_isim] = round(fiyat, 2)
                    
        except Exception as e:
            print(f"⚠️ {sembol} alınamadı.")


    # ---------------------------------------------------------
    # 2. DÖVİZ (Genelpara)
    # ---------------------------------------------------------
    print("2. Döviz kurları çekiliyor...")
    try:
        r_doviz = requests.get("https://api.genelpara.com/embed/doviz.json", headers=headers, timeout=10)
        if r_doviz.status_code == 200:
            d_json = r_doviz.json()
            data_doviz["DOLAR"] = metni_sayiya_cevir(d_json.get('USD', {}).get('satis'))
            data_doviz["EURO"] = metni_sayiya_cevir(d_json.get('EUR', {}).get('satis'))
    except Exception as e:
        print(f"⚠️ Döviz Hatası: {e}")


    # ---------------------------------------------------------
    # 3. ALTIN (altin.doviz.com)
    # ---------------------------------------------------------
    print("3. Altın verileri çekiliyor...")
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
                        
                        if "Ons" not in isim: # Ons'u istememiştin
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
    
    # Tüm kutuları ana pakete koyuyoruz
    # Eğer veri boşsa o kutuyu göndermeyebiliriz veya boş {} gönderebiliriz.
    final_paket = {
        "altin": data_altin,
        "doviz": data_doviz,
        "kripto_usd": data_kripto,     # Uygulamada bilsinler diye _usd ekledim
        "borsa_abd_usd": data_borsa_abd,
        "borsa_tr_tl": data_borsa_tr
    }

    # Sadece en azından bir veri varsa kaydet
    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        doc_ref = db.collection(u'market_history').document(bugun_tarih)
        
        # Saatlik verinin içine kategorili paketi koyuyoruz
        kayit = {
            u'hourly': {
                su_an_saat_dakika: final_paket
            }
        }
        
        doc_ref.set(kayit, merge=True)
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler KATEGORİLİ olarak kaydedildi.")
        
        # Ekrana özet bas
        print("Özet:")
        print(f"- Altın: {len(data_altin)} adet")
        print(f"- Borsa TR: {len(data_borsa_tr)} adet")
        print(f"- Kripto: {len(data_kripto)} adet")
        
    else:
        print("❌ HATA: Hiçbir kategoriden veri gelmedi!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
