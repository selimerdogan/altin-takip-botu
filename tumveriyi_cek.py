import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd

# --- 1. AYARLAR & LİSTELER ---

# ABD ve Kripto için hala Yahoo kullanıyoruz (En iyisi bu)
ABD_TOP = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX"]
KRIPTO_TOP = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "XRP-USD", "DOGE-USD"]
DOVIZ_LISTE = ["USDTRY=X", "EURTRY=X"]

# Borsa İstanbul ve Altın Kaynakları (HTML Kazıma)
url_bist_tumu = "https://borsa.doviz.com/hisseler"
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

# --- YARDIMCI FONKSİYON ---
def metni_sayiya_cevir(metin):
    try:
        # 1.250,50 TL -> 1250.50
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').strip()
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

try:
    print("--- FİNANS MERKEZİ BAŞLADI (KAYNAK: HİBRİT) ---")
    
    # SONUÇ KUTULARI
    data_altin = {}
    data_doviz = {}
    data_kripto = {}
    data_borsa_abd = {}
    data_borsa_tr = {}

    # ---------------------------------------------------------
    # 1. BORSA İSTANBUL (TÜM HİSSELER - borsa.doviz.com)
    # ---------------------------------------------------------
    print("1. Borsa İstanbul (TÜMÜ) çekiliyor...")
    try:
        session = requests.Session()
        # Siteye bağlan
        resp_bist = session.get(url_bist_tumu, headers=headers, timeout=25)
        
        if resp_bist.status_code == 200:
            soup = BeautifulSoup(resp_bist.content, "html.parser")
            
            # Tabloyu bul
            # Genellikle id='stocks' veya class='stock-table' olur.
            # Garanti olsun diye tüm satırları geziyoruz.
            satirlar = soup.find_all("tr")
            
            for satir in satirlar:
                cols = satir.find_all("td")
                # Tablo yapısı genellikle: [0]:İsim/Sembol, [1]:Son Fiyat, [2]:Değişim...
                if len(cols) > 1:
                    try:
                        # Doviz.com'da hisse adı ve sembolü bazen aynı sütundadır.
                        # Örnek: "THYAO\nTurk Hava Yollari" gibi.
                        # Biz sadece ilk kelimeyi (Sembolü) alacağız.
                        
                        ham_isim = cols[0].get_text(strip=True)
                        sembol = ham_isim.split()[0] # İlk kelimeyi al (Örn: THYAO)
                        
                        # Fiyat sütunu (Genellikle 1. index, bazen 2)
                        # Sitede Son Fiyat genellikle 2. sıradadır.
                        fiyat_txt = cols[1].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(fiyat_txt)
                        
                        # Sadece geçerli veri varsa ve uzunluk mantıklıysa (Sembol 3-6 harf olur)
                        if fiyat > 0 and 2 < len(sembol) < 10:
                            data_borsa_tr[sembol] = fiyat
                            
                    except:
                        continue
            
            print(f"✅ BIST Verileri Alındı: Toplam {len(data_borsa_tr)} hisse.")
        else:
            print(f"⚠️ Borsa sitesine girilemedi: {resp_bist.status_code}")

    except Exception as e:
        print(f"⚠️ BIST Hatası: {e}")


    # ---------------------------------------------------------
    # 2. GLOBAL PİYASALAR (Yahoo Finance)
    # ---------------------------------------------------------
    print("2. Global (ABD, Kripto, Döviz) çekiliyor...")
    try:
        # ABD ve Kripto listesini birleştir
        global_semboller = ABD_TOP + KRIPTO_TOP + DOVIZ_LISTE
        
        # Toplu İndir
        df = yf.download(global_semboller, period="1d", progress=False)['Close']
        
        # Son fiyatları al (Tek bir satır dönerse Series, çok satırsa DataFrame olur, iloc[-1] ile sonuncuyu alırız)
        if not df.empty:
            # Tek bir sembol varsa df bir Series olabilir, kontrol edelim
            if isinstance(df, pd.Series):
                # Tek veri geldiyse (Nadir olur ama önlem)
                 pass 
            else:
                son_fiyatlar = df.iloc[-1]
                
                for sembol in global_semboller:
                    try:
                        fiyat = son_fiyatlar.get(sembol)
                        if pd.notna(fiyat):
                            fiyat = round(float(fiyat), 2)
                            
                            if sembol in ABD_TOP:
                                data_borsa_abd[sembol] = fiyat
                            elif sembol in KRIPTO_TOP:
                                temiz = sembol.replace("-USD", "")
                                data_kripto[temiz] = fiyat
                            elif sembol in DOVIZ_LISTE:
                                if "USD" in sembol: data_doviz["DOLAR"] = fiyat
                                if "EUR" in sembol: data_doviz["EURO"] = fiyat
                    except:
                        continue
                        
        print(f"✅ Global Veriler Alındı.")

    except Exception as e:
        print(f"⚠️ Yahoo Hatası: {e}")


    # ---------------------------------------------------------
    # 3. ALTIN (Mevcut Sistem)
    # ---------------------------------------------------------
    print("3. Altın verileri çekiliyor...")
    try:
        session = requests.Session()
        r_altin = session.get(url_altin, headers=headers, timeout=20)
        if r_altin.status_code == 200:
            soup = BeautifulSoup(r_altin.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(cols[2].get_text(strip=True))
                        if "Ons" not in isim and fiyat > 0:
                            data_altin[isim] = fiyat
                    except: continue
    except Exception as e:
        print(f"⚠️ Altın Hatası: {e}")

    # ---------------------------------------------------------
    # KAYIT
    # ---------------------------------------------------------
    final_paket = {
        "altin": data_altin,
        "doviz": data_doviz,
        "kripto_usd": data_kripto,
        "borsa_abd_usd": data_borsa_abd,
        "borsa_tr_tl": data_borsa_tr
    }

    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        db.collection(u'market_history').document(bugun_tarih).set(
            {u'hourly': {su_an_saat_dakika: final_paket}}, merge=True
        )
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler kaydedildi.")
        print(f"📊 Özet: BIST({len(data_borsa_tr)}), ABD({len(data_borsa_abd)}), Kripto({len(data_kripto)}), Altın({len(data_altin)})")
    else:
        print("❌ HATA: Hiç veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
