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
# Sitelerin bot engeline takılmaması için kimlik bilgisi
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
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

# --- YARDIMCI FONKSİYON: METİN TEMİZLEME ---
def metni_sayiya_cevir(metin):
    try:
        # TL, $, %, harfler ve boşlukları temizle
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        # 1.250,50 -> 1250.50 (Türkçe format)
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# --- VERİ ÇEKME FONKSİYONLARI ---

def get_bist_all():
    """Borsa İstanbul'daki TÜM hisseleri çeker"""
    url = "https://borsa.doviz.com/hisseler"
    veri = {}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 1:
                    try:
                        # İsim sütunundan sembolü ayıkla (Örn: THYAO\nTurk Hava Yollari)
                        ham_isim = cols[0].get_text(strip=True)
                        sembol = ham_isim.split()[0] # İlk kelime semboldür
                        fiyat = metni_sayiya_cevir(cols[1].get_text(strip=True))
                        
                        if fiyat > 0 and 2 < len(sembol) < 10:
                            veri[sembol] = fiyat
                    except: continue
    except Exception as e:
        print(f"BIST Hatası: {e}")
    return veri

def get_kripto_all():
    """En popüler ~100 Kripto Parayı çeker"""
    url = "https://www.doviz.com/kripto-paralar"
    veri = {}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        # Sembol genellikle ilk sütunda gizlidir veya text içindedir
                        # Doviz.com yapısı: 1. sıra İsim (Bitcoin BTC), 2. sıra Fiyat ($95.000)
                        isim_blok = cols[0].get_text(" ", strip=True) # "Bitcoin BTC"
                        sembol = isim_blok.split()[-1] # Sondaki kelimeyi al: BTC
                        
                        fiyat_txt = cols[1].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(fiyat_txt)
                        
                        if fiyat > 0:
                            veri[sembol] = fiyat
                    except: continue
    except Exception as e:
        print(f"Kripto Hatası: {e}")
    return veri

def get_doviz_all():
    """Tüm Serbest Piyasa Döviz Kurlarını çeker"""
    url = "https://www.doviz.com/serbest-piyasa-doviz-kurlari"
    veri = {}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            # Tabloyu bul (class="currencies" veya benzeri)
            # Garantili yöntem: item-name class'ına sahip olanları bulmak
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(cols[2].get_text(strip=True)) # Satış fiyatı
                        
                        # "Dolar", "Euro", "Sterlin" gibi temiz isimler gelir
                        if fiyat > 0:
                            veri[isim] = fiyat
                    except: continue
    except Exception as e:
        print(f"Döviz Hatası: {e}")
    return veri

def get_abd_sp500():
    """ABD'nin en büyük 500 şirketini (S&P 500) Wikipedia'dan bulup Yahoo'dan çeker"""
    veri = {}
    try:
        # 1. Wikipedia'dan güncel listeyi al (Scraping)
        print("   -> S&P 500 listesi Wikipedia'dan alınıyor...")
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(sp500_url)
        df_symbols = tables[0] # İlk tablo şirket listesidir
        sembol_listesi = df_symbols['Symbol'].tolist()
        
        # Bazı semboller Yahoo'da farklıdır (BRK.B -> BRK-B)
        sembol_listesi = [s.replace('.', '-') for s in sembol_listesi]
        
        print(f"   -> Toplam {len(sembol_listesi)} ABD hissesi Yahoo'dan indiriliyor (Bu biraz sürebilir)...")
        
        # 2. Yahoo Finance ile Toplu İndir (Batch Download)
        # Hepsini tek seferde çekiyoruz
        df_yahoo = yf.download(sembol_listesi, period="1d", progress=False)['Close']
        
        if not df_yahoo.empty:
            son_fiyatlar = df_yahoo.iloc[-1]
            for sembol in sembol_listesi:
                try:
                    fiyat = son_fiyatlar.get(sembol)
                    if pd.notna(fiyat):
                        veri[sembol] = round(float(fiyat), 2)
                except: continue
    except Exception as e:
        print(f"ABD Borsa Hatası: {e}")
    return veri

def get_altin_all():
    """Altın Verileri (Mevcut)"""
    url = "https://altin.doviz.com/"
    veri = {}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        fiyat = metni_sayiya_cevir(cols[2].get_text(strip=True))
                        if "Ons" not in isim and fiyat > 0:
                            veri[isim] = fiyat
                    except: continue
    except: pass
    return veri

# --- ANA PROGRAM ---
try:
    print("--- ULTIMATE FİNANS BOTU ÇALIŞIYOR ---")
    
    # 1. BIST (TR)
    print("1. Borsa İstanbul taranıyor...")
    data_bist = get_bist_all()
    print(f"   ✅ {len(data_bist)} hisse alındı.")
    
    # 2. KRİPTO
    print("2. Kripto piyasası taranıyor...")
    data_kripto = get_kripto_all()
    print(f"   ✅ {len(data_kripto)} coin alındı.")
    
    # 3. DÖVİZ
    print("3. Tüm Döviz kurları taranıyor...")
    data_doviz = get_doviz_all()
    print(f"   ✅ {len(data_doviz)} kur alındı.")
    
    # 4. ABD BORSASI (S&P 500)
    print("4. ABD Borsası (S&P 500) taranıyor...")
    data_abd = get_abd_sp500()
    print(f"   ✅ {len(data_abd)} ABD hissesi alındı.")
    
    # 5. ALTIN
    print("5. Altın verileri taranıyor...")
    data_altin = get_altin_all()
    print(f"   ✅ {len(data_altin)} altın türü alındı.")

    # PAKETLEME
    final_paket = {
        "borsa_tr_tl": data_bist,
        "borsa_abd_usd": data_abd,
        "kripto_usd": data_kripto,
        "doviz_tl": data_doviz,
        "altin_tl": data_altin
    }

    # KAYIT
    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        doc_ref = db.collection(u'market_history').document(bugun_tarih)
        doc_ref.set({u'hourly': {su_an_saat_dakika: final_paket}}, merge=True)
        
        print(f"🎉 TEBRİKLER: [{bugun_tarih} - {su_an_saat_dakika}] Toplam 1000+ veri kaydedildi.")
    else:
        print("❌ HATA: Veri toplanamadı.")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
