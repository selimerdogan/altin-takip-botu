import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json

# --- 1. AYARLAR ---
# Garanti Kaynak (JSON)
url_api_altin = "https://api.genelpara.com/embed/altin.json"
url_api_doviz = "https://api.genelpara.com/embed/doviz.json"

# Detay Kaynak (HTML - Bilezik vb. için)
# altin.in sitesi daha hafiftir ve bot koruması düşüktür.
url_detay = "https://altin.in/"

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
    print(f"Firebase Bağlantı Hatası: {e}")
    sys.exit(1)

# --- YARDIMCI FONKSİYONLAR ---
def metni_sayiya_cevir(metin):
    try:
        metin = str(metin).strip()
        # Temizlik: 1.250,50 -> 1250.50
        temiz = metin.replace('TL', '').replace('USD', '').replace('%', '').strip()
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# --- ANA VERİ DEPOSU ---
veri_sozlugu = {}
guncel_dolar = 0.0

try:
    print("--- İŞLEM BAŞLIYOR (MELEZ MOD) ---")

    # --- ADIM 1: GARANTİ VERİLER (API) ---
    try:
        print("1. API verileri (Gram, Çeyrek, Dolar) çekiliyor...")
        
        # Doları Al
        r_doviz = requests.get(url_api_doviz, headers=headers, timeout=10)
        if r_doviz.status_code == 200:
            d_json = r_doviz.json()
            guncel_dolar = metni_sayiya_cevir(d_json.get('USD', {}).get('satis'))
            print(f"Dolar Kuru: {guncel_dolar}")
        
        # Altınları Al
        r_altin = requests.get(url_api_altin, headers=headers, timeout=10)
        if r_altin.status_code == 200:
            a_json = r_altin.json()
            
            # İsim Eşleştirme Haritası
            api_haritasi = {
                "GA": "Gram Altın",
                "C": "Çeyrek Altın",
                "Y": "Yarım Altın",
                "T": "Tam Altın",
                "CUM": "Cumhuriyet Altını",
                "ATA": "Ata Altın",
                "ONS": "Ons Altın"
            }
            
            for kod, veri in a_json.items():
                if kod in api_haritasi:
                    isim = api_haritasi[kod]
                    fiyat = metni_sayiya_cevir(veri.get('satis'))
                    
                    # Ons Çevirisi
                    if kod == "ONS" and fiyat < 5000:
                        veri_sozlugu[isim] = round(fiyat * guncel_dolar, 2)
                    else:
                        veri_sozlugu[isim] = fiyat
                        
            print(f"API'den {len(veri_sozlugu)} adet temel veri alındı.")
            
    except Exception as e:
        print(f"UYARI: API verisi çekilirken hata oldu: {e}")

    # --- ADIM 2: DETAY VERİLER (HTML - BİLEZİK VB.) ---
    # Burası hata verirse programı DURDURMAYACAĞIZ, sadece pas geçeceğiz.
    try:
        print("2. Detay veriler (Bilezik, Reşat) altin.in sitesinden deneniyor...")
        r_detay = requests.get(url_detay, headers=headers, timeout=15)
        
        if r_detay.status_code == 200:
            soup = BeautifulSoup(r_detay.content, "html.parser")
            
            # altin.in sitesindeki veri ID'leri
            ozel_veriler = {
                "22 Ayar Bilezik": "bilezik22",
                "14 Ayar Altın": "altin14",
                "Gremse Altın": "gremse",
                "Reşat Altın": "resat"
            }
            
            eklenen_sayisi = 0
            for isim, site_id in ozel_veriler.items():
                # Sitede genellikle "c_bilezik22_s" ID'si satış fiyatını tutar
                # Veya "li" etiketleri içinde arama yapabiliriz.
                # altin.in yapısı: <li id="c_bilezik22_s">2.500,50</li>
                
                # Hem normal ID hem de 'c_' önekiyle deniyoruz
                element = soup.find(id=f"c_{site_id}_s")
                if not element:
                    element = soup.find(id=site_id)
                
                if element:
                    fiyat_txt = element.text
                    fiyat = metni_sayiya_cevir(fiyat_txt)
                    if fiyat > 0:
                        veri_sozlugu[isim] = fiyat
                        eklenen_sayisi += 1
                        print(f"Eklenen: {isim} -> {fiyat}")
            
            print(f"HTML Tarama Bitti. {eklenen_sayisi} adet ekstra veri eklendi.")
        else:
            print(f"UYARI: altin.in sitesi erişimi engelledi (Kod: {r_detay.status_code}). Sadece API verileri kullanılacak.")
            
    except Exception as e:
        print(f"UYARI: Detay veriler alınamadı (GitHub IP engeli olabilir). Devam ediliyor... Hata: {e}")

    # --- ADIM 3: KAYDETME (HER DURUMDA ÇALIŞIR) ---
    if len(veri_sozlugu) > 0:
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        doc_ref = db.collection(u'market_history').document(bugun_tarih)
        
        kayit = {
            u'hourly': {
                su_an_saat_dakika: veri_sozlugu
            }
        }
        
        doc_ref.set(kayit, merge=True)
        print(f"✅ BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Toplam {len(veri_sozlugu)} veri kaydedildi.")
    else:
        print("❌ HATA: Hiçbir kaynaktan veri alınamadı.")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK PROGRAM HATASI: {e}")
    sys.exit(1)
