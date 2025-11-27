import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os

# --- 1. AYARLAR ---
# Hedef sitemiz yeniden burası
url = "https://altin.doviz.com/"

# Site bizi insan sansın diye güçlü başlıklar
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
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
    print(f"HATA: Firebase bağlantısı başarısız: {e}")
    sys.exit(1)

# --- YARDIMCI FONKSİYON: TERTEMİZ SAYI YAPAR ---
def metni_sayiya_cevir(metin):
    try:
        # Gereksiz her şeyi at (TL, $, harfler, boşluklar)
        temiz = metin.replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        # 1.250,50 -> 1250.50 formatına çevir
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# --- ANA İŞLEM ---
try:
    print("--- Altin.doviz.com Veri Çekme Başladı ---")
    
    # Session kullanarak bağlanmak bazen engelleri aşmaya yarar
    session = requests.Session()
    response = session.get(url, headers=headers, timeout=20)
    
    if response.status_code != 200:
        print(f"Siteye erişilemedi! Kod: {response.status_code}")
        sys.exit(1)

    # HTML'i parçala
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Sitedeki o meşhur tabloyu bulalım
    # Genellikle 'table' etiketidir veya id="golds" olabilir.
    # En garanti yöntem sayfadaki tüm 'tr' (satır) etiketlerini gezmektir.
    satirlar = soup.find_all("tr")
    
    veri_sozlugu = {}
    
    for satir in satirlar:
        # Her satırdaki sütunları (td) bul
        sutunlar = satir.find_all("td")
        
        # Eğer satırda veri varsa (Başlık satırı değilse)
        # Genellikle: [0]=İsim, [1]=Alış, [2]=Satış ...
        if len(sutunlar) > 2:
            try:
                # İsim sütununda bazen 'div' içinde gizlidir, text'i temizle
                isim = sutunlar[0].get_text(strip=True)
                
                # Fiyat sütunu (Satış)
                fiyat_text = sutunlar[2].get_text(strip=True)
                
                # ONS FİLTRESİ: Ons genellikle Dolar olduğu için listeye almıyoruz.
                if "Ons" in isim:
                    continue
                
                # Diğer "Gümüş", "Bilezik", "Gram" vb. hepsi alınır.
                fiyat = metni_sayiya_cevir(fiyat_text)
                
                if fiyat > 0:
                    veri_sozlugu[isim] = fiyat
            except:
                continue

    print(f"Toplam {len(veri_sozlugu)} adet veri çekildi.")

    if len(veri_sozlugu) == 0:
        print("HATA: Tablo okundu ama hiç veri çıkarılamadı (Site yapısı değişmiş veya engelleme var).")
        sys.exit(1)

    # --- FIREBASE KAYIT ---
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
    print(f"✅ BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler kaydedildi.")

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
