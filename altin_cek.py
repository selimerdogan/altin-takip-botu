import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json

# --- 1. AYARLAR ---
# İki farklı kaynaktan JSON çekeceğiz
url_doviz = "https://api.genelpara.com/embed/doviz.json"
url_altin = "https://api.genelpara.com/embed/altin.json"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

# Kod Haritası: API'den gelen kısaltmaları İnsan Diline çevirir
ISIM_HARITASI = {
    "GA": "Gram Altın",
    "C": "Çeyrek Altın",
    "Y": "Yarım Altın",
    "T": "Tam Altın",
    "CUM": "Cumhuriyet Altını",
    "ATA": "Ata Altın",
    "ONS": "Ons Altın",
    "22": "22 Ayar Bilezik", # Bazen veri gelmeyebilir
    "14": "14 Ayar Altın",   # Bazen veri gelmeyebilir
    "GUM": "Gümüş"
}

# Firebase Başlatma (Hata Yönetimi ile)
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: Anahtar dosyası (serviceAccountKey.json) yok!")
    sys.exit(1)

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase bağlantısı kurulamadı. {e}")
    sys.exit(1)

def metni_sayiya_cevir(metin):
    try:
        # Gelen veri zaten string formatında sayı olabilir "2950.12"
        return float(str(metin).strip())
    except:
        return 0.0

try:
    print("--- API VERİ ÇEKME MODU BAŞLADI ---")
    
    # 1. DOLAR KURUNU AL
    resp_doviz = requests.get(url_doviz, headers=headers, timeout=10)
    if resp_doviz.status_code != 200:
        raise Exception("Döviz servisine ulaşılamadı.")
        
    json_doviz = resp_doviz.json()
    # Genelpara USD verisi
    dolar_satis = json_doviz.get('USD', {}).get('satis')
    guncel_dolar = metni_sayiya_cevir(dolar_satis)
    print(f"Dolar Kuru: {guncel_dolar} TL")

    # 2. ALTIN VERİLERİNİ AL
    resp_altin = requests.get(url_altin, headers=headers, timeout=10)
    if resp_altin.status_code != 200:
        raise Exception("Altın servisine ulaşılamadı.")
        
    json_altin = resp_altin.json()
    
    veri_sozlugu = {}
    
    # API'den gelen karmaşık listeyi dönüyoruz
    for kod, bilgiler in json_altin.items():
        # Sadece bizim haritamızda olanları alalım (Gereksizleri at)
        if kod in ISIM_HARITASI:
            guzel_isim = ISIM_HARITASI[kod]
            satis_fiyati = metni_sayiya_cevir(bilgiler.get('satis'))
            
            # Ons Altın Kontrolü (Genellikle Dolar gelir)
            if kod == "ONS":
                # Eğer 5000'den küçükse muhtemelen dolardır, TL'ye çevir
                if satis_fiyati < 5000:
                     tl_karsiligi = satis_fiyati * guncel_dolar
                     veri_sozlugu[guzel_isim] = round(tl_karsiligi, 2)
                     print(f"Ons Altın TL'ye çevrildi: {veri_sozlugu[guzel_isim]}")
                else:
                    # Zaten TL gelmişse (bazı API'ler TL verir)
                    veri_sozlugu[guzel_isim] = satis_fiyati
            else:
                veri_sozlugu[guzel_isim] = satis_fiyati

    print(f"Toplam {len(veri_sozlugu)} altın türü işlendi.")

    # 3. FIREBASE KAYDI
    simdi = datetime.now()
    bugun_tarih = simdi.strftime("%Y-%m-%d")
    su_an_saat_dakika = simdi.strftime("%H:%M") # Örn: 10:20
    
    doc_ref = db.collection(u'market_history').document(bugun_tarih)
    
    kayit = {
        u'hourly': {
            su_an_saat_dakika: veri_sozlugu
        }
    }
    
    doc_ref.set(kayit, merge=True)
    print(f"✅ BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler kaydedildi.")

except Exception as e:
    print(f"❌ KRİTİK HATA: {e}")
    sys.exit(1)
