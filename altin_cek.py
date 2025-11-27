import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# --- 1. AYARLAR ---
url = "https://altin.doviz.com/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Firebase Başlat
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

def metni_sayiya_cevir(metin):
    try:
        return float(metin.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# Türkçe karakterleri bozmadan, sadece boşlukları düzelten fonksiyon
def ismi_duzenle(isim):
    return isim.strip()

try:
    print("Veri çekme işlemi başladı...")
    
    # --- 2. VERİYİ ÇEK ---
    response = requests.get(url, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 2]] # İsim ve Satış Fiyatı
    df.columns = ["isim", "fiyat"]
    
    # --- 3. VERİYİ HAZIRLA (CÖMERT MOD: TAM İSİMLER) ---
    veri_sozlugu = {}
    
    for index, satir in df.iterrows():
        # Artık kısaltma yok, sitedeki ismin aynısını alıyoruz
        # Örn: "Gram Altın" -> "Gram Altın"
        altin_ismi = ismi_duzenle(satir['isim'])
        
        # Fiyatı sayıya çevir
        fiyat = metni_sayiya_cevir(satir['fiyat'])
        
        # Sözlüğe ekle
        veri_sozlugu[altin_ismi] = fiyat
            
    # Tarih Ayarları (Türkiye Saati Değil, Sunucu Saati ile kaydedilir ama sorun değil)
    # İstersen +3 saat ekleme yapılabilir ama standart kalması daha iyidir.
    simdi = datetime.now()
    bugun_tarih = simdi.strftime("%Y-%m-%d") # 2025-11-27
    
    # Dakikayı da ekleyelim ki 18:15 ile 18:00 farkı anlaşılsın
    # Örnek Anahtar: "10:20", "14:00", "18:15"
    su_an_saat_dakika = simdi.strftime("%H:%M") 
    
    # --- 4. FIREBASE KAYDI ---
    doc_ref = db.collection(u'market_history').document(bugun_tarih)
    
    # Veriyi SAAT:DAKİKA anahtarı altına gömüyoruz
    kayit = {
        u'hourly': {
            su_an_saat_dakika: veri_sozlugu
        }
    }
    
    doc_ref.set(kayit, merge=True)
    
    print(f"[{bugun_tarih} - {su_an_saat_dakika}] Veriler TAM İSİM formatında başarıyla kaydedildi.")

except Exception as e:
    print(f"HATA: {e}")
