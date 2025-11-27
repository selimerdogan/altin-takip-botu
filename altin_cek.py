import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os

# --- 1. AYARLAR ---
url_altin = "https://altin.doviz.com/"
url_kur = "https://www.doviz.com/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

print("--- DEBUG BAŞLANGIÇ ---")

# KONTROL 1: JSON Dosyası var mı?
if os.path.exists("serviceAccountKey.json"):
    print(f"Key Dosyası Mevcut. Boyut: {os.path.getsize('serviceAccountKey.json')} byte")
else:
    print("HATA: serviceAccountKey.json dosyası BULUNAMADI!")
    sys.exit(1) # Hata koduyla çık

# Firebase Başlat
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase bağlantısı başarılı.")
except Exception as e:
    print(f"HATA: Firebase'e bağlanılamadı. Key hatalı olabilir.\nDetay: {e}")
    sys.exit(1)

def metni_sayiya_cevir(metin):
    try:
        metin = str(metin)
        temiz = metin.replace('$', '').replace('USD', '').replace('TL', '').strip()
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

try:
    print("Kur bilgisi çekiliyor...")
    
    # --- ADIM A: DOLAR KURU ---
    resp_kur = requests.get(url_kur, headers=headers)
    # Status Code kontrolü
    if resp_kur.status_code != 200:
        raise Exception(f"Doviz.com erişim hatası! Kod: {resp_kur.status_code}")
        
    tablolar_kur = pd.read_html(resp_kur.text)
    df_kur = tablolar_kur[0]
    
    # Dolar satırını bulma
    dolar_satiri = df_kur[df_kur.iloc[:, 0].str.contains("Dolar", case=False, na=False)]
    
    if dolar_satiri.empty:
        raise Exception("Dolar kuru tablodan okunamadı! Site tasarımı değişmiş olabilir.")
        
    dolar_satis_fiyati = dolar_satiri.iloc[0, 2]
    guncel_dolar_kuru = metni_sayiya_cevir(dolar_satis_fiyati)
    
    print(f"Dolar Kuru: {guncel_dolar_kuru}")

    # --- ADIM B: ALTIN FİYATLARI ---
    print("Altın fiyatları çekiliyor...")
    response = requests.get(url_altin, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 2]]
    df.columns = ["isim", "fiyat"]
    
    veri_sozlugu = {}
    
    for index, satir in df.iterrows():
        isim = satir['isim'].strip()
        ham_fiyat = satir['fiyat']
        fiyat_sayi = metni_sayiya_cevir(ham_fiyat)
        
        if "Ons" in isim:
            tl_karsiligi = fiyat_sayi * guncel_dolar_kuru
            veri_sozlugu[isim] = round(tl_karsiligi, 2)
        else:
            veri_sozlugu[isim] = fiyat_sayi
            
    print(f"Toplam {len(veri_sozlugu)} adet veri hazırlandı.")
    
    # --- ADIM C: FIREBASE KAYDI ---
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
    print(f"BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler Firebase'e yazıldı.")

except Exception as e:
    # İşte burası GitHub'a hatayı bildirecek
    print(f"KRİTİK HATA: {e}")
    sys.exit(1) # Programı "HATA" koduyla bitir, Yeşil Tik olma!
