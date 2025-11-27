import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import json

# --- 1. AYARLAR ---
# Altın için yine aynı site (Burası çalışıyor)
url_altin = "https://altin.doviz.com/"

# Dolar Kuru için artık HTML kazımıyoruz, direkt JSON verisi alıyoruz (Daha güvenli)
# Alternatif API: https://api.genelpara.com/embed/doviz.json
url_dolar_api = "https://api.genelpara.com/embed/doviz.json"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

# Firebase Kontrolü
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

def metni_sayiya_cevir(metin):
    try:
        metin = str(metin)
        # Sadece sayı, nokta ve virgül kalsın
        temiz = metin.replace('$', '').replace('USD', '').replace('TL', '').strip()
        # 2.650,50 -> 2650.50
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

try:
    print("--- İŞLEM BAŞLIYOR ---")
    
    # --- ADIM A: DOLAR KURUNU JSON OLARAK ÇEK (En Sağlam Yöntem) ---
    print("Dolar kuru API'den çekiliyor...")
    resp_api = requests.get(url_dolar_api, headers=headers)
    
    if resp_api.status_code == 200:
        veri_json = resp_api.json()
        # Genelpara API yapısı: veri_json['USD']['satis'] (String olarak gelir)
        dolar_satis_str = veri_json.get('USD', {}).get('satis', '0')
        guncel_dolar_kuru = float(dolar_satis_str)
        print(f"GÜNCEL DOLAR KURU: {guncel_dolar_kuru} TL")
    else:
        # Yedek Plan: API çalışmazsa manuel bir değer veya hata
        raise Exception("Dolar API servisine erişilemedi.")

    # --- ADIM B: ALTIN FİYATLARINI ÇEK ---
    print("Altın verileri çekiliyor...")
    response = requests.get(url_altin, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 2]]
    df.columns = ["isim", "fiyat"]
    
    veri_sozlugu = {}
    
    for index, satir in df.iterrows():
        isim = satir['isim'].strip()
        ham_fiyat = satir['fiyat']
        fiyat_sayi = metni_sayiya_cevir(ham_fiyat)
        
        # ONS ALTIN HESAPLAMASI
        if "Ons" in isim:
            # Ons genellikle Dolar gelir, TL'ye çevir
            tl_karsiligi = fiyat_sayi * guncel_dolar_kuru
            veri_sozlugu[isim] = round(tl_karsiligi, 2)
            print(f"Ons Çevrildi: {fiyat_sayi} USD -> {veri_sozlugu[isim]} TL")
        else:
            veri_sozlugu[isim] = fiyat_sayi
            
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
    print(f"BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler kaydedildi.")

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
