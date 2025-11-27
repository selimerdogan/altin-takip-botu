import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# --- 1. AYARLAR ---
url_altin = "https://altin.doviz.com/"
url_kur = "https://www.doviz.com/" # Dolar kurunu buradan alacağız

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Firebase Başlat
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Yardımcı Fonksiyon: Metni Sayıya Çevir (Dolar işaretini de temizler)
def metni_sayiya_cevir(metin):
    try:
        metin = str(metin)
        # Dolar, TL ve harfleri sil
        temiz = metin.replace('$', '').replace('USD', '').replace('TL', '').strip()
        # 2.650,50 -> 2650.50 formatına dönüştür
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

try:
    print("İşlem başlıyor...")
    
    # --- ADIM A: GÜNCEL DOLAR KURUNU ÇEK ---
    # Ons Altın'ı TL'ye çevirmek için dolar kuruna ihtiyacımız var.
    resp_kur = requests.get(url_kur, headers=headers)
    tablolar_kur = pd.read_html(resp_kur.text)
    
    # Genellikle ana sayfadaki ilk tablo kurları verir
    # "DOLAR" yazan satırı bulup SATIŞ fiyatını alıyoruz
    df_kur = tablolar_kur[0]
    
    # Tabloda "Dolar" veya "ABD Doları" geçen satırı bul
    dolar_satiri = df_kur[df_kur.iloc[:, 0].str.contains("Dolar", case=False, na=False)]
    dolar_satis_fiyati = dolar_satiri.iloc[0, 2] # 2. Sütun genellikle Satış'tır
    
    guncel_dolar_kuru = metni_sayiya_cevir(dolar_satis_fiyati)
    print(f"Güncel Dolar Kuru Alındı: {guncel_dolar_kuru} TL")

    # --- ADIM B: ALTIN FİYATLARINI ÇEK ---
    response = requests.get(url_altin, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 2]] # İsim ve Satış Fiyatı
    df.columns = ["isim", "fiyat"]
    
    veri_sozlugu = {}
    
    for index, satir in df.iterrows():
        isim = satir['isim'].strip()
        ham_fiyat = satir['fiyat']
        
        # Sayısal değere çevir (Dolar işareti varsa temizlenir)
        fiyat_sayi = metni_sayiya_cevir(ham_fiyat)
        
        # --- KRİTİK NOKTA: ONS ALTIN ÇEVİRİSİ ---
        # Eğer isimde "Ons" geçiyorsa ve fiyat mantıken çok düşükse (TL değil USD ise)
        # veya ham veride '$' işareti varsa çarpma işlemi yap.
        if "Ons" in isim:
            tl_karsiligi = fiyat_sayi * guncel_dolar_kuru
            # Veritabanına TL karşılığını yaz (Virgülden sonra 2 hane)
            veri_sozlugu[isim] = round(tl_karsiligi, 2)
            print(f"Ons Altın ({fiyat_sayi} USD) -> TL'ye çevrildi: {veri_sozlugu[isim]} TL")
        else:
            # Diğerleri zaten TL, olduğu gibi yaz
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
    
    print(f"[{bugun_tarih} - {su_an_saat_dakika}] Tüm veriler (Ons dahil) TL olarak kaydedildi.")

except Exception as e:
    print(f"HATA OLUŞTU: {e}")
