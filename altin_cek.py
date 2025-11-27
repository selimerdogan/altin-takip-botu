import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os

# --- 1. AYARLAR ---
# Kaynak: Hürriyet Bigpara (Daha güvenilir ve robot engeli az)
url_altin = "https://bigpara.hurriyet.com.tr/altin/"
url_dolar = "https://bigpara.hurriyet.com.tr/doviz/dolar/"

# Tarayıcı gibi görünmek için başlıklar
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"
}

# --- FIREBASE BAĞLANTISI ---
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json dosyası bulunamadı!")
    sys.exit(1)

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase bağlantısı kurulamadı. {e}")
    sys.exit(1)

# --- YARDIMCI FONKSİYONLAR ---
def metni_sayiya_cevir(metin):
    """
    Bigpara'dan gelen "2.950,50 TL" veya "1.200" verisini temizler.
    """
    try:
        metin = str(metin)
        # TL, Dolar, %, ok işaretleri ve boşlukları temizle
        temiz = metin.replace('TL', '').replace('USD', '').replace('%', '').strip()
        # Binlik ayracı noktayı sil, ondalık virgülü nokta yap
        # Örnek: 2.950,50 -> 2950,50 -> 2950.50
        sayi = float(temiz.replace('.', '').replace(',', '.'))
        return sayi
    except:
        return 0.0

def ismi_temizle(isim):
    """
    İsimlerdeki gereksiz etiketleri atar.
    """
    return isim.strip()

# --- ANA İŞLEM ---
try:
    print("--- BIGPARA VERİ ÇEKME MODU ---")

    # 1. GÜNCEL DOLAR KURUNU AL (ONS Çevirisi İçin)
    print("Dolar kuru alınıyor...")
    resp_dolar = requests.get(url_dolar, headers=headers, timeout=20)
    
    # Bigpara Dolar sayfasında genellikle üstteki büyük kutuda veya tabloda veri vardır.
    # Tablo yöntemi en garantisidir.
    dolar_df_list = pd.read_html(resp_dolar.text)
    
    # Genellikle sayfadaki 0. veya 1. tablo ana veridir.
    # Bigpara Dolar sayfasında 'Satış' sütunu genellikle 2. sıradadır (0:İsim, 1:Alış, 2:Satış)
    df_dolar = dolar_df_list[0]
    
    # Tablodaki ilk satırın Satış fiyatını al (Genellikle Dolar sayfası olduğu için ilk satır dolardır)
    dolar_satis_raw = df_dolar.iloc[0, 2] 
    guncel_dolar = metni_sayiya_cevir(dolar_satis_raw)
    
    print(f"Güncel Dolar Kuru: {guncel_dolar} TL")

    # 2. ALTIN VERİLERİNİ AL
    print("Altın verileri alınıyor...")
    resp_altin = requests.get(url_altin, headers=headers, timeout=20)
    
    # Sayfadaki tabloları oku
    altin_df_list = pd.read_html(resp_altin.text)
    
    # Bigpara altın sayfasındaki ana tablo genellikle en büyüktür (satır sayısı en fazla olan)
    # Garanti olsun diye en uzun tabloyu buluyoruz.
    altin_tablosu = max(altin_df_list, key=len)
    
    # Bigpara Tablo Yapısı Genellikle:
    # 0: Altın Türü, 1: Alış, 2: Satış, 3: Saat vs...
    # Biz 0 (İsim) ve 2 (Satış) sütunlarını alacağız.
    df = altin_tablosu.iloc[:, [0, 2]]
    df.columns = ["isim", "fiyat"]
    
    veri_sozlugu = {}
    
    for index, satir in df.iterrows():
        ham_isim = str(satir['isim'])
        ham_fiyat = str(satir['fiyat'])
        
        # Sadece anlamlı verileri al (Başlık satırlarını atla)
        if "Alış" in ham_isim or "Satış" in ham_isim:
            continue
            
        isim = ismi_temizle(ham_isim)
        fiyat = metni_sayiya_cevir(ham_fiyat)
        
        # ONS ALTIN KONTROLÜ
        # Bigpara'da Ons genellikle "Ons Altın" olarak geçer ve Dolar cinsindendir.
        if "Ons" in isim:
            # Dolar mı TL mi olduğunu anlamak için fiyata bakabiliriz
            # Eğer fiyat 5000'den küçükse Dolardır (Çünkü Ons 2600$ civarı, TL olsa 90.000 olur)
            if fiyat < 10000: 
                tl_karsiligi = fiyat * guncel_dolar
                veri_sozlugu[isim] = round(tl_karsiligi, 2)
                print(f"{isim} (Dolar) -> TL'ye çevrildi: {veri_sozlugu[isim]}")
            else:
                veri_sozlugu[isim] = fiyat
        else:
            veri_sozlugu[isim] = fiyat

    print(f"Toplam {len(veri_sozlugu)} adet veri işlendi.")

    # 3. FIREBASE KAYDI
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
    print(f"✅ BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Veriler Bigpara üzerinden kaydedildi.")

except Exception as e:
    # Hatanın detayını yazdır ki loglarda görelim
    print("❌ KRİTİK HATA DETAYI:")
    print(e)
    # Hata oluşursa programı durdur (GitHub kırmızı X versin)
    sys.exit(1)
