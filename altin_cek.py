import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os

# --- 1. AYARLAR ---
url = "https://altin.doviz.com/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Firebase Bağlantısı
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

# Yardımcı Fonksiyon: "2.950,12" gibi metinleri sayıya (2950.12) çevirir
def metni_sayiya_cevir(metin):
    try:
        # Noktaları sil (binlik ayracı), virgülü noktaya çevir (kuruş)
        temiz = metin.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# Yardımcı Fonksiyon: İsimleri düzeltir (Örn: "Gram Altın" -> "GRAM_ALTIN")
def ismi_duzelt(isim):
    tr_karakterler = str.maketrans("ğüşıöçĞÜŞİÖÇ ", "gusiocGUSIOC_")
    return isim.translate(tr_karakterler).upper()

try:
    print("Veri çekme işlemi başladı...")
    
    # --- 2. VERİYİ ÇEK ---
    response = requests.get(url, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 2]] # Sadece İsim ve SATIŞ sütununu alıyoruz
    df.columns = ["tur", "satis"]
    
    # --- 3. VERİYİ HAZIRLA (Attığın fotograftaki formata göre) ---
    # Liste yerine, varlık isminin anahtar olduğu bir sözlük yapıyoruz.
    fiyat_sozlugu = {}
    
    for index, satir in df.iterrows():
        anahtar = ismi_duzelt(satir['tur']) # Örn: CEYREK_ALTIN
        deger = metni_sayiya_cevir(satir['satis']) # Örn: 5025.50
        fiyat_sozlugu[anahtar] = deger
        
    # Tarih ve Saat Bilgisi
    simdi = datetime.now()
    bugun_tarih = simdi.strftime("%Y-%m-%d") # Doküman adı: 2025-11-27
    su_an_saat = simdi.strftime("%H")        # Saat alanı: 14, 15 vs.
    
    # --- 4. FIREBASE'E YAZ (MERGE/BİRLEŞTİRME MODU) ---
    # Collection adını fotograftaki gibi 'market_history' yaptım.
    doc_ref = db.collection(u'market_history').document(bugun_tarih)
    
    # Var olan veriyi silmeden, sadece o saatin verisini içine gömer.
    veriler = {
        u'hourly': {
            su_an_saat: fiyat_sozlugu
        }
    }
    
    # merge=True demezsek eski saatleri siler!
    doc_ref.set(veriler, merge=True)
    
    print(f"[{bugun_tarih} - Saat: {su_an_saat}] Veriler 'market_history' altına başarıyla işlendi.")

except Exception as e:
    print(f"HATA: {e}")
