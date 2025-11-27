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
# Not: GitHub'da çalışırken bu dosya o an oluşturulacak.
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

try:
    print("Veri çekme işlemi başladı...")
    
    # --- 2. VERİYİ ÇEK ---
    response = requests.get(url, headers=headers)
    tablolar = pd.read_html(response.text)
    df = tablolar[0].iloc[:, [0, 1, 2]]
    df.columns = ["tur", "alis", "satis"]
    
    # --- 3. VERİYİ HAZIRLA ---
    veri_listesi = df.to_dict(orient='records')
    tarih_saat = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # --- 4. FIREBASE'E YAZ ---
    doc_ref = db.collection(u'altin_gecmisi').document(tarih_saat)
    doc_ref.set({
        u'tarih': tarih_saat,
        u'fiyatlar': veri_listesi
    })
    
    print(f"[{tarih_saat}] BAŞARILI: Veriler Firebase'e yüklendi.")

except Exception as e:
    print(f"HATA: {e}")