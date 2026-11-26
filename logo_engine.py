import requests
import firebase_admin
from firebase_admin import credentials, firestore, storage
import os
import sys
import json
import io
import uuid 
from PIL import Image 
import time

headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
BUCKET_NAME = "vario-264d9.firebasestorage.app"

def init_firebase_storage():
    if firebase_admin._apps:
        return firestore.client(), storage.bucket()
        
    firebase_key_str = os.environ.get('FIREBASE_KEY')
    if firebase_key_str:
        cred = credentials.Certificate(json.loads(firebase_key_str))
    elif os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
    else:
        cred = credentials.ApplicationDefault()
        
    firebase_admin.initialize_app(cred, {'storageBucket': BUCKET_NAME})
    return firestore.client(), storage.bucket()

def upload_logo(bucket, original_url, file_name, folder_name):
    if not original_url: return None
    if "firebasestorage.googleapis.com" in original_url: return original_url
    
    try:
        resp = requests.get(original_url, headers=headers_general, timeout=10)
        if resp.status_code != 200: return original_url 

        content_type = resp.headers.get('Content-Type', '')
        file_data = resp.content
        
        if "svg" in content_type or original_url.endswith(".svg") or b"<svg" in file_data[:100]:
            extension = "svg"
            final_content_type = "image/svg+xml"
            blob_data = file_data
        else:
            img_bytes = io.BytesIO(file_data)
            try:
                img = Image.open(img_bytes)
                if img.mode != 'RGBA': img = img.convert('RGBA')
                img = img.resize((128, 128), Image.Resampling.LANCZOS)
                output_io = io.BytesIO()
                img.save(output_io, format='PNG', optimize=True)
                blob_data = output_io.getvalue()
                extension = "png"
                final_content_type = "image/png"
            except:
                return original_url 

        blob_path = f"logos/{folder_name}/{file_name}.{extension}"
        blob = bucket.blob(blob_path)
        
        new_token = str(uuid.uuid4())
        blob.metadata = {"firebaseStorageDownloadTokens": new_token}
        blob.upload_from_string(blob_data, content_type=final_content_type)
        
        encoded_path = blob_path.replace("/", "%2F") 
        download_url = f"https://firebasestorage.googleapis.com/v0/b/{BUCKET_NAME}/o/{encoded_path}?alt=media&token={new_token}"
        return download_url
    except Exception as e:
        print(f"      Resim yükleme hatası ({file_name}): {e}")
        return original_url

def update_missing_logos(db, bucket):
    print("--- 🎨 VARIO LOGO MOTORU: Eksik Logolar Taranıyor ---")
    cmc_key = os.environ.get('CMC_API_KEY')
    total_added = 0
    
    live_ref = db.collection('market_data').document('LIVE_PRICES')
    live_doc = live_ref.get()
    if not live_doc.exists:
        return 0
    live_data = live_doc.to_dict() or {}
    
    # 1. BIST
    try:
        bist_ref = db.collection('system_data').document('bist')
        bist_current = bist_ref.get().to_dict() or {}
        live_bist = live_data.get('borsa_tr_tl', {})
        missing_bist = [s for s in live_bist.keys() if s not in bist_current][:30]
        if missing_bist:
            print(f"   🇹🇷 {len(missing_bist)} eksik BIST logosu aranıyor...")
            url = "https://scanner.tradingview.com/turkey/scan"
            payload = {
                "symbols": {"tickers": [f"BIST:{s}" for s in missing_bist]},
                "columns": ["name", "description", "logoid"]
            }
            r = requests.post(url, json=payload, headers=headers_general, timeout=20)
            if r.status_code == 200:
                updates = {}
                base_logo = "https://s3-symbol-logo.tradingview.com/"
                for h in r.json().get('data', []):
                    d = h.get('d', [])
                    sym = d[0]
                    name = d[1]
                    logoid = d[2]
                    final_url = upload_logo(bucket, f"{base_logo}{logoid}.svg" if logoid else "", sym, "bist")
                    updates[sym] = {"name": name, "logo": final_url}
                    total_added += 1
                if updates:
                    bist_ref.set(updates, merge=True)
    except Exception as e:
        print(f"   ⚠️ BIST logo hatası: {e}")

    # 2. KRİPTO
    if cmc_key:
        try:
            crypto_ref = db.collection('system_data').document('kripto')
            crypto_current = crypto_ref.get().to_dict() or {}
            live_crypto = live_data.get('kripto_usd', {})
            missing_crypto = [s for s in live_crypto.keys() if s not in crypto_current][:30]
            if missing_crypto:
                print(f"   🪙 {len(missing_crypto)} eksik Kripto logosu aranıyor...")
                clean_map = {k: k.replace("-USD", "") for k in missing_crypto}
                symbols_str = ','.join(list(clean_map.values()))
                url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
                r = requests.get(url, headers={'X-CMC_PRO_API_KEY': cmc_key, 'Accept': 'application/json'}, params={'symbol': symbols_str}, timeout=20)
                if r.status_code == 200:
                    updates = {}
                    data = r.json().get('data', {})
                    for clean_sym, details in data.items():
                        orig_key = next((k for k, v in clean_map.items() if v == clean_sym), None)
                        if orig_key and details.get('logo'):
                            final_url = upload_logo(bucket, details.get('logo'), clean_sym, "crypto")
                            updates[orig_key] = {"name": details.get('name'), "logo": final_url}
                            total_added += 1
                    if updates:
                        crypto_ref.set(updates, merge=True)
        except Exception as e:
            print(f"   ⚠️ Kripto logo hatası: {e}")

    return total_added

def execute_logo_engine():
    db, bucket = init_firebase_storage()
    start_time = time.time()
    added = update_missing_logos(db, bucket)
    elapsed = round(time.time() - start_time, 2)
    return {
        "status": "success",
        "elapsed_seconds": elapsed,
        "new_logos_added": added
    }
