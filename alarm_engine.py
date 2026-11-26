import datetime
from firebase_admin import firestore, messaging

def extract_price(ticker, final_paket):
    """
    Tüm piyasa paketinde verilen ticker'ın anlık fiyatını arar.
    Örn: THYAO -> borsa_tr_tl['THYAO']['price']
         BTC-USD -> kripto_usd['BTC-USD']['price']
         Gram Altın -> altin_tl['Gram Altın']['price']
         USD -> doviz_tl['USD']['price']
    """
    t_upper = ticker.strip().upper()
    
    # 1. BIST
    borsa_tr = final_paket.get('borsa_tr_tl', {})
    if t_upper in borsa_tr:
        return borsa_tr[t_upper].get('price')
        
    # 2. ABD
    borsa_abd = final_paket.get('borsa_abd_usd', {})
    if t_upper in borsa_abd:
        return borsa_abd[t_upper].get('price')
        
    # 3. Kripto
    kripto = final_paket.get('kripto_usd', {})
    if t_upper in kripto:
        return kripto[t_upper].get('price')
    if f"{t_upper}-USD" in kripto:
        return kripto[f"{t_upper}-USD"].get('price')
        
    # 4. Döviz
    doviz = final_paket.get('doviz_tl', {})
    if t_upper in doviz:
        return doviz[t_upper].get('price')
        
    # 5. Altın
    altin = final_paket.get('altin_tl', {})
    for k, v in altin.items():
        if k.strip().lower() == ticker.strip().lower():
            return v.get('price')
            
    # 6. Fon
    fon = final_paket.get('fon_tl', {})
    if t_upper in fon:
        return fon[t_upper].get('price')
        
    # 7. Endeks
    endeks = final_paket.get('endeks_tr', {})
    if t_upper in endeks:
        return endeks[t_upper].get('price')
        
    return None

def check_and_trigger_alarms(final_paket, db):
    """
    active_alarms koleksiyonunu tarar.
    Hedefe ulaşan alarmlar için FCM bildirimi gönderir,
    kullanıcı dokümanındaki alarmı isTriggered=true yapar,
    ve aktif listeden çıkararak bir daha tetiklenmesini engeller.
    """
    print("\n🔔 [ALARM MOTORU] Aktif alarmlar kontrol ediliyor...")
    triggered_count = 0
    checked_count = 0
    
    try:
        active_docs = db.collection('active_alarms').get()
        if not active_docs:
            print("   -> Aktif bekleyen alarm yok.")
            return {"checked": 0, "triggered": 0}
            
        for doc in active_docs:
            ticker = doc.id
            data = doc.to_dict() or {}
            alarm_list = data.get('alarms', [])
            
            if not alarm_list:
                # Boş dokümanı temizle
                db.collection('active_alarms').document(ticker).delete()
                continue
                
            current_price = extract_price(ticker, final_paket)
            if current_price is None:
                continue
                
            remaining_alarms = []
            
            for alarm in alarm_list:
                checked_count += 1
                target = float(alarm.get('targetPrice', 0))
                condition = alarm.get('condition', 'ABOVE').upper()
                fcm_token = alarm.get('fcmToken')
                user_id = alarm.get('userId')
                alarm_id = alarm.get('alarmId')
                
                is_hit = False
                if condition == 'ABOVE' and current_price >= target:
                    is_hit = True
                elif condition == 'BELOW' and current_price <= target:
                    is_hit = True
                    
                if is_hit:
                    triggered_count += 1
                    print(f"   🎯 HEDEF VURULDU: {ticker} Hedef: {target} | Güncel: {current_price} -> User: {user_id}")
                    
                    # 1. FCM Bildirimi Gönder
                    if fcm_token:
                        try:
                            msg = messaging.Message(
                                notification=messaging.Notification(
                                    title=f"🔔 {ticker} Hedefe Ulaştı!",
                                    body=f"{ticker} belirlediğin {target:,.2f} seviyesine ulaştı! (Güncel: {current_price:,.2f})"
                                ),
                                data={
                                    "ticker": str(ticker),
                                    "price": str(current_price),
                                    "alarmId": str(alarm_id),
                                    "type": "PRICE_ALERT"
                                },
                                token=fcm_token
                            )
                            messaging.send(msg)
                            print(f"      ✅ Push bildirim gönderildi: {fcm_token[:15]}...")
                        except Exception as e:
                            print(f"      ⚠️ Push bildirim hatası: {e}")
                            
                    # 2. Kullanıcının kendi alarm kaydını "Tetiklendi" olarak güncelle
                    if user_id and alarm_id:
                        try:
                            user_alarm_ref = db.collection('users').document(user_id).collection('alarms').document(alarm_id)
                            user_alarm_ref.update({
                                'isTriggered': True,
                                'isActive': False,
                                'triggeredAt': firestore.SERVER_TIMESTAMP,
                                'triggeredPrice': current_price
                            })
                        except Exception as e:
                            print(f"      ⚠️ Kullanıcı alarm güncelleme hatası: {e}")
                else:
                    # Henüz hedefe ulaşmadı, aktif listede kalmaya devam etsin
                    remaining_alarms.append(alarm)
                    
            # 3. Kalan alarmları güncelle veya temizle
            if len(remaining_alarms) != len(alarm_list):
                if remaining_alarms:
                    db.collection('active_alarms').document(ticker).set({'alarms': remaining_alarms})
                else:
                    db.collection('active_alarms').document(ticker).delete()
                    
        print(f"🔔 [ALARM MOTORU] Tamamlandı. Kontrol edilen: {checked_count}, Tetiklenen: {triggered_count}\n")
        return {"checked": checked_count, "triggered": triggered_count}
        
    except Exception as e:
        print(f"❌ [ALARM MOTORU HATASI]: {e}")
        return {"checked": checked_count, "triggered": triggered_count, "error": str(e)}
