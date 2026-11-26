from fastapi import FastAPI, HTTPException
import os
from market_engine import execute_engine
from history_engine import execute_history_engine
from logo_engine import execute_logo_engine

app = FastAPI(
    title="Vario Unified Market & Intelligence Engine",
    description="Canlı Piyasa Fiyatları, Fiyat Alarmları, 1 Yıllık Geçmiş Grafikler ve Logo Senkronizasyonu",
    version="2.0.0"
)

@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "vario-unified-market-engine",
        "version": "2.0.0",
        "endpoints": {
            "/run": "Fiyatları çeker, alarmları eşleştirir ve anında FCM bildirimi basar",
            "/run-history": "TEFAS ve hisselerin 1 yıllık grafik geçmişini (market_grafigi) günceller",
            "/run-logos": "Eksik şirket ve kripto logolarını Storage'a yükleyip system_data sözlüğünü günceller",
            "/run-all": "Tüm motorları sırayla çalıştırır"
        }
    }

@app.api_route("/run", methods=["GET", "POST"])
def run_prices_and_alarms():
    """1. Canlı Piyasa Fiyatları & Fiyat Alarmları Motoru"""
    try:
        return execute_engine()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/run-history", methods=["GET", "POST"])
def run_history():
    """2. TEFAS & YFinance 1 Yıllık Grafik Motoru"""
    try:
        return execute_history_engine()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/run-logos", methods=["GET", "POST"])
def run_logos():
    """3. Otomatik Logo İndirme & Firebase Storage Senkronizasyon Motoru"""
    try:
        return execute_logo_engine()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/run-all", methods=["GET", "POST"])
def run_all():
    """Tüm Motorları Sırayla Çalıştırır"""
    results = {}
    try:
        results["prices_and_alarms"] = execute_engine()
    except Exception as e:
        results["prices_and_alarms"] = {"error": str(e)}
        
    try:
        results["history"] = execute_history_engine()
    except Exception as e:
        results["history"] = {"error": str(e)}
        
    try:
        results["logos"] = execute_logo_engine()
    except Exception as e:
        results["logos"] = {"error": str(e)}
        
    return results

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
