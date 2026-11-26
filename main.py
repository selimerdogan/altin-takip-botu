from fastapi import FastAPI, BackgroundTasks, HTTPException
import os
from market_engine import execute_engine

app = FastAPI(title="Vario Cloud Run Market Engine", version="2.0")

@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "vario-market-engine",
        "version": "2.0.0",
        "framework": "Google Cloud Run + FastAPI"
    }

@app.api_route("/run", methods=["GET", "POST"])
def run_manually():
    """
    Cloud Scheduler veya manuel tetikleyici tarafından çağrılır.
    Piyasa verilerini çeker, alarmları inceler ve bildirim basar.
    """
    try:
        result = execute_engine()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
