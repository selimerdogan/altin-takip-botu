import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import sys
import os
import time
import warnings

# Gereksiz uyarıları kapat
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

# --- KİMLİK KONTROLLERİ ---
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json bulunamadı!")
    sys.exit(1)

# 1. CMC ANAHTARI
CMC_API_KEY = os.environ.get('CMC_API_KEY')
if not CMC_API_KEY:
    print("UYARI: CMC_API_KEY yok! Kriptolar çekilemeyecek.")

# 2. FMP ANAHTARI (YENİ)
FMP_API_KEY = os.environ.get('FMP_API_KEY')
if not FMP_API_KEY:
    print("KRİTİK HATA: FMP_API_KEY bulunamadı! GitHub Secrets'a ekleyin.")
    sys.exit(1)

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase hatası: {e}")
    sys.exit(1)

def metni_sayiya_cevir(metin):
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        return float(temiz.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# ==============================================================================
# 1. KRİPTO PARALAR (COINMARKETCAP API)
# ==============================================================================
def get_crypto_from_cmc(limit=250):
    """En iyi 250 kripto parayı CMC'den çeker."""
    if not CMC_API_KEY: return {}
    
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data_kripto = {}

    try:
        print(f"   -> CMC: Top {limit} coin çekiliyor...")
        r = requests.get(url, headers=headers, params=parameters)
        if r.status_code == 200:
            data = r.json()['data']
            for coin in data:
                symbol = coin['symbol']
                price = coin['quote']['USD']['price']
                # İsim standardı: BTC-USD
                data_kripto[f"{symbol}-USD"] = round(float(price), 4)
            print(f"   -> ✅ CMC Başarılı: {len(data_kripto)} adet.")
        else:
            print(f"   -> ⚠️ CMC Hatası: {r.status_code}")
    except Exception as e:
        print(f"   -> ⚠️ CMC Bağlantı Hatası: {e}")
    return data_kripto

# ==============================================================================
# 2. HİSSE & DÖVİZ (FINANCIAL MODELING PREP API)
# ==============================================================================
def get_data_from_fmp(sembol_listesi):
    """
    FMP API kullanarak ABD, BIST ve Döviz verilerini çeker.
    URL çok uzamasın diye listeyi 50'şerli paketlere böler.
    """
    tum_veriler = {}
    
    # Listeyi parçalara ayıran yardımcı fonksiyon
    def parcalara_bol(liste, n):
        for i in range(0, len(liste), n):
            yield liste[i:i + n]

    paketler = list(parcalara_bol(sembol_listesi, 50))
    print(f"   -> FMP API: Toplam {len(sembol_listesi)} varlık, {len(paketler)} pakette çekilecek...")
    
    for i, paket in enumerate(paketler):
        # Sembolleri virgülle birleştir
        sembol_string = ",".join(paket)
        url = f"https://financialmodelingprep.com/api/v3/quote/{sembol_string}?apikey={FMP_API_KEY}"
        
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                data = r.json()
                for item in data:
                    symbol = item.get('symbol')
                    price = item.get('price')
                    if symbol and price:
                        tum_veriler[symbol] = price
            else:
                print(f"      ⚠️ Paket {i+1} FMP Hatası: {r.status_code}")
        except Exception as e:
            print(f"      ⚠️ FMP Bağlantı Hatası: {e}")
            
    return tum_veriler

# ==============================================================================
# 3. YATIRIM FONLARI (TEFAS)
# ==============================================================================
def get_tefas_data():
    url_api = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReturns"
    url_home = "https://www.tefas.gov.tr/FonKarsilastirma.aspx"
    data_fon = {}
    session = requests.Session()
    tefas_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "Origin": "https://www.tefas.gov.tr", "Content-Type": "application/json; charset=UTF-8"
    }
    try:
        session.get(url_home, headers=tefas_headers, timeout=15)
        simdi = datetime.now()
        for i in range(7):
            tarih_obj = simdi - timedelta(days=i)
            # Yıl koruması
            if tarih_obj.year > datetime.now().year: tarih_obj = tarih_obj.replace(year=datetime.now().year)
            tarih_str = tarih_obj.strftime("%d.%m.%Y")
            
            payload = { "calismatipi": "2", "fontip": "YAT", "bastarih": tarih_str, "bittarih": tarih_str }
            try:
                r = session.post(url_api, json=payload, headers=tefas_headers, timeout=30)
                if r.status_code == 200:
                    d = r.json().get('data', [])
                    if d and len(d) > 50:
                        for f in d:
                            if f.get('FONKODU') and f.get('FIYAT'):
                                data_fon[f['FONKODU']] = float(str(f['FIYAT']).replace(',', '.'))
                        print(f"   -> ✅ TEFAS Başarılı ({tarih_str}): {len(data_fon)} adet fon.")
                        return data_fon
            except: continue
    except: pass
    return {}

# ==============================================================================
# LİSTELER (FMP UYUMLU FORMAT)
# ==============================================================================

# ABD (S&P 500'den Örnekler - FMP Limitsiz ise 500'ünü de koyabilirsin)
# FMP Formatı: AAPL, MSFT (Yahoo ile aynı)
LISTE_ABD = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK-B", "LLY", "AVGO", "V", "JPM", "XOM", "WMT", "UNH", "MA", "PG", "JNJ", "HD", "MRK", "COST", "ABBV", "CVX", "CRM", "BAC", "AMD", "PEP", "KO", "NFLX", "ADBE", "DIS", "MCD", "CSCO", "TMUS", "ABT", "INTC", "INTU", "CMCSA", "PFE", "NKE", "WFC", "QCOM", "TXN", "DHR", "PM", "UNP", "IBM", "AMGN", "GE", "HON", "BA", "SPY", "QQQ", "UBER", "PLTR",
    "LIN", "ACN", "RTX", "VZ", "T", "CAT", "LOW", "BKNG", "NEE", "GS", "MS", "BMY", "DE", "MDT", "SCHW", "BLK", "TJX", "PGR", "COP", "ISRG", "LMT", "ADP", "AXP", "MMC", "GILD", "VRTX", "C", "MDLZ", "ADI", "REGN", "LRCX", "CI", "CVS", "BSX", "ZTS", "AMT", "ETN", "SLB", "FI", "BDX", "SYK", "CB", "EOG", "TM", "SO", "CME", "MU", "KLAC", "PANW", "MO", "SHW", "SNPS", "EQIX", "CDNS", "ITW", "DUK", "CL", "APH", "PYPL", "CSX", "PH", "TGT", "USB", "ICE", "NOC", "WM", "FCX", "GD", "NXPI", "ORLY", "HCA", "MCK", "EMR", "MAR", "PNC", "PSX", "BDX", "ROP", "NSC", "GM", "FDX", "MCO", "AFL", "CARR", "ECL", "APD", "AJG", "MSI", "AZO", "TT", "WMB", "TFC", "COF", "PCAR", "D", "SRE", "AEP", "HLT", "O", "TRV", "MET", "PSA", "PAYX", "ROST", "KMB", "JCI", "URI", "ALL", "PEG", "ED", "XEL", "GWW", "YUM", "FAST", "WELL", "AMP", "DLR", "VLO", "AME", "CMI", "FIS", "ILMN", "AIG", "KR", "PPG", "KMI", "EXC", "LUV", "DAL"
]

# DÖVİZ (FMP Formatı: EURUSD, USDTRY - Slaş veya =X yok!)
LISTE_DOVIZ = [
    "USDTRY", "EURTRY", "GBPTRY", "CHFTRY", "CADTRY", "JPYTRY", "AUDTRY",
    "EURUSD", "GBPUSD"
]

# BIST (FMP Formatı: THYAO.IS - Yahoo ile aynı)
LISTE_BIST = [
    "A1CAP.IS", "ACSEL.IS", "ADEL.IS", "ADESE.IS", "ADGYO.IS", "AEFES.IS", "AFYON.IS", "AGESA.IS", "AGHOL.IS", "AGROT.IS", "AGYO.IS", "AHGAZ.IS", "AKBNK.IS", "AKCNS.IS", "AKENR.IS", "AKFGY.IS", "AKFYE.IS", "AKGRT.IS", "AKMGY.IS", "AKSA.IS", "AKSEN.IS", "AKSGY.IS", "AKSUE.IS", "AKYHO.IS", "ALARK.IS", "ALBRK.IS", "ALCAR.IS", "ALCTL.IS", "ALFAS.IS", "ALGYO.IS", "ALKA.IS", "ALKIM.IS", "ALTNY.IS", "ANELE.IS", "ANGEN.IS", "ANHYT.IS", "ANSGR.IS", "ARASE.IS", "ARCLK.IS", "ARDYZ.IS", "ARENA.IS", "ARSAN.IS", "ARZUM.IS", "ASELS.IS", "ASGYO.IS", "ASTOR.IS", "ASUZU.IS", "ATAGY.IS", "ATAKP.IS", "ATATP.IS", "ATEKS.IS", "ATLAS.IS", "ATSYH.IS", "AVGYO.IS", "AVHOL.IS", "AVOD.IS", "AVPGY.IS", "AVTUR.IS", "AYCES.IS", "AYDEM.IS", "AYEN.IS", "AYES.IS", "AYGAZ.IS", "AZTEK.IS", 
    "BAGFS.IS", "BAKAB.IS", "BALAT.IS", "BANVT.IS", "BARMA.IS", "BASCM.IS", "BASGZ.IS", "BAYRK.IS", "BEGYO.IS", "BERA.IS", "BEYAZ.IS", "BFREN.IS", "BIENY.IS", "BIGCH.IS", "BIMAS.IS", "BINHO.IS", "BIOEN.IS", "BIZIM.IS", "BJKAS.IS", "BLCYT.IS", "BMSCH.IS", "BMSTL.IS", "BNTAS.IS", "BOBET.IS", "BORLS.IS", "BOSSA.IS", "BRISA.IS", "BRKO.IS", "BRKSN.IS", "BRKVY.IS", "BRLSM.IS", "BRMEN.IS", "BRSAN.IS", "BRYAT.IS", "BSOKE.IS", "BTCIM.IS", "BUCIM.IS", "BURCE.IS", "BURVA.IS", "BVSAN.IS", "BYDNR.IS", 
    "CANTE.IS", "CASA.IS", "CCOLA.IS", "CELHA.IS", "CEMAS.IS", "CEMTS.IS", "CEOEM.IS", "CIMSA.IS", "CLEBI.IS", "CMBTN.IS", "CMENT.IS", "CONSE.IS", "COSMO.IS", "CRDFA.IS", "CRFSA.IS", "CUSAN.IS", "CVKMD.IS", "CWENE.IS", 
    "DAPGM.IS", "DARDL.IS", "DENGE.IS", "DERHL.IS", "DERIM.IS", "DESA.IS", "DESPC.IS", "DEVA.IS", "DGATE.IS", "DGGYO.IS", "DGNMO.IS", "DIRIT.IS", "DITAS.IS", "DMSAS.IS", "DNISI.IS", "DOAS.IS", "DOBUR.IS", "DOCO.IS", "DOGUB.IS", "DOHOL.IS", "DOKTA.IS", "DURDO.IS", "DYOBY.IS", "DZGYO.IS", 
    "EBEBK.IS", "ECILC.IS", "ECZYT.IS", "EDATA.IS", "EDIP.IS", "EGEEN.IS", "EGGUB.IS", "EGPRO.IS", "EGSER.IS", "EKGYO.IS", "EKIZ.IS", "EKSUN.IS", "ELITE.IS", "EMKEL.IS", "EMNIS.IS", "ENJSA.IS", "ENKAI.IS", "ENSRI.IS", "EPLAS.IS", "ERBOS.IS", "ERCB.IS", "EREGL.IS", "ERSU.IS", "ESCAR.IS", "ESCOM.IS", "ESEN.IS", "ETILR.IS", "ETYAT.IS", "EUHOL.IS", "EUKYO.IS", "EUPWR.IS", "EUREN.IS", "EUYO.IS", "EYGYO.IS", 
    "FADE.IS", "FENER.IS", "FLAP.IS", "FMIZP.IS", "FONET.IS", "FORMT.IS", "FORTE.IS", "FRIGO.IS", "FROTO.IS", 
    "GARAN.IS", "GARFA.IS", "GEDIK.IS", "GEDZA.IS", "GENIL.IS", "GENTS.IS", "GEREL.IS", "GESAN.IS", "GIPTA.IS", "GLBMD.IS", "GLRYH.IS", "GLYHO.IS", "GMTAS.IS", "GOKNR.IS", "GOLTS.IS", "GOODY.IS", "GOZDE.IS", "GRNYO.IS", "GRSEL.IS", "GWIND.IS", "GZNMI.IS", 
    "HALKB.IS", "HATEK.IS", "HATSN.IS", "HDFGS.IS", "HEDEF.IS", "HEKTS.IS", "HKTM.IS", "HLGYO.IS", "HTTBT.IS", "HUBVC.IS", "HUNER.IS", "HURGZ.IS", 
    "ICBCT.IS", "IDGYO.IS", "IEYHO.IS", "IHAAS.IS", "IHEVA.IS", "IHGZT.IS", "IHLAS.IS", "IHLGM.IS", "IHYAY.IS", "IMASM.IS", "INDES.IS", "INFO.IS", "INGRM.IS", "INTEM.IS", "INVEO.IS", "INVES.IS", "IPEKE.IS", "ISATR.IS", "ISBIR.IS", "ISBTR.IS", "ISCTR.IS", "ISDMR.IS", "ISFIN.IS", "ISGSY.IS", "ISGYO.IS", "ISKPL.IS", "ISMEN.IS", "ISSEN.IS", "ISYAT.IS", "IZENR.IS", "IZFAS.IS", "IZINV.IS", "IZMDC.IS", 
    "JANTS.IS", 
    "KAPLM.IS", "KAREL.IS", "KARSN.IS", "KARTN.IS", "KATMR.IS", "KAYSE.IS", "KCAER.IS", "KFEIN.IS", "KGYO.IS", "KIMMR.IS", "KLGYO.IS", "KLKIM.IS", "KLMSN.IS", "KLNMA.IS", "KLRHO.IS", "KLSYN.IS", "KMPUR.IS", "KNFRT.IS", "KONKA.IS", "KONTR.IS", "KONYA.IS", "KOPOL.IS", "KORDS.IS", "KOZAA.IS", "KOZAL.IS", "KRDMA.IS", "KRDMB.IS", "KRDMD.IS", "KRGYO.IS", "KRONT.IS", "KRPLS.IS", "KRSTL.IS", "KRTEK.IS", "KRVGD.IS", "KSTUR.IS", "KTLEV.IS", "KTSKR.IS", "KUTPO.IS", "KUYAS.IS", "KZBGY.IS", "KZGYO.IS", 
    "LIDER.IS", "LIDFA.IS", "LINK.IS", "LKMNH.IS", "LOGO.IS", "LUKSK.IS", 
    "MAALT.IS", "MACKO.IS", "MAGEN.IS", "MAKIM.IS", "MAKTK.IS", "MANAS.IS", "MARKA.IS", "MARTI.IS", "MAVI.IS", "MEDTR.IS", "MEGAP.IS", "MEPET.IS", "MERCN.IS", "MERIT.IS", "MERKO.IS", "METRO.IS", "METUR.IS", "MGROS.IS", "MIATK.IS", "MMCAS.IS", "MNDRS.IS", "MNDTR.IS", "MOBTL.IS", "MPARK.IS", "MRGYO.IS", "MRSHL.IS", "MSGYO.IS", "MTRKS.IS", "MTRYO.IS", "MZHLD.IS", 
    "NATEN.IS", "NETAS.IS", "NIBAS.IS", "NTGAZ.IS", "NTHOL.IS", "NUGYO.IS", "NUHCM.IS", 
    "OBAMS.IS", "ODAS.IS", "OFSYM.IS", "ONCSM.IS", "ORCAY.IS", "ORGE.IS", "ORMA.IS", "OSMEN.IS", "OSTIM.IS", "OTKAR.IS", "OTTO.IS", "OYAKC.IS", "OYAYO.IS", "OYLUM.IS", "OYYAT.IS", "OZGYO.IS", "OZKGY.IS", "OZRDN.IS", "OZSUB.IS", 
    "PAGYO.IS", "PAMEL.IS", "PAPIL.IS", "PARSN.IS", "PASEU.IS", "PCILT.IS", "PEKGY.IS", "PENGD.IS", "PENTA.IS", "PETKM.IS", "PETUN.IS", "PGSUS.IS", "PINSU.IS", "PKART.IS", "PKENT.IS", "PLAT.IS", "PLTUR.IS", "PNLSN.IS", "PNSUT.IS", "POLHO.IS", "POLTK.IS", "PRDGS.IS", "PRKAB.IS", "PRKME.IS", "PRZMA.IS", "PSGYO.IS", "PSDTC.IS", 
    "QUAGR.IS", 
    "RALYH.IS", "RAYSG.IS", "REEDR.IS", "RNPOL.IS", "RODRG.IS", "ROYAL.IS", "RTALB.IS", "RUBNS.IS", "RYGYO.IS", "RYSAS.IS", 
    "SAHOL.IS", "SAMAT.IS", "SANEL.IS", "SANFM.IS", "SANKO.IS", "SARKY.IS", "SASA.IS", "SAYAS.IS", "SDTTR.IS", "SEKFK.IS", "SEKUR.IS", "SELEC.IS", "SELGD.IS", "SELVA.IS", "SEYKM.IS", "SILVR.IS", "SISE.IS", "SKBNK.IS", "SKTAS.IS", "SMART.IS", "SMRTG.IS", "SNGYO.IS", "SNKRN.IS", "SNPAM.IS", "SODSN.IS", "SOKE.IS", "SOKM.IS", "SONME.IS", "SRVGY.IS", "SUMAS.IS", "SUNTK.IS", "SURGY.IS", "SUWEN.IS", 
    "TABGD.IS", "TATGD.IS", "TAVHL.IS", "TBORG.IS", "TCELL.IS", "TDGYO.IS", "TEKTU.IS", "TERA.IS", "TEZOL.IS", "TGSAS.IS", "THYAO.IS", "TKFEN.IS", "TKNSA.IS", "TLMAN.IS", "TMPOL.IS", "TMSN.IS", "TNZTP.IS", "TOASO.IS", "TRCAS.IS", "TRGYO.IS", "TRILC.IS", "TSGYO.IS", "TSKB.IS", "TSPOR.IS", "TTKOM.IS", "TTRAK.IS", "TUCLK.IS", "TUKAS.IS", "TUPRS.IS", "TURGG.IS", "TURSG.IS", 
    "ULAS.IS", "ULKER.IS", "ULUFA.IS", "ULUSE.IS", "ULUUN.IS", "UMPAS.IS", "UNLU.IS", "USAK.IS", 
    "VAKBN.IS", "VAKFN.IS", "VAKKO.IS", "VANGD.IS", "VBTYZ.IS", "VERTU.IS", "VERUS.IS", "VESBE.IS", "VESTL.IS", "VKFYO.IS", "VKGYO.IS", "VKING.IS", "VRGYO.IS", 
    "YAPRK.IS", "YATAS.IS", "YAYLA.IS", "YEOTK.IS", "YESIL.IS", "YGGYO.IS", "YGYO.IS", "YKBNK.IS", "YKSLN.IS", "YONGA.IS", "YUNSA.IS", "YYAPI.IS", "YYLGD.IS", 
    "ZEDUR.IS", "ZOREN.IS", "ZRGYO.IS"
]

# ==============================================================================
# ANA PROGRAM
# ==============================================================================

try:
    print("--- MEGA FİNANS BOTU (FMP + CMC + TEFAS) ---")
    
    # 1. KRİPTO (CMC)
    data_kripto = get_crypto_from_cmc(250)
    
    # 2. TEFAS FONLARI
    print("2. Yatırım fonları taranıyor...")
    data_fonlar = get_tefas_data()
    
    # 3. FMP API (BIST, ABD, DÖVİZ)
    print("3. Borsa ve Döviz verileri FMP API'den çekiliyor...")
    
    # FMP listesini birleştir
    tum_fmp = LISTE_ABD + LISTE_DOVIZ + LISTE_BIST
    fmp_sonuclar = get_data_from_fmp(tum_fmp)
    
    data_borsa_tr = {}
    data_borsa_abd = {}
    data_doviz = {}
    
    # FMP verilerini kutulara dağıt
    for sembol, fiyat in fmp_sonuclar.items():
        if sembol in LISTE_BIST:
            data_borsa_tr[sembol.replace(".IS", "")] = fiyat
        elif sembol in LISTE_ABD:
            data_borsa_abd[sembol] = fiyat
        elif sembol in LISTE_DOVIZ:
            data_doviz[sembol.replace("TRY", "")] = fiyat # USDTRY -> USD
            
    print(f"   -> ✅ FMP Bitti: BIST({len(data_borsa_tr)}), ABD({len(data_borsa_abd)}), Döviz({len(data_doviz)})")

    # 4. ALTIN (Siteden Kazıma)
    print("4. Altın verileri çekiliyor...")
    data_altin = {}
    try:
        session = requests.Session()
        r = session.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        from bs4 import BeautifulSoup
        if r.status_code == 200:
            soup = BeautifulSoup(r.content, "html.parser")
            for satir in soup.find_all("tr"):
                cols = satir.find_all("td")
                if len(cols) > 2:
                    try:
                        isim = cols[0].get_text(strip=True)
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(cols[2].get_text(strip=True))
                            if fiyat > 0: data_altin[isim] = fiyat
                    except: continue
    except: pass
    print(f"   -> ✅ Altın Bitti: {len(data_altin)} adet")

    # 5. KAYIT
    final_paket = {
        "borsa_tr_tl": data_borsa_tr,
        "borsa_abd_usd": data_borsa_abd,
        "kripto_usd": data_kripto,
        "doviz_tl": data_doviz,
        "altin_tl": data_altin,
        "fon_tl": data_fonlar
    }

    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        db.collection(u'market_history').document(bugun_tarih).set(
            {u'hourly': {su_an_saat_dakika: final_paket}}, merge=True
        )
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] FMP + CMC + TEFAS Kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
