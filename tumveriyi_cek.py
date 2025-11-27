import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import sys
import os
import yfinance as yf
import pandas as pd

# --- AYARLAR ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/115.0.0.0 Safari/537.36"
}

# --- FIREBASE BAĞLANTISI ---
if not os.path.exists("serviceAccountKey.json"):
    print("HATA: serviceAccountKey.json bulunamadı!")
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
# 1. ABD BORSASI (S&P 500 TAM LİSTE)
# ==============================================================================
LISTE_ABD = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "BRK-B", "LLY", "AVGO", "V", "JPM", "XOM", "WMT", "UNH", "MA", "PG", "JNJ", "HD", "MRK", "COST", "ABBV", "CVX", "CRM", "BAC", "AMD", "PEP", "KO", "NFLX", "ADBE", "DIS", "MCD", "CSCO", "TMUS", "ABT", "INTC", "INTU", "CMCSA", "PFE", "NKE", "WFC", "QCOM", "TXN", "DHR", "PM", "UNP", "IBM", "AMGN", "GE", "HON", "BA", "SPY", "QQQ", "UBER", "PLTR",
    "LIN", "ACN", "RTX", "VZ", "T", "CAT", "LOW", "BKNG", "NEE", "GS", "MS", "BMY", "DE", "MDT", "SCHW", "BLK", "TJX", "PGR", "COP", "ISRG", "LMT", "ADP", "AXP", "MMC", "GILD", "VRTX", "C", "MDLZ", "ADI", "REGN", "LRCX", "CI", "CVS", "BSX", "ZTS", "AMT", "ETN", "SLB", "FI", "BDX", "SYK", "CB", "EOG", "TM", "SO", "CME", "MU", "KLAC", "PANW", "MO", "SHW", "SNPS", "EQIX", "CDNS", "ITW", "DUK", "CL", "APH", "PYPL", "CSX", "PH", "TGT", "USB", "ICE", "NOC", "WM", "FCX", "GD", "NXPI", "ORLY", "HCA", "MCK", "EMR", "MAR", "PNC", "PSX", "BDX", "ROP", "NSC", "GM", "FDX", "MCO", "AFL", "CARR", "ECL", "APD", "AJG", "MSI", "AZO", "TT", "WMB", "TFC", "COF", "PCAR", "D", "SRE", "AEP", "HLT", "O", "TRV", "MET", "PSA", "PAYX", "ROST", "KMB", "JCI", "URI", "ALL", "PEG", "ED", "XEL", "GWW", "YUM", "FAST", "WELL", "AMP", "DLR", "VLO", "AME", "CMI", "FIS", "ILMN", "AIG", "KR", "PPG", "KMI", "DFS", "EXC", "LUV", "DAL", "OXY", "PSX", "VLO", "HES", "KMI", "WMB", "OKE", "TRGP", "CTRA", "DVN", "FANG", "HAL", "BKR", "MRO", "APA", "EQT", "OVV", "CHK", "SWN", "AR", "RRC", "MTDR", "PDCE", "CIVI", "CNX", "CRK", "MGY", "SM", "VNOM", "ESTE", "MUR", "LPI", "CPE", "TALO", "WLL", "OAS"
]

# ==============================================================================
# 2. KRİPTO (TOP 250 - DEV LİSTE)
# ==============================================================================
LISTE_KRIPTO = [
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD", "DOGE-USD", "TRX-USD", "DOT-USD",
    "LINK-USD", "LTC-USD", "SHIB-USD", "ATOM-USD", "XLM-USD", "NEAR-USD", "INJ-USD", "FIL-USD", "HBAR-USD", "LDO-USD",
    "ARB-USD", "ALGO-USD", "SAND-USD", "QNT-USD", "VET-USD", "OP-USD", "EGLD-USD", "AAVE-USD", "THETA-USD", "AXS-USD",
    "MANA-USD", "EOS-USD", "FLOW-USD", "XTZ-USD", "MKR-USD", "SNX-USD", "NEO-USD", "JASMY-USD", "KLAY-USD", "GALA-USD",
    "CFX-USD", "CHZ-USD", "CRV-USD", "ZEC-USD", "XEC-USD", "IOTA-USD", "LUNC-USD", "BTT-USD", "MINA-USD", "DASH-USD",
    "CAKE-USD", "RUNE-USD", "KAVA-USD", "ENJ-USD", "ZIL-USD", "BAT-USD", "TWT-USD", "QTUM-USD", "CELO-USD", "RVN-USD",
    "LRC-USD", "ENS-USD", "CVX-USD", "YFI-USD", "ANKR-USD", "1INCH-USD", "HOT-USD", "PEPE-USD", "FLOKI-USD", "BONK-USD",
    "WIF-USD", "FET-USD", "AGIX-USD", "RNDR-USD", "GRT-USD", "OCEAN-USD", "AKT-USD", "TAO-USD", "KAS-USD", "SUI-USD",
    "SEI-USD", "TIA-USD", "ORDI-USD", "BLUR-USD", "MEME-USD", "BGB-USD", "GT-USD", "TKX-USD", "KCS-USD", "HT-USD",
    "OKB-USD", "LEO-USD", "CRO-USD", "BIT-USD", "XAUT-USD", "PAXG-USD", "TUSD-USD", "USDD-USD", "FDUSD-USD", "PYUSD-USD",
    "GNO-USD", "ROSE-USD", "WLD-USD", "STRK-USD", "JUP-USD", "DYM-USD", "PYTH-USD", "MANTA-USD", "ALT-USD", "XAI-USD",
    "AI-USD", "NFP-USD", "ACE-USD", "JTO-USD", "BEAM-USD", "GAS-USD", "GLM-USD", "SC-USD", "ONT-USD", "IOST-USD",
    "WAKP-USD", "SXP-USD", "ICX-USD", "KDA-USD", "ZRX-USD", "BCH-USD", "ETC-USD", "APT-USD", "IMX-USD", "STX-USD",
    "ICP-USD", "MNT-USD", "FDUSD-USD", "HNT-USD", "BSV-USD", "BTT-USD", "XMR-USD", "EGLD-USD", "ASTR-USD", "OSMO-USD",
    "RPL-USD", "FXS-USD", "COMP-USD", "NEXO-USD", "GMX-USD", "WOO-USD", "ILV-USD", "YGG-USD", "MAGIC-USD", "GMT-USD",
    "APE-USD", "LUNA-USD", "USTC-USD", "VGX-USD", "SRM-USD", "RAY-USD", "FIDA-USD", "MSOL-USD", "STSOL-USD", "LSETH-USD",
    "RETH-USD", "WETH-USD", "WBTC-USD", "HBTC-USD", "REN-USD", "UMA-USD", "BAL-USD", "BNT-USD", "SNT-USD", "OMG-USD",
    "POLY-USD", "POWR-USD", "STORJ-USD", "SKL-USD", "CTSI-USD", "OGN-USD", "OXT-USD", "NMR-USD", "RLC-USD", "BAND-USD",
    "TRB-USD", "DIA-USD", "API3-USD", "LPT-USD", "OCEAN-USD", "AGLD-USD", "RAD-USD", "GODS-USD", "HIGH-USD", "ERN-USD",
    "SUPER-USD", "POLS-USD", "BICO-USD", "C98-USD", "ALICE-USD", "TLM-USD", "ATA-USD", "LINA-USD", "DENT-USD", "WIN-USD",
    "STMX-USD", "DGB-USD", "XVG-USD", "ZEN-USD", "ARRR-USD", "KMD-USD", "SYS-USD", "LSK-USD", "STEEM-USD", "HIVE-USD",
    "BTS-USD", "NXT-USD", "IGNIS-USD", "ARDR-USD", "XEM-USD", "XYM-USD", "WAVES-USD", "DCR-USD", "SC-USD", "MAID-USD",
    "XCP-USD", "PPC-USD", "NMC-USD", "FTC-USD", "VTC-USD", "MONA-USD", "VIA-USD", "FLO-USD", "BLK-USD", "QRK-USD",
    "MEC-USD", "WDC-USD", "IFC-USD", "TRC-USD", "IXC-USD", "I0C-USD", "DVC-USD", "GLC-USD", "YAC-USD", "CNC-USD",
    "FTM-USD", "UNI-USD" # Bunlar hata verebilir ama listede kalsın, gelirse gelir
]

# ==============================================================================
# 3. DÖVİZ
# ==============================================================================
LISTE_DOVIZ = [
    "USDTRY=X", "EURTRY=X", "GBPTRY=X", "CHFTRY=X", "CADTRY=X", "JPYTRY=X", "AUDTRY=X",
    "EURUSD=X", "GBPUSD=X", "JPY=X", "DX-Y.NYB"
]

# ==============================================================================
# 4. BIST (TAM LİSTE)
# ==============================================================================
LISTE_BIST = [
    "A1CAP.IS", "ACSEL.IS", "ADEL.IS", "ADESE.IS", "ADGYO.IS", "AEFES.IS", "AFYON.IS", "AGESA.IS", "AGHOL.IS", "AGROT.IS", "AGYO.IS", "AHGAZ.IS", "AKBNK.IS", "AKCNS.IS", "AKENR.IS", "AKFGY.IS", "AKFYE.IS", "AKGRT.IS", "AKMGY.IS", "AKSA.IS", "AKSEN.IS", "AKSGY.IS", "AKSUE.IS", "AKYHO.IS", "ALARK.IS", "ALBRK.IS", "ALCAR.IS", "ALCTL.IS", "ALFAS.IS", "ALGYO.IS", "ALKA.IS", "ALKIM.IS", "ALMAD.IS", "ALTNY.IS", "ANELE.IS", "ANGEN.IS", "ANHYT.IS", "ANSGR.IS", "ARASE.IS", "ARCLK.IS", "ARDYZ.IS", "ARENA.IS", "ARSAN.IS", "ARZUM.IS", "ASELS.IS", "ASGYO.IS", "ASTOR.IS", "ASUZU.IS", "ATAGY.IS", "ATAKP.IS", "ATATP.IS", "ATEKS.IS", "ATLAS.IS", "ATSYH.IS", "AVGYO.IS", "AVHOL.IS", "AVOD.IS", "AVPGY.IS", "AVTUR.IS", "AYCES.IS", "AYDEM.IS", "AYEN.IS", "AYES.IS", "AYGAZ.IS", "AZTEK.IS", 
    "BAGFS.IS", "BAKAB.IS", "BALAT.IS", "BANVT.IS", "BARMA.IS", "BASCM.IS", "BASGZ.IS", "BAYRK.IS", "BEGYO.IS", "BERA.IS", "BEYAZ.IS", "BFREN.IS", "BIENY.IS", "BIGCH.IS", "BIMAS.IS", "BINHO.IS", "BIOEN.IS", "BIZIM.IS", "BJKAS.IS", "BLCYT.IS", "BMSCH.IS", "BMSTL.IS", "BNTAS.IS", "BOBET.IS", "BORLS.IS", "BOSSA.IS", "BRISA.IS", "BRKO.IS", "BRKSN.IS", "BRKVY.IS", "BRLSM.IS", "BRMEN.IS", "BRSAN.IS", "BRYAT.IS", "BSOKE.IS", "BTCIM.IS", "BUCIM.IS", "BURCE.IS", "BURVA.IS", "BVSAN.IS", "BYDNR.IS", 
    "CANTE.IS", "CASA.IS", "CCOLA.IS", "CELHA.IS", "CEMAS.IS", "CEMTS.IS", "CEOEM.IS", "CIMSA.IS", "CLEBI.IS", "CMBTN.IS", "CMENT.IS", "CONSE.IS", "COSMO.IS", "CRDFA.IS", "CRFSA.IS", "CUSAN.IS", "CVKMD.IS", "CWENE.IS", 
    "DAGHL.IS", "DAGI.IS", "DAPGM.IS", "DARDL.IS", "DENGE.IS", "DERHL.IS", "DERIM.IS", "DESA.IS", "DESPC.IS", "DEVA.IS", "DGATE.IS", "DGGYO.IS", "DGNMO.IS", "DIRIT.IS", "DITAS.IS", "DMSAS.IS", "DNISI.IS", "DOAS.IS", "DOBUR.IS", "DOCO.IS", "DOGUB.IS", "DOHOL.IS", "DOKTA.IS", "DURDO.IS", "DYOBY.IS", "DZGYO.IS", 
    "EBEBK.IS", "ECILC.IS", "ECZYT.IS", "EDATA.IS", "EDIP.IS", "EGEEN.IS", "EGGUB.IS", "EGPRO.IS", "EGSER.IS", "EKGYO.IS", "EKIZ.IS", "EKSUN.IS", "ELITE.IS", "EMKEL.IS", "EMNIS.IS", "ENJSA.IS", "ENKAI.IS", "ENSRI.IS", "EPLAS.IS", "ERBOS.IS", "ERCB.IS", "EREGL.IS", "ERSU.IS", "ESCAR.IS", "ESCOM.IS", "ESEN.IS", "ETILR.IS", "ETYAT.IS", "EUHOL.IS", "EUKYO.IS", "EUPWR.IS", "EUREN.IS", "EUYO.IS", "EYGYO.IS", 
    "FADE.IS", "FENER.IS", "FLAP.IS", "FMIZP.IS", "FONET.IS", "FORMT.IS", "FORTE.IS", "FRIGO.IS", "FROTO.IS", "FZCMI.IS", 
    "GARAN.IS", "GARFA.IS", "GEDIK.IS", "GEDZA.IS", "GENIL.IS", "GENTS.IS", "GEREL.IS", "GESAN.IS", "GIPTA.IS", "GLBMD.IS", "GLRYH.IS", "GLYHO.IS", "GMTAS.IS", "GOKNR.IS", "GOLTS.IS", "GOODY.IS", "GOZDE.IS", "GRNYO.IS", "GRSEL.IS", "GRTRK.IS", "GUBRF.IS", "GWIND.IS", "GZNMI.IS", 
    "HALKB.IS", "HATEK.IS", "HATSN.IS", "HDFGS.IS", "HEDEF.IS", "HEKTS.IS", "HKTM.IS", "HLGYO.IS", "HTTBT.IS", "HUBVC.IS", "HUNER.IS", "HURGZ.IS", 
    "ICBCT.IS", "IDEAS.IS", "IDGYO.IS", "IEYHO.IS", "IHAAS.IS", "IHEVA.IS", "IHGZT.IS", "IHLAS.IS", "IHLGM.IS", "IHYAY.IS", "IMASM.IS", "INDES.IS", "INFO.IS", "INGRM.IS", "INTEM.IS", "INVEO.IS", "INVES.IS", "IPEKE.IS", "ISATR.IS", "ISBIR.IS", "ISBTR.IS", "ISCTR.IS", "ISDMR.IS", "ISFIN.IS", "ISGSY.IS", "ISGYO.IS", "ISKPL.IS", "ISKUR.IS", "ISMEN.IS", "ISSEN.IS", "ISYAT.IS", "ITTFH.IS", "IZENR.IS", "IZFAS.IS", "IZINV.IS", "IZMDC.IS", 
    "JANTS.IS", 
    "KAPLM.IS", "KAREL.IS", "KARSN.IS", "KARTN.IS", "KATMR.IS", "KAYSE.IS", "KCAER.IS", "KFEIN.IS", "KGYO.IS", "KIMMR.IS", "KLGYO.IS", "KLKIM.IS", "KLMSN.IS", "KLNMA.IS", "KLRHO.IS", "KLSYN.IS", "KMPUR.IS", "KNFRT.IS", "KONKA.IS", "KONTR.IS", "KONYA.IS", "KOPOL.IS", "KORDS.IS", "KOZAA.IS", "KOZAL.IS", "KRDMA.IS", "KRDMB.IS", "KRDMD.IS", "KRGYO.IS", "KRONT.IS", "KRPLS.IS", "KRSTL.IS", "KRTEK.IS", "KRVGD.IS", "KSTUR.IS", "KTLEV.IS", "KTSKR.IS", "KUTPO.IS", "KUYAS.IS", "KZBGY.IS", "KZGYO.IS", 
    "LIDER.IS", "LIDFA.IS", "LINK.IS", "LKMNH.IS", "LOGO.IS", "LORAS.IS", "LUKSK.IS", 
    "MAALT.IS", "MACKO.IS", "MAGEN.IS", "MAKIM.IS", "MAKTK.IS", "MANAS.IS", "MARKA.IS", "MARTI.IS", "MAVI.IS", "MEDTR.IS", "MEGAP.IS", "MEPET.IS", "MERCN.IS", "MERIT.IS", "MERKO.IS", "METRO.IS", "METUR.IS", "MGROS.IS", "MIATK.IS", "MIPAZ.IS", "MMCAS.IS", "MNDRS.IS", "MNDTR.IS", "MOBTL.IS", "MPARK.IS", "MRGYO.IS", "MRSHL.IS", "MSGYO.IS", "MTRKS.IS", "MTRYO.IS", "MZHLD.IS", 
    "NATEN.IS", "NETAS.IS", "NIBAS.IS", "NTGAZ.IS", "NTHOL.IS", "NUGYO.IS", "NUHCM.IS", 
    "OBAMS.IS", "ODAS.IS", "OFSYM.IS", "ONCSM.IS", "ORCAY.IS", "ORGE.IS", "ORMA.IS", "OSMEN.IS", "OSTIM.IS", "OTKAR.IS", "OTTO.IS", "OYAKC.IS", "OYAYO.IS", "OYLUM.IS", "OYYAT.IS", "OZGYO.IS", "OZKGY.IS", "OZRDN.IS", "OZSUB.IS", 
    "PAGYO.IS", "PAMEL.IS", "PAPIL.IS", "PARSN.IS", "PASEU.IS", "PCILT.IS", "PEGYO.IS", "PEKGY.IS", "PENGD.IS", "PENTA.IS", "PETKM.IS", "PETUN.IS", "PGSUS.IS", "PINSU.IS", "PKART.IS", "PKENT.IS", "PLAT.IS", "PLTUR.IS", "PNLSN.IS", "PNSUT.IS", "POLHO.IS", "POLTK.IS", "PRDGS.IS", "PRKAB.IS", "PRKME.IS", "PRZMA.IS", "PSGYO.IS", "PSDTC.IS", 
    "QNBFB.IS", "QNBFL.IS", "QUAGR.IS", 
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
    print("--- MEGA FİNANS BOTU (250 KRİPTO DAHİL) ---")
    
    # 2. TOPLU İNDİRME
    tum_semboller = LISTE_ABD + LISTE_KRIPTO + LISTE_DOVIZ + LISTE_BIST
    print(f"Toplam Varlık Sayısı: {len(tum_semboller)} adet (BIST: {len(LISTE_BIST)})")
    
    print("Yahoo Finance verileri çekiliyor...")
    # period="5d" -> Hafta sonu boşluğunu doldurmak için
    df = yf.download(tum_semboller, period="5d", progress=False, threads=True, auto_adjust=True)['Close']
    
    data_borsa_tr = {}
    data_borsa_abd = {}
    data_kripto = {}
    data_doviz = {}
    
    if not df.empty:
        df_dolu = df.ffill() # Tatil günlerini doldur
        son_fiyatlar = df_dolu.iloc[-1]
        
        for sembol in tum_semboller:
            try:
                fiyat = son_fiyatlar.get(sembol)
                if pd.notna(fiyat):
                    fiyat = round(float(fiyat), 2)
                    
                    if sembol in LISTE_BIST:
                        data_borsa_tr[sembol.replace(".IS", "")] = fiyat
                    elif sembol in LISTE_ABD:
                        data_borsa_abd[sembol] = fiyat
                    elif sembol in LISTE_KRIPTO:
                        data_kripto[sembol.replace("-USD", "")] = fiyat
                    elif sembol in LISTE_DOVIZ:
                        data_doviz[sembol.replace("TRY=X", "").replace("=X", "")] = fiyat
            except: continue
    
    print(f"✅ Yahoo Bitti: BIST({len(data_borsa_tr)}), ABD({len(data_borsa_abd)}), Kripto({len(data_kripto)}), Döviz({len(data_doviz)})")

    # 3. ALTIN
    print("Altın verileri çekiliyor...")
    data_altin = {}
    try:
        session = requests.Session()
        r = session.get("https://altin.doviz.com/", headers=headers, timeout=20)
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
    print(f"✅ Altın Bitti: {len(data_altin)} adet")

    # 3. KAYIT
    final_paket = {
        "borsa_tr_tl": data_borsa_tr,
        "borsa_abd_usd": data_borsa_abd,
        "kripto_usd": data_kripto,
        "doviz_tl": data_doviz,
        "altin_tl": data_altin
    }

    if any(final_paket.values()):
        simdi = datetime.now()
        bugun_tarih = simdi.strftime("%Y-%m-%d")
        su_an_saat_dakika = simdi.strftime("%H:%M")
        
        db.collection(u'market_history').document(bugun_tarih).set(
            {u'hourly': {su_an_saat_dakika: final_paket}}, merge=True
        )
        print(f"🎉 BAŞARILI: [{bugun_tarih} - {su_an_saat_dakika}] Dev Veri Paketi Kaydedildi.")
    else:
        print("❌ HATA: Veri yok!")
        sys.exit(1)

except Exception as e:
    print(f"KRİTİK HATA: {e}")
    sys.exit(1)
