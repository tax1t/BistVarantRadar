import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Ana dizindeki main.py'deki fonksiyonu Ã§aÄŸÄ±racaÄŸÄ±z
from main import run_simulation_api
from decision.exceptions import InsufficientConfidenceError

# Scanner iÃ§in gerekenler
from scanner.universal_scanner import UniversalScanner
from data.pipeline import DataPipeline
from data.providers.yfinance_provider import YFinanceProvider
from config.bist_symbols import BIST_SYMBOLS, BIST30_SYMBOLS, BIST50_SYMBOLS, YILDIZ_SYMBOLS, FX_SYMBOLS, COMMODITY_SYMBOLS, CRYPTO_SYMBOLS
import feedparser
import threading
import time
import json
import os

STATS_FILE = "stats.json"
GLOBAL_OPPORTUNITIES_CACHE = []

def load_stats():
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r") as f:
                return json.load(f)
        except:
            return {"total_analyzed": 0}
    return {"total_analyzed": 0}

def save_stats(stats):
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(stats, f)
    except Exception as e:
        print(f"Stats save error: {e}")

CACHE_FILE = "dashboard_cache.json"

def load_dashboard_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_dashboard_cache(data):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Cache save error: {e}")

GLOBAL_DASHBOARD_CACHE = load_dashboard_cache()
def background_scanner():
    """Arka planda çalışıp periyodik olarak TÜM BIST fırsatlarını tarar ve belleğe alır."""
    global GLOBAL_DASHBOARD_CACHE
    pipeline = DataPipeline()
    scanner = UniversalScanner(pipeline)
    
    # Hızlı Başlangıç (Fast Start): Eğer cache boşsa kullanıcıyı bekletmemek için sadece BIST 50'yi anında tara
    if not GLOBAL_DASHBOARD_CACHE:
        try:
            print("[BACKGROUND] Hızlı Başlangıç (Fast Start) - Sadece BIST 50 taranıyor...")
            fast_results = scanner.scan_pool_bulk(BIST50_SYMBOLS)
            
            # 1d taraması bitince HEMEN cache'i doldur ki kullanıcı beklemeden verileri görsün
            GLOBAL_DASHBOARD_CACHE = fast_results
            save_dashboard_cache(fast_results)
           
            print("[BACKGROUND] Hızlı Başlangıç Faz 1 tamamlandı - Günlük veriler HAZIR!")
            
            # Şimdi 1h taramasını yap, bitince cache'i güncelle
            print("[BACKGROUND] Hızlı Başlangıç Faz 2 - 1 Saatlik (1h) Taraması yapılıyor...")
            fast_1h = scanner.scan_pool_bulk_1h(BIST50_SYMBOLS)
            fast_results["opportunities_1h"] = fast_1h
            # --- Varant Eşleştirme ve Risk Filtresi ---
            # --- Varant Eşleştirme ve Risk Filtresi ---
            warrant_opportunities = []
            for signal in fast_1h:
                # --- Test Amaçlı Direkt Listeleme ---
             warrant_opportunities = []
            for signal in fast_1h[:5]:  # İlk 5 hisseyi direkt alalım
                symbol = signal.get("symbol", "ASELS") 
                signal["warrant_match"] = f"{symbol} için test varantı eşleştirildi"
                warrant_opportunities.append(signal)
                
                # --- TELEGRAM BİLDİRİM KISMI ---
                try:
                    import requests
                    from services.notification_manager import NotificationManager
                    
                    notif = NotificationManager()
                    if notif.telegram_token and notif.telegram_chat_id:
                        mesaj = f"🚨 YENİ BIST50 FIRSATI!\n\n📈 Hisse: {symbol}\n🎯 Durum: {signal['warrant_match']}"
                        url = f"https://api.telegram.org/bot{notif.telegram_token}/sendMessage"
                        requests.post(url, json={"chat_id": notif.telegram_chat_id, "text": mesaj})
                except Exception as e:
                    print(f"Telegram hatası: {e}")
                # -------------------------------

            fast_results["warrant_opportunities_1h"] = warrant_opportunities
            if signal.get("score") == 3:
                    symbol = signal.get("symbol")
                    signal["warrant_match"] = f"{symbol} için en uygun alım varantı eşleştirildi"
                    warrant_opportunities.append(signal)

            fast_results["warrant_opportunities_1h"] = warrant_opportunities

            # 1h sinyalleri taranır taranmaz varant eşleştirmesini çalıştırıyoruz
            fast_results["warrant_opportunities_1h"] = filter_warrants_for_signals(fast_1h)
            GLOBAL_DASHBOARD_CACHE = fast_results
            save_dashboard_cache(fast_results)
            print("[BACKGROUND] Hızlı Başlangıç Faz 2 tamamlandı - 1h fırsatları da HAZIR!")
        except Exception as e:
            print(f"[BACKGROUND] Hızlı Başlangıç Hatası: {e}")
            import traceback
            traceback.print_exc()

    while True:
        try:
            print("[BACKGROUND] Tüm BIST hisseleri için Kapsamlı (Bulk) Günlük Data indiriliyor...")
            
            # 550 hisseyi tek bir pakette indir:
            results = scanner.scan_pool_bulk(BIST_SYMBOLS)
            
            # Günlük verileri kaydet AMA eski 1h verisini koru!
            existing_1h = GLOBAL_DASHBOARD_CACHE.get("opportunities_1h", [])
            if existing_1h:
                results["opportunities_1h"] = existing_1h
            GLOBAL_DASHBOARD_CACHE = results
            save_dashboard_cache(results)
            print("[BACKGROUND] Günlük veriler güncellendi. 1h taraması başlıyor...")
            
            opportunities_1h = scanner.scan_pool_bulk_1h(BIST_SYMBOLS)
            
            # 1h verilerini de ekle
            results["opportunities_1h"] = opportunities_1h
            
            GLOBAL_DASHBOARD_CACHE = results
            save_dashboard_cache(results)
            
            print(f"[BACKGROUND] Kapsamlı Tarama tamamlandı. Veriler önbelleğe ve diske kaydedildi.")
            
        except Exception as e:
            print(f"[BACKGROUND] Bulk Tarama hatası: {e}")
            import traceback
            traceback.print_exc()
            
        # Dinlen (15 dakika)
        time.sleep(900)

# Varant Sembolleri (Örnek Liste - IS Warrant yapısı)
WARRANT_SYMBOLS = [
    "GARAN-240726-C-130.IS", "GARAN-240726-P-120.IS",
    "THYAO-240726-C-350.IS", "THYAO-240726-P-300.IS",
    "ASELS-240726-C-80.IS", "ASELS-240726-P-60.IS",
    "TUPRS-240726-C-200.IS", "TUPRS-240726-P-150.IS",
    "AKBNK-240726-C-60.IS", "AKBNK-240726-P-50.IS",
    "EREGL-240726-C-60.IS", "EREGL-240726-P-45.IS",
    "SAHOL-240726-C-90.IS", "SAHOL-240726-P-75.IS",
    "BIMAS-240726-C-600.IS", "BIMAS-240726-P-500.IS",
    "KCHOL-240726-C-250.IS", "KCHOL-240726-P-200.IS",
    "SISE-240726-C-100.IS", "SISE-240726-P-80.IS",
]

# Tüm sembol listesi (autocomplete için)
ALL_SYMBOLS = [s.replace('.IS','') for s in BIST_SYMBOLS] + [w.replace('.IS','') for w in WARRANT_SYMBOLS] + FX_SYMBOLS + COMMODITY_SYMBOLS + CRYPTO_SYMBOLS

app = Flask(__name__, static_folder='ui')
CORS(app) # GeliÅŸtirme aÅŸamasÄ± iÃ§in Cross-Origin izin verilir

@app.route('/')
def serve_ui():
    """VarsayÄ±lan olarak index.html'i aÃ§ar"""
    return send_from_directory('ui', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    """CSS ve JS dosyalarÄ±nÄ± UI klasÃ¶rÃ¼nden sunar"""
    return send_from_directory('ui', path)

import math
import numpy as np
import pandas as pd

def sanitize_for_json(obj):
    """
    Sözlük veya liste içindeki tüm NumPy tiplerini ve NaN değerlerini 
    JSON ile uyumlu standart Python tiplerine (None vb.) çevirir.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float) or isinstance(obj, np.floating):
        if math.isnan(obj) or np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.ndarray):
        return sanitize_for_json(obj.tolist())
    elif pd.isna(obj): # pd.NaT veya pandas NA
        return None
    return obj

@app.route('/api/analyze', methods=['GET'])
def api_analyze():
    """Ana analiz uÃ§ noktasÄ± (Ã–rn: /api/analyze?symbol=AAPL)"""
    symbol = request.args.get('symbol', 'AAPL').upper()
    
    # Türk hisseleri (Örn: EFOR, AHSGY, SASA, GARAN) 4 veya 5 harfli olabilir.
    # Eğer sonu .IS ile bitmiyorsa ve döviz/kripto değilse otomatik .IS ekliyoruz.
    # Özel Düzeltmeler (Kullanıcılar genelde ALTIN.S1 yazar)
    if symbol == "ALTIN.S1" or symbol == "ALTINS1":
        symbol = "ALTINS1.IS"
        
    # Eğer sonu .IS ile bitmiyorsa ve döviz/kripto değilse otomatik .IS ekliyoruz.
    if not symbol.endswith(".IS"):
        if symbol not in FX_SYMBOLS and symbol not in CRYPTO_SYMBOLS and symbol not in COMMODITY_SYMBOLS:
            symbol = f"{symbol}.IS"
    try:
        # main.py iÃ§erisindeki o devasa 13 soruluk dÃ¶ngÃ¼yÃ¼ baÅŸlat
        report = run_simulation_api(symbol)
        
        # Analiz sayacını artır
        stats = load_stats()
        stats["total_analyzed"] = stats.get("total_analyzed", 0) + 1
        save_stats(stats)
        
        if "error" in report:
            return jsonify({"status": "error", "message": report["error"]}), 400
            
        # JSON'a çevrilirken hata vermemesi için NaN'ları ve Numpy tiplerini temizle
        safe_report = sanitize_for_json(report)
        
        return jsonify({
            "status": "success",
            "symbol": symbol,
            "report": safe_report
        })
        
    except InsufficientConfidenceError as e:
        # DeÄŸiÅŸtirilemez Ä°lke (BÃ¶lÃ¼m 18) Devreye Girdi
        return jsonify({
            "status": "rejected",
            "symbol": symbol,
            "message": str(e)
        }), 403
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/autocomplete', methods=['GET'])
def api_autocomplete():
    """Hisse veya Varant sembolünün ilk harflerine göre eşleşen listesini döndürür."""
    q = request.args.get('q', '').upper()
    if len(q) < 1:
        return jsonify([])
    matches = [s for s in ALL_SYMBOLS if s.startswith(q)][:15]
    return jsonify(matches)

@app.route('/api/dashboard_init', methods=['GET'])
def api_dashboard_init():
    """Ön yüz ilk açıldığında gösterilecek Fırsatları ve Sayaçları döner."""
    # Taranan toplam benzersiz hisse sayısını cache'den hesapla
    total = 0
    if GLOBAL_DASHBOARD_CACHE:
        seen = set()
        for cat_name, cat_items in GLOBAL_DASHBOARD_CACHE.items():
            if isinstance(cat_items, list):
                for item in cat_items:
                    if isinstance(item, dict) and 'Symbol' in item:
                        seen.add(item['Symbol'])
        total = len(seen)
    
    return jsonify({
        "status": "success",
        "total_analyzed": total,
        "dashboard_data": GLOBAL_DASHBOARD_CACHE
    })

@app.route('/api/pool_info', methods=['GET'])
def pool_info():
    """Önyüze radar havuzunun boyutunu döndürür."""
    pool = BIST30_SYMBOLS
    return jsonify({"status": "success", "pool_size": len(pool), "pool": pool})

@app.route('/api/scan', methods=['GET'])
def api_scan():
    """Hisse Radarı: BIST30 listesini tarar."""
    pool = BIST30_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_warrants', methods=['GET'])
def api_scan_warrants():
    """Varant Radarı: Varant listesini tarar."""
    pool = WARRANT_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_bist50', methods=['GET'])
def api_scan_bist50():
    pool = BIST50_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_yildiz', methods=['GET'])
def api_scan_yildiz():
    pool = YILDIZ_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_all', methods=['GET'])
def api_scan_all():
    pool = BIST_SYMBOLS # Artık Bulk Scan sayesinde tüm hisseleri saniyeler içinde tarayabilir.
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_fx', methods=['GET'])
def api_scan_fx():
    pool = FX_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_commodity', methods=['GET'])
def api_scan_commodity():
    pool = COMMODITY_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/scan_crypto', methods=['GET'])
def api_scan_crypto():
    pool = CRYPTO_SYMBOLS
    try:
        pipeline = DataPipeline()
        scanner = UniversalScanner(pipeline)
        results = scanner.scan_pool_bulk(pool).get("opportunities", [])
        return jsonify({"status": "success", "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route('/api/health', methods=['GET'])
def api_health():
    """Tüm veri sağlayıcılarının sağlık durumunu döndürür."""
    try:
        pipeline = DataPipeline()
        health_report = pipeline.get_health_report()
        return jsonify({"status": "success", "data": health_report})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/news/global', methods=['GET'])
def api_news_global():
    feeds = [
        "https://www.haberturk.com/rss/ekonomi.xml",
        "https://www.trthaber.com/ekonomi_articles.rss"
    ]
    news_items = []
    try:
        for feed_url in feeds:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:
                news_items.append({
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source": "Habertürk" if "haberturk" in feed_url else "TRT Haber"
                })
        return jsonify({"status": "success", "news": news_items})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/news/ticker/<symbol>', methods=['GET'])
def api_news_ticker(symbol):
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if not news:
            news = []
        return jsonify({"status": "success", "news": news})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/brokerage/<symbol>', methods=['GET'])
def api_brokerage(symbol):
    try:
        import yfinance as yf
        from analysis.broker_ai import generate_ai_akd
        
        # Get latest day info to generate AKD
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2d")
        
        if hist.empty or len(hist) < 1:
            return jsonify({"status": "error", "message": "No data available for symbol"})
            
        latest = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else latest
        
        c = float(latest['Close'])
        p = float(prev['Close'])
        vol = float(latest['Volume'])
        chg_pct = ((c - p) / p) * 100 if p > 0 else 0
        
        akd_data = generate_ai_akd(symbol, c, chg_pct, vol)
        return jsonify(akd_data)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    """Ayarları getiren ve güncelleyen API ucu."""
    # Bu endpoint'ler normalde bir yetkilendirme (API Key vb.) ile korunmalıdır.
    from database.db_manager import DBManager
    from services.notification_manager import NotificationManager

    if request.method == 'GET':
        try:
            notif = NotificationManager()
            db = DBManager()
            gemini_key = db.get_setting('gemini_api_key')
            
            return jsonify({
                "status": "success",
                "telegram_token": notif.telegram_token,
                "telegram_chat_id": notif.telegram_chat_id,
                "gemini_api_key": gemini_key
            })
        except Exception as e:
            # 500 hatası fırlatmak yerine boş değer döndürüyoruz ki arayüz çökmesin
            return jsonify({
                "status": "success",
                "telegram_token": "",
                "telegram_chat_id": "",
                "gemini_api_key": ""
            }), 200

    if request.method == 'POST':
     try:
            data = request.json
            notif = NotificationManager()
            db = DBManager()
            
            # Ayarları arka plana kaydediyoruz
            if 'telegram_token' in data and 'telegram_chat_id' in data:
                notif.update_settings(data['telegram_token'], data['telegram_chat_id'])
            
            if 'gemini_api_key' in data:
                db.set_setting('gemini_api_key', data['gemini_api_key'])
                
            return jsonify({"status": "success"})
            
     except Exception as e:
            # Sistem çökmek yerine hatanın ne olduğunu terminale yazsın
            print(f"KAYIT HATASI: {e}")
            return jsonify({"status": "success"}), 200
            
     except Exception as e:
            # Sistem çökmek yerine hatanın ne olduğunu terminale yazsın
            print(f"KAYIT HATASI: {e}")
            return jsonify({"status": "success"}), 200

@app.route('/api/chart_data', methods=['GET'])
def api_chart_data():
    symbol = request.args.get('symbol', 'AAPL').upper()
    interval = request.args.get('interval', '1d')
    try:
        from analysis.technical import TechnicalEngine
        data = TechnicalEngine.get_chart_data(symbol, interval)
        return jsonify(sanitize_for_json(data))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":

    print("[SYSTEM] VarantRadar Pro Web Server Baslatiliyor...")

    port = int(os.environ.get("PORT", 5000))

    # Flask debug restart yapınca ikinci thread açılmasın
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        t = threading.Thread(
            target=background_scanner,
            daemon=True
        )
        t.start()

    render_url = os.getenv("RENDER_EXTERNAL_URL")

    if render_url:
        print(f"[URL] Render URL: {render_url}")
    else:
        print(f"[URL] Local URL: http://127.0.0.1:{port}")

    if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e
