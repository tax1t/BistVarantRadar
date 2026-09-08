import concurrent.futures
from typing import List, Dict, Any
import yfinance as yf
import pandas as pd
import numpy as np

from data.pipeline import DataPipeline
from analysis.technical import TechnicalEngine
from core.event_bus import EventBus

# Yapay Zeka Favori 15 Hissesi (Kurumsal & Yüksek Momentumlu)
AI_FAVORITES = [
    "THYAO.IS", "TUPRS.IS", "KCHOL.IS", "SAHOL.IS", "FROTO.IS", 
    "ISCTR.IS", "YKBNK.IS", "AKBNK.IS", "GARAN.IS", "BIMAS.IS",
    "ASELS.IS", "SISE.IS", "ENKAI.IS", "EREGL.IS", "TTKOM.IS"
]

class UniversalScanner:
    """
    CFG-03 Scanner Layer (Evrensel Tarayıcı)
    Verilen sembol havuzunu (Örn: BIST100) paralel işleyerek tarar.
    Amacı binlerce hisse arasından ön filtrelemeyi geçebilen 'Adayları' bulmaktır.
    """
    
    def __init__(self, data_pipeline: DataPipeline):
        self.pipeline = data_pipeline
        self.tech_engine = TechnicalEngine()
        
    def scan_pool(self, symbols: List[str], min_score: float = 60.0) -> List[Dict[str, Any]]:
        """Hisse havuzunu paralel tarar ve eşik değerini geçenleri listeler."""
        
        candidates = []
        EventBus.publish("SCAN_STARTED", {"total": len(symbols)})
        
        # Multithreading (Paralel Tarama) ile API bekleme sürelerini (I/O) minimize ederiz
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_symbol = {executor.submit(self._scan_single, sym): sym for sym in symbols}
            
            for future in concurrent.futures.as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    result = future.result()
                    if result and result.get("Score", 0) >= min_score:
                        candidates.append({
                            "Symbol": sym,
                            "Score": result["Score"],
                            "Trend": result.get("Trend", "UNKNOWN"),
                            "Momentum": result.get("Momentum", "UNKNOWN")
                        })
                except Exception as e:
                    print(f"[Scanner] Hata ({sym}): {e}")
                    
        # Yüksek puandan düşüğe doğru sırala
        candidates = sorted(candidates, key=lambda x: x["Score"], reverse=True)
        EventBus.publish("SCAN_FINISHED", {"candidates_found": len(candidates)})
        
        return candidates
        
    def _scan_single(self, symbol: str) -> Dict[str, Any]:
        """Tek bir hisse için hızlı (sadece teknik) bir ön eleme yapar."""
        df = self.pipeline.get_clean_data(symbol, period="3mo", interval="1d")
        if df.empty:
            return None
            
        # Ön eleme genellikle çok hızlı olan Teknik Motor ile yapılır
        # Buradan geçerse Phase 5 (Executive Engine) detaylı analiz yapar
        tech_result = self.tech_engine.analyze(symbol, df)
        return tech_result

    def scan_pool_bulk(self, symbols: List[str]) -> Dict[str, List[Dict]]:
        """
        YFinance Bulk Download (Toplu İndirme) ile tüm sembolleri tek HTTP isteğinde çeker.
        IP Ban riskini sıfırlar ve Yükselen/Düşen/Hacimli/Sığ/Favori kategorilerini üretir.
        """
        EventBus.publish("SCAN_STARTED", {"total": len(symbols), "mode": "bulk"})
        print(f"[SCANNER] {len(symbols)} hisse tek seferde indiriliyor (Bulk Download)...")
        
        # Toplu indirme:
        data = yf.download(symbols, period="3mo", interval="1d", group_by='ticker', threads=False, progress=False)
        
        all_results = []
        
        if len(symbols) == 1:
            sym = symbols[0]
            df = data.dropna(how='all').copy()
            res = self._process_bulk_df(sym, df)
            if res: all_results.append(res)
        else:
            for sym in symbols:
                # yfinance 0.2.x ve sonrasında MultiIndex yapısı
                if hasattr(data.columns, 'levels') and sym in data.columns.levels[0]:
                    df = data[sym].dropna(how='all').copy()
                    res = self._process_bulk_df(sym, df)
                    if res: all_results.append(res)
                    
        # Şimdi sonuçları kategorize edelim:
        
        # 1. Yükselenler (En çok % artanlar)
        gainers = sorted([r for r in all_results if r["Change_Pct"] > 0], key=lambda x: x["Change_Pct"], reverse=True)[:20]
        
        # 2. Düşenler (En çok % azalanlar)
        losers = sorted([r for r in all_results if r["Change_Pct"] < 0], key=lambda x: x["Change_Pct"])[:20]
        
        # 3. En Hacimliler / Kurumsal (Volume * Price en yüksek)
        high_vol = sorted(all_results, key=lambda x: x["Money_Volume"], reverse=True)[:20]
        
        # 4. En Sığ Hisseler (Volume * Price en düşük)
        low_vol = sorted([r for r in all_results if r["Money_Volume"] > 0], key=lambda x: x["Money_Volume"])[:20]
        
        # 5. Favoriler (Sadece listemizdeki hisseler)
        favorites = [r for r in all_results if r["Symbol"] in AI_FAVORITES]
        favorites = sorted(favorites, key=lambda x: x.get("Score", 0), reverse=True)
        
        # 6. Fırsatlar (Skoru en yüksek olanlar AL sinyalli)
        opportunities = sorted([r for r in all_results if r.get("Score", 0) >= 65 and r.get("Status") == "AL"], 
                               key=lambda x: x["Score"], reverse=True)[:20]
                               
        print("[SCANNER] Bulk analiz tamamlandı ve kategorize edildi.")
        return {
            "opportunities": opportunities,
            "gainers": gainers,
            "losers": losers,
            "high_volume": high_vol,
            "low_volume": low_vol,
            "favorites": favorites
        }
        
    def _process_bulk_df(self, symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty or len(df) < 5:
            return None
            
        # Standartlaştır (Clean)
        df.columns = [str(c).lower() for c in df.columns]
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0 if col != 'volume' else 0
                
        df['close'] = df['close'].ffill()
        df = df[required_cols]
        
        # Günlük değişim hesapla
        close_today = float(df['close'].iloc[-1])
        close_yday = float(df['close'].iloc[-2]) if len(df) > 1 else close_today
        
        change_pct = ((close_today - close_yday) / close_yday * 100) if close_yday > 0 else 0
        volume_today = float(df['volume'].iloc[-1])
        money_volume = close_today * volume_today
        
        # Teknik motor analizi
        tech_result = self.tech_engine.analyze(symbol, df)
        if not tech_result:
            return None
            
        # Ek verileri sonucun içine göm
        tech_result["Symbol"] = symbol
        tech_result["Change_Pct"] = round(change_pct, 2)
        tech_result["Volume"] = volume_today
        tech_result["Money_Volume"] = money_volume
        tech_result["Price"] = round(close_today, 2)
        
        return tech_result

    def scan_pool_bulk_1h(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """
        1 Saatlik (1h) periyotta teknik fırsat taraması yapar.
        Yalnızca Score_5 >= 3 (Potansiyel) olanları döndürür.
        """
        EventBus.publish("SCAN_STARTED", {"total": len(symbols), "mode": "bulk_1h"})
        print(f"[SCANNER 1H] {len(symbols)} hisse 1 saatlik periyotta indiriliyor...")
        
        # 1 Saatlik periyot max 730 gün destekler. 1mo yeterli.
        data = yf.download(symbols, period="1mo", interval="1h", group_by='ticker', threads=False, progress=False)
        
        opportunities = []
        
        if len(symbols) == 1:
            sym = symbols[0]
            df = data.dropna(how='all').copy()
            res = self._process_bulk_df_1h(sym, df)
            if res: opportunities.append(res)
        else:
            for sym in symbols:
                if hasattr(data.columns, 'levels') and sym in data.columns.levels[0]:
                    df = data[sym].dropna(how='all').copy()
                    res = self._process_bulk_df_1h(sym, df)
                    if res: opportunities.append(res)
                    
        # Kesişim ne kadar yeniyse (Crossover_Bars_Ago az ise) o kadar üstte olsun.
        # İkinci kriter olarak fark (EMA_Gap_Pct) büyük olanı üste al.
        opportunities = sorted(
            opportunities,
            key=lambda x: (x.get("Crossover_Bars_Ago", 999), -x.get("EMA_Gap_Pct", 0))
        )
        # Sadece kullanıcının kuralını karşılayanları döndür (Sınır kaldırıldı, tüm fırsatlar gösterilecek)
        # opportunities = opportunities[:20]
        
        print(f"[SCANNER 1H] Tarama tamamlandı. {len(opportunities)} fırsat bulundu.")
        return opportunities

    def _process_bulk_df_1h(self, symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty or len(df) < 30:
            return None
            
        df.columns = [str(c).lower() for c in df.columns]
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                return None
                
        df['close'] = df['close'].ffill()
        df = df[required_cols]
        
        # 1 saatlik teknik analizi çağır
        return self.tech_engine.analyze_1h_opportunities(symbol, df)
