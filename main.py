import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import pandas as pd

from pipeline import DataPipeline

from analysis.technical import TechnicalEngine
from analysis.fundamental import FundamentalEngine
from analysis.smart_money import SmartMoneyEngine
from analysis.sentiment import SentimentEngine
from analysis.macro import MacroEngine
from analysis.options_engine import OptionsEngine

from decision.executive import ExecutiveDecisionEngine
from execution.planner import TradePlanner

# Kendi yazdığımız Backtest ve Optimizasyon Motorunu dahil ediyoruz
from engines.backtest_engine import BacktestEngine

def run_simulation(symbol: str):
    print(f"\n[VARANTRADAR PRO] {symbol} İçin 13 Altın Soru Analizi ve Optimizasyon Başlatılıyor...")
    print("-" * 60)
    
    # 1. DATA LAYER (CFG-03.1 Enterprise Data Architecture)
    print("[1/6] Veriler Çekiliyor (Multi-Source Failover Aktif)...")
    pipeline = DataPipeline()
    
    df = pipeline.get_clean_data(symbol, period="1y", interval="1d")
    
    if df.empty:
        print(f"HATA: {symbol} için veri çekilemedi!")
        return
        
    current_price = df['close'].iloc[-1]
    high_low = df['high'] - df['low']
    atr = high_low.rolling(14).mean().iloc[-1]
    
    # BIST100 (Makro) Verisi Çekelim -> Test için S&P 500 (^GSPC) kullanıyoruz
    print("[2/6] Makro Rejim Verisi Çekiliyor...")
    bist_df = pipeline.get_clean_data("^GSPC", period="1y", interval="1d")
    
    # 2. OPTİMİZASYON VE BACKTEST MOTORU (Kendi Kendini Test Eden Beyin)
    print("[3/6] Backtest ve Optimizasyon Motoru Çalıştırılıyor (En İyi Parametreler Aranıyor)...")
    backtester = BacktestEngine(initial_capital=100000.0, commission=0.001)
    
    # Kısa ve uzun hareketli ortalama periyotlarını test et (Örn: Kısa 5-15, Uzun 20-50)
    opt_sonuclari = backtester.optimize_parameters(
        df=df,
        short_window_range=range(5, 16, 2),
        long_window_range=range(20, 51, 5)
    )
    
    en_iyi_kisa = int(opt_sonuclari.iloc[0]['Kisa_Periyot'])
    en_iyi_uzun = int(opt_sonuclari.iloc[0]['Uzun_Periyot'])
    print(f"[*] En Optimizasyon Sonucu Seçildi -> En İyi Kısa: {en_iyi_kisa}, En İyi Uzun: {en_iyi_uzun}")

    # 3. ANALYSIS LAYER (Kaslar)
    print("[4/6] Analiz Motorları (Kaslar) Çalıştırılıyor...")
    tech_engine = TechnicalEngine()
    fund_engine = FundamentalEngine()
    smart_engine = SmartMoneyEngine()
    sent_engine = SentimentEngine()
    macro_engine = MacroEngine()
    
    tech_res = tech_engine.analyze(symbol, df)
    fund_res = fund_engine.analyze(symbol, df) 
    smart_res = smart_engine.analyze(symbol, df)
    sent_res = sent_engine.analyze(symbol, df) 
    macro_res = macro_engine.analyze("^GSPC", bist_df)
    options_engine = OptionsEngine()
    options_res = options_engine.analyze(symbol, df)
    
    # Optimizasyondan gelen veriyi teknik sonuca veya rapora işleyebiliriz
    tech_res['optimized_short_ma'] = en_iyi_kisa
    tech_res['optimized_long_ma'] = en_iyi_uzun
    
    # 4. DECISION ENGINE (Büyük Beyin)
    print("[5/6] Büyük Beyin ve Portföy Yöneticisi Karar Veriyor...")
    total_capital = 100000.0 # Örnek 100.000 TL Kasa
    
    final_report = ExecutiveDecisionEngine.generate_final_report(
        symbol=symbol,
        current_price=current_price,
        atr=atr,
        total_capital=total_capital,
        df=df,
        technical_result=tech_res,
        fundamental_result=fund_res,
        smart_money_result=smart_res,
        sentiment_result=sent_res,
        macro_result=macro_res,
        options_result=options_res
    )
    
    # 5. JSON ÇIKTISI
    print("[6/6] Analiz Tamamlandı. '13 Altın Soru' Raporu Basılıyor:\n")
    print("=" * 70)
    print(json.dumps(final_report, indent=4, ensure_ascii=False))
    print("=" * 70)
    
    return final_report

def run_simulation_api(symbol: str) -> dict:
    """Web API (Flask) üzerinden çağrılacak olan fonksiyon."""
    pipeline = DataPipeline()
    tech_engine = TechnicalEngine()
    fund_engine = FundamentalEngine()
    smart_engine = SmartMoneyEngine()
    sent_engine = SentimentEngine()
    macro_engine = MacroEngine()
    options_engine = OptionsEngine()
    backtester = BacktestEngine()
    
    df = pipeline.get_clean_data(symbol, period="1y", interval="1d")
    if df is None or df.empty:
        return {"error": f"Veri çekilemedi veya geçersiz sembol: {symbol}"}
        
    # Arka planda optimizasyon çalıştırıp en iyi parametreyi bulalım
    opt_res = backtester.optimize_parameters(df, range(5, 16, 5), range(20, 51, 10))
    best_short = int(opt_res.iloc[0]['Kisa_Periyot'])
    best_long = int(opt_res.iloc[0]['Uzun_Periyot'])
        
    high_low = df['high'] - df['low']
    atr = high_low.rolling(14).mean().iloc[-1]
    bist_df = pipeline.get_clean_data("^GSPC", period="1y", interval="1d")
    
    tech_res = tech_engine.analyze(symbol, df)
    tech_res['optimized_short_ma'] = best_short
    tech_res['optimized_long_ma'] = best_long
    
    fund_res = fund_engine.analyze(symbol, df) 
    smart_res = smart_engine.analyze(symbol, df)
    sent_res = sent_engine.analyze(symbol, df) 
    macro_res = macro_engine.analyze("^GSPC", bist_df)
    options_res = options_engine.analyze(symbol, df)
    
    current_price = df['close'].iloc[-1]
    total_capital = 100000.0

    json_report = ExecutiveDecisionEngine.generate_final_report(
        symbol=symbol,
        current_price=current_price,
        atr=atr,
        total_capital=total_capital,
        df=df,
        technical_result=tech_res,
        fundamental_result=fund_res,
        smart_money_result=smart_res,
        sentiment_result=sent_res,
        macro_result=macro_res,
        options_result=options_res
    )
    
    return json_report

if __name__ == "__main__":
    run_simulation("AAPL")
