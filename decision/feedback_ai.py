class GenerativeFeedbackEngine:
    """
    Simulates a Generative AI / NLP engine that analyzes multiple data points 
    (LSTM predictions, Technical Indicators, and Historical News Sentiment)
    to produce a human-readable, cohesive strategy report in Turkish.
    """
    def __init__(self):
        pass
        
    def generate_feedback(self, symbol, technicals, predictions, news_sentiment, confidence):
        """
        Generates a paragraph summarizing the AI's analysis.
        """
        # Parse inputs
        rsi = technicals.get('RSI_14', 50)
        ema20 = technicals.get('EMA_20', 0)
        ema50 = technicals.get('EMA_50', 0)
        
        # Determine trend
        trend = "yatay"
        if ema20 > ema50:
            trend = "yükseliş"
        elif ema20 < ema50:
            trend = "düşüş"
            
        # Analyze RSI
        rsi_status = "nötr bölgede"
        if rsi >= 70:
            rsi_status = "aşırı alım bölgesinde (düzeltme riski taşıyor)"
        elif rsi <= 30:
            rsi_status = "aşırı satım bölgesinde (tepki alımı gelebilir)"
            
        # Parse News
        news_desc = "nötr"
        if news_sentiment > 0.3:
            news_desc = "genel olarak pozitif (olumlu)"
        elif news_sentiment < -0.3:
            news_desc = "genel olarak negatif (olumsuz)"
            
        # AI Opinion
        if confidence >= 70:
            verdict = "Güçlü bir fırsat barındırıyor."
        elif confidence >= 50:
            verdict = "İzlenmeye değer bir potansiyel var, ancak risklere dikkat edilmeli."
        else:
            verdict = "Şu aşamada yeni bir pozisyon açmak için riskli görünüyor."
            
        summary = (
            f"VarantRadar Otonom Yapay Zeka Komitesi, {symbol} hissesini detaylı şekilde analiz etti. "
            f"Teknik olarak hisse {trend} trendinde hareket ediyor ve RSI göstergesi şu anda {rsi_status}. "
            f"Derin Öğrenme (LSTM) modelimizin geleceğe yönelik tahminleri %{confidence} güven skoru ile desteklenmektedir. "
            f"Ayrıca geriye dönük 6 aylık haber akışını incelediğimizde piyasa duyarlılığının {news_desc} olduğunu görüyoruz. "
            f"Sonuç olarak; sistemin ortak kararı: {verdict}"
        )
        
        return summary
