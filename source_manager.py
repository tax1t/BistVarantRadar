import pandas as pd
import time
from typing import Optional, List

from providers.base_provider import BaseDataProvider
from providers.yfinance_provider import YFinanceProvider
from providers.finnhub_provider import FinnhubProvider
from validation import DataValidator
from cache import DataCache
from health_monitor import DataHealthMonitor
from audit import Auditor
from governance import DataGovernance

class ValidatedDataResult:
    """Doğrulanmış veri sonucu. Kalite skoru ve kaynak bilgisi içerir."""
    def __init__(self, df: pd.DataFrame, quality_score: int, provider_name: str, 
                 from_cache: bool = False, validation_report: dict = None):
        self.df = df
        self.quality_score = quality_score
        self.provider_name = provider_name
        self.from_cache = from_cache
        self.validation_report = validation_report or {}
