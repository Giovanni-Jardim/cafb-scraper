from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from yahoo_finance_scraper import Demonstrativo, YahooFinanceScraper


class YahooFinanceAPI:
    """
    API wrapper para Yahoo Finance.
    Mantém compatibilidade com interface antiga.
    """

    def __init__(self, cache_dir: Optional[str] = None):
        self.scraper = YahooFinanceScraper(cache_dir=cache_dir)

    def get_all_demonstrativos(self, ticker: str) -> Dict[str, Demonstrativo]:
        import asyncio
        return asyncio.run(self.scraper.scrape_ticker(ticker))

    def get_historical_data(self, ticker: str, period: str = "1y") -> pd.DataFrame:
        """
        Bônus: Dados históricos de preço (não disponível no Fundamentus).
        """
        import yfinance as yf
        
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker = f"{ticker}.SA"
            
        stock = yf.Ticker(ticker)
        return stock.history(period=period)


async def scrape_yahoo(ticker: str) -> Dict[str, Demonstrativo]:
    """Função helper"""
    api = YahooFinanceAPI()
    return api.get_all_demonstrativos(ticker)