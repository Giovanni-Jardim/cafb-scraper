from __future__ import annotations

from typing import Dict

from status_invest_scraper import Demonstrativo, StatusInvestScraper


class StatusInvestAPI:
    """
    Mantém o nome para compatibilidade, mas agora delega a coleta ao Fundamentus.
    """

    def __init__(self, headless: bool = True, proxy: str | None = None):
        self.scraper = StatusInvestScraper(headless=headless, proxy=proxy)

    def get_historical_data(self, ticker: str, tipo: str = "bp") -> Dict[str, Demonstrativo]:
        raise NotImplementedError(
            "A integração antiga via endpoints do Status Invest foi substituída pelo loader do Fundamentus. "
            "Use get_all_demonstrativos()."
        )

    def get_all_demonstrativos(self, ticker: str) -> Dict[str, Demonstrativo]:
        import asyncio

        return asyncio.run(self.scraper.scrape_ticker(ticker))


async def scrape_hibrido(ticker: str) -> Dict[str, Demonstrativo]:
    api = StatusInvestAPI()
    return api.get_all_demonstrativos(ticker)
