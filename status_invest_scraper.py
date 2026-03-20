# status_invest_scraper.py
# Fase 2: Extração completa de demonstrativos históricos

import asyncio
import json
import re
import time
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import stealth_async
from tenacity import retry, stop_after_attempt, wait_exponential
import fake_useragent


@dataclass
class Demonstrativo:
    tipo: str  # 'bp', 'dre', 'dfc'
    ticker: str
    trimestres: List[str]  # ['4T2023', '3T2023', ...]
    contas: Dict[str, List[float]]  # {'Ativo Total': [1000.5, 950.2, ...]}
    unidade: str  # 'milhares', 'milhoes', 'bilhoes'

    def to_dataframe(self) -> pd.DataFrame:
        """Converte para DataFrame com trimestres como colunas"""
        df = pd.DataFrame(self.contas, index=self.trimestres).T
        df.index.name = "Conta"
        return df


class StatusInvestScraper:
    def __init__(self, headless: bool = True, proxy: Optional[str] = None):
        self.headless = headless
        self.proxy = proxy
        self.ua = fake_useragent.UserAgent()
        self.base_url = "https://statusinvest.com.br/acoes"
        self.data_dir = Path("data/raw/status_invest")
        self.data_dir.mkdir(parents=True, exist_ok=True)

    async def _create_context(self, playwright) -> BrowserContext:
        """Cria contexto com stealth máximo"""
        browser = await playwright.chromium.launch(
            headless=self.headless,
            proxy={"server": self.proxy} if self.proxy else None
        )

        context = await browser.new_context(
            user_agent=self.ua.random,
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            permissions=["geolocation"],
            color_scheme="light",
        )

        # Anti-detecção adicional
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            window.chrome = { runtime: {} };
        """)

        return browser, context

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _get_page_content(self, page: Page, ticker: str, tipo: str) -> str:
        """Navega para a página e extrai dados brutos"""
        url = f"{self.base_url}/{ticker}"

        print(f"🌐 Acessando {ticker} - {tipo.upper()}...")

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Aguarda carregamento inicial
        await page.wait_for_selector("h1.title-ticker", timeout=20000)

        # Clica na aba de demonstrativos financeiros
        try:
            await page.click('a[href="#financial-section"]', timeout=5000)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"⚠️ Não conseguiu clicar na seção financeira de {ticker}: {e}")
            html_debug = await page.content()
            self._save_raw(html_debug, ticker, f"{tipo}_erro")
            raise

        # Seleciona o tipo de demonstrativo (BP, DRE, DFC)
        selector_map = {
            "bp": 'button[data-tab="balance-sheet"]',
            "dre": 'button[data-tab="income-statement"]',
            "dfc": 'button[data-tab="cash-flow"]'
        }

        await page.click(selector_map[tipo])
        await asyncio.sleep(2)  # Aguarda renderização do grid

        # Verifica se há dados históricos disponíveis
        try:
            await page.wait_for_selector("table", timeout=15000)
        except Exception as e:
            print(f"⚠️ Tabela não encontrada para {ticker}/{tipo}, tentando alternativa... {e}")

            try:
                await page.click('button:has-text("Ver mais")', timeout=5000)
                await asyncio.sleep(2)
                await page.wait_for_selector("table", timeout=10000)
            except Exception as e2:
                print(f"❌ Falha no fallback para {ticker}/{tipo}: {e2}")
                html_debug = await page.content()
                self._save_raw(html_debug, ticker, f"{tipo}_erro")
                raise

        # Extrai o HTML da tabela de dados históricos
        content = await page.content()

        # Delay humano entre requests
        await asyncio.sleep(random.uniform(2, 5))

        return content

    def _parse_demonstrativo(self, html: str, ticker: str, tipo: str) -> Demonstrativo:
        """Parseia HTML e extrai estrutura de dados"""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Encontra a tabela de dados históricos
        table = soup.find("table", {"class": re.compile("data-table|financial-table")})

        if not table:
            raise ValueError(f"Tabela não encontrada para {ticker}/{tipo}")

        # Extrai headers (trimestres)
        headers = []
        header_row = table.find("thead")
        if header_row:
            ths = header_row.find_all("th")
            headers = [th.get_text(strip=True) for th in ths[1:]]

        # Extrai linhas de dados
        contas = {}
        rows = table.find("tbody").find_all("tr") if table.find("tbody") else []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            conta_nome = cols[0].get_text(strip=True)
            valores = []

            for col in cols[1:]:
                texto = col.get_text(strip=True)
                valor = self._parse_valor(texto)
                valores.append(valor)

            if conta_nome and any(v is not None for v in valores):
                contas[conta_nome] = valores

        unidade = self._detectar_unidade(html, contas)

        return Demonstrativo(
            tipo=tipo,
            ticker=ticker,
            trimestres=headers,
            contas=contas,
            unidade=unidade
        )

    def _parse_valor(self, texto: str) -> Optional[float]:
        """Converte string brasileira de valor para float"""
        if not texto or texto in ["-", "—", "", "ND"]:
            return None

        texto = texto.strip().upper()

        multiplicador = 1
        if "BI" in texto or "BILH" in texto:
            multiplicador = 1_000_000_000
            texto = texto.replace("BI", "").replace("BILHÕES", "").replace("BILHAO", "")
        elif "MI" in texto or "MILH" in texto:
            multiplicador = 1_000_000
            texto = texto.replace("MI", "").replace("MILHÕES", "").replace("MILHAO", "")
        elif "MIL" in texto:
            multiplicador = 1_000
            texto = texto.replace("MIL", "")

        texto = re.sub(r"[R$%\s]", "", texto)

        try:
            if "," in texto and "." in texto:
                texto = texto.replace(".", "").replace(",", ".")
            elif "," in texto:
                texto = texto.replace(",", ".")

            return float(texto) * multiplicador
        except ValueError:
            return None

    def _detectar_unidade(self, html: str, contas: Dict) -> str:
        """Detecta se valores estão em milhares, milhões ou bilhões"""
        if "milhão" in html.lower() or "mi " in html.lower():
            return "milhoes"
        elif "bilhão" in html.lower() or "bi " in html.lower():
            return "bilhoes"
        elif "mil" in html.lower():
            return "milhares"

        amostra = []
        for valores in contas.values():
            amostra.extend([v for v in valores if v is not None])

        if amostra:
            media = sum(amostra) / len(amostra)
            if media > 1_000_000_000:
                return "bilhoes"
            elif media > 1_000_000:
                return "milhoes"

        return "milhares"

    async def scrape_ticker(self, ticker: str, tipos: List[str] = None) -> Dict[str, Demonstrativo]:
        """Extrai todos os demonstrativos para um ticker"""
        tipos = tipos or ["bp", "dre", "dfc"]
        resultados = {}

        async with async_playwright() as playwright:
            browser, context = await self._create_context(playwright)
            page = await context.new_page()
            await stealth_async(page)

            try:
                for tipo in tipos:
                    try:
                        html = await self._get_page_content(page, ticker, tipo)
                        demo = self._parse_demonstrativo(html, ticker, tipo)
                        resultados[tipo] = demo

                        # Salva raw para debug
                        self._save_raw(html, ticker, tipo)

                        print(f"✅ {ticker}/{tipo.upper()}: {len(demo.contas)} contas x {len(demo.trimestres)} trimestres")

                    except Exception as e:
                        print(f"❌ Erro em {ticker}/{tipo}: {e}")
                        try:
                            html_debug = await page.content()
                            self._save_raw(html_debug, ticker, f"{tipo}_erro")
                        except Exception:
                            pass
                        continue

            finally:
                await context.close()
                await browser.close()

        return resultados

    def _save_raw(self, html: str, ticker: str, tipo: str):
        """Salva HTML bruto para análise de falhas"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.data_dir / f"{ticker}_{tipo}_{timestamp}.html"
        filename.write_text(html, encoding="utf-8")

    def save_to_csv(self, resultados: Dict[str, Demonstrativo], ticker: str):
        """Exporta para CSV estruturado"""
        output_dir = Path("data/processed/status_invest")
        output_dir.mkdir(parents=True, exist_ok=True)

        for tipo, demo in resultados.items():
            df = demo.to_dataframe()

            df["ticker"] = ticker
            df["tipo"] = tipo
            df["unidade"] = demo.unidade
            df["data_extracao"] = datetime.now().isoformat()

            cols = ["ticker", "tipo", "unidade", "data_extracao"] + [
                c for c in df.columns if c not in ["ticker", "tipo", "unidade", "data_extracao"]
            ]
            df = df[cols]

            filename = output_dir / f"{ticker}_{tipo}_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename)
            print(f"💾 Salvo: {filename}")


# ============================================================
# EXECUÇÃO E ORQUESTRAÇÃO
# ============================================================

async def main():
    """Exemplo de uso para múltiplos tickers"""

    tickers = ["PETR4", "VALE3", "WEGE3", "ITUB4", "BBAS3"]
    scraper = StatusInvestScraper(headless=True)

    for ticker in tickers:
        print(f"\n{'='*50}")
        print(f"🔍 Processando {ticker}")
        print(f"{'='*50}")

        try:
            resultados = await scraper.scrape_ticker(ticker)
            scraper.save_to_csv(resultados, ticker)

            for tipo, demo in resultados.items():
                df = demo.to_dataframe()
                print(f"\n📊 {tipo.upper()} - Primeiras 5 contas:")
                print(df.head())

        except Exception as e:
            print(f"💥 Falha crítica em {ticker}: {e}")
            continue

        await asyncio.sleep(random.uniform(5, 10))

    print("\n✨ Scraping concluído!")


if __name__ == "__main__":
    asyncio.run(main())
