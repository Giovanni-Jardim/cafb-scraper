from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, List
import math

import pandas as pd

from yahoo_finance_scraper import Demonstrativo, YahooFinanceScraper, MarketData


@dataclass
class CalculosBazinGraham:
    """Resultados dos cálculos de valuation"""
    ticker: str
    
    # Inputs
    cotacao: Optional[Decimal] = None
    dividendo_por_acao: Optional[Decimal] = None
    dividend_yield: Optional[float] = None
    lpa: Optional[Decimal] = None
    vpa: Optional[Decimal] = None
    shares_outstanding: Optional[int] = None
    
    # Resultados Bazin
    preco_teto_bazin: Optional[Decimal] = None
    margem_seguranca_bazin_pct: Optional[float] = None
    recomendacao_bazin: Optional[str] = None  # 'COMPRA', 'AGUARDAR', 'VENDA'
    
    # Resultados Graham
    preco_justo_graham: Optional[Decimal] = None
    margem_seguranca_graham_pct: Optional[float] = None
    recomendacao_graham: Optional[str] = None
    
    # Validação
    dados_completos: bool = False


class YahooFinanceAPI:
    """
    API wrapper para Yahoo Finance com cálculos Bazin e Graham integrados.
    """

    # Constantes dos métodos
    YIELD_DESEJADO_BAZIN = Decimal('0.10')  # 10%
    MULTIPLICADOR_GRAHAM = Decimal('22.5')

    def __init__(self, cache_dir: Optional[str] = None):
        self.scraper = YahooFinanceScraper(cache_dir=cache_dir)

    def get_all_demonstrativos(self, ticker: str, incluir_mercado: bool = True) -> Dict[str, Demonstrativo]:
        """Busca demonstrativos e dados de mercado."""
        import asyncio
        return asyncio.run(self.scraper.scrape_ticker(ticker, incluir_mercado=incluir_mercado))

    def get_historical_data(self, ticker: str, period: str = "1y") -> pd.DataFrame:
        """Bônus: Dados históricos de preço."""
        import yfinance as yf
        
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker = f"{ticker}.SA"
            
        stock = yf.Ticker(ticker)
        return stock.history(period=period)

    def calcular_bazin_graham(self, ticker: str) -> CalculosBazinGraham:
        """
        Calcula Preço Teto (Bazin) e Preço Justo (Graham) para um ticker.
        Retorna estrutura completa com recomendações.
        """
        # Busca dados
        dados = self.get_all_demonstrativos(ticker, incluir_mercado=True)
        
        # Extrai dados de mercado
        mercado = dados.get('mercado')
        if not mercado:
            return CalculosBazinGraham(ticker=ticker, dados_completos=False)
        
        contas = mercado.contas
        
        # Helper para extrair valor da lista
        def get_val(key: str) -> Optional[float]:
            val = contas.get(key, [None])[0]
            return val if val is not None else None
        
        # Extrai inputs
        cotacao = Decimal(str(get_val("Cotação Atual"))) if get_val("Cotação Atual") else None
        dy = get_val("Dividend Yield (%)")
        div_rate = Decimal(str(get_val("Dividendo por Ação"))) if get_val("Dividendo por Ação") else None
        lpa = Decimal(str(get_val("LPA (TTM)"))) if get_val("LPA (TTM)") else None
        vpa = Decimal(str(get_val("VPA"))) if get_val("VPA") else None
        shares = int(get_val("Número de Ações")) if get_val("Número de Ações") else None
        
        # Se não tem dividendo direto, calcula via cotação * DY
        dividendo = div_rate
        if not dividendo and cotacao and dy:
            dividendo = cotacao * (Decimal(str(dy)) / 100)
        
        # ========== CÁLCULO BAZIN ==========
        preco_teto = None
        margem_bazin = None
        rec_bazin = None
        
        if dividendo and dividendo > 0:
            preco_teto = dividendo / self.YIELD_DESEJADO_BAZIN
            
            if cotacao:
                margem_bazin = float((preco_teto / cotacao - 1) * 100)
                if margem_bazin >= 10:
                    rec_bazin = "COMPRA"
                elif margem_bazin <= -10:
                    rec_bazin = "VENDA"
                else:
                    rec_bazin = "AGUARDAR"
        
        # ========== CÁLCULO GRAHAM ==========
        preco_justo = None
        margem_graham = None
        rec_graham = None
        
        if lpa and vpa and lpa > 0 and vpa > 0:
            try:
                # √(22.5 × LPA × VPA)
                valor_interno = self.MULTIPLICADOR_GRAHAM * lpa * vpa
                preco_justo = Decimal(str(math.sqrt(float(valor_interno))))
                
                if cotacao:
                    margem_graham = float((preco_justo / cotacao - 1) * 100)
                    if margem_graham >= 15:
                        rec_graham = "COMPRA"
                    elif margem_graham <= -15:
                        rec_graham = "VENDA"
                    else:
                        rec_graham = "AGUARDAR"
            except:
                pass
        
        return CalculosBazinGraham(
            ticker=ticker.replace(".SA", ""),
            cotacao=cotacao,
            dividendo_por_acao=dividendo,
            dividend_yield=dy,
            lpa=lpa,
            vpa=vpa,
            shares_outstanding=shares,
            preco_teto_bazin=preco_teto,
            margem_seguranca_bazin_pct=margem_bazin,
            recomendacao_bazin=rec_bazin,
            preco_justo_graham=preco_justo,
            margem_seguranca_graham_pct=margem_graham,
            recomendacao_graham=rec_graham,
            dados_completos=all([cotacao, dividendo or (cotacao and dy), lpa, vpa])
        )

    def gerar_markdown_analise(self, ticker: str) -> str:
        """
        Gera análise em Markdown para NotebookLM com Bazin e Graham.
        """
        calc = self.calcular_bazin_graham(ticker)
        
        if not calc.dados_completos:
            return f"# Análise {ticker}: Dados incompletos\n\nNão foi possível obter dados de mercado suficientes."
        
        # Busca também dados fundamentalistas para contexto
        dados = self.get_all_demonstrativos(ticker, incluir_mercado=False)
        
        lines = [
            f"# Análise de Valuation: {calc.ticker}",
            "",
            f"**Data da análise:** {pd.Timestamp.now().strftime('%d/%m/%Y')}**",
            "",
            "## Resumo Executivo",
            "",
            f"| Métrica | Valor |",
            f"|---------|-------|",
            f"| Cotação Atual | R$ {calc.cotacao:.2f} |",
            f"| Dividendo/Ação (12m) | R$ {calc.dividendo_por_acao:.2f} |" if calc.dividendo_por_acao else "",
            f"| Dividend Yield | {calc.dividend_yield:.2f}% |" if calc.dividend_yield else "",
            f"| LPA (TTM) | R$ {calc.lpa:.2f} |" if calc.lpa else "",
            f"| VPA | R$ {calc.vpa:.2f} |" if calc.vpa else "",
            f"| Nº Ações | {calc.shares_outstanding:,.0f} |" if calc.shares_outstanding else "",
            "",
            "---",
            "",
            "## 🎯 Estratégia Bazin (Preço Teto)",
            "",
            "**Fórmula:** Dividendo por Ação / 10% (yield desejado)",
            "",
            f"- **Dividendo utilizado:** R$ {calc.dividendo_por_acao:.2f}",
            f"- **Preço Teto calculado:** R$ {calc.preco_teto_bazin:.2f}",
            f"- **Margem de Segurança:** {calc.margem_seguranca_bazin_pct:+.1f}%",
            f"- **Recomendação:** {calc.recomendacao_bazin}",
            "",
            f"**Interpretação:** A cotação atual está {'abaixo' if calc.margem_seguranca_bazin_pct and calc.margem_seguranca_bazin_pct > 0 else 'acima'} do preço teto para yield de 10%.",
            "",
            "---",
            "",
            "## 📊 Fórmula de Graham (Preço Justo)",
            "",
            "**Fórmula:** √(22.5 × LPA × VPA)",
            "",
            f"- **LPA (TTM):** R$ {calc.lpa:.2f}",
            f"- **VPA:** R$ {calc.vpa:.2f}",
            f"- **Preço Justo calculado:** R$ {calc.preco_justo_graham:.2f}",
            f"- **Margem de Segurança:** {calc.margem_seguranca_graham_pct:+.1f}%",
            f"- **Recomendação:** {calc.recomendacao_graham}",
            "",
            f"**Interpretação:** Segundo Graham, o valor intrínseco é R$ {calc.preco_justo_graham:.2f}, sugerindo que a ação está {'barata' if calc.margem_seguranca_graham_pct and calc.margem_seguranca_graham_pct > 0 else 'cara'}.",
            "",
            "---",
            "",
            "## 📈 Dados Fundamentalistas (Yahoo Finance)",
            "",
        ]
        
        # Adiciona BP, DRE, DFC se disponíveis
        for tipo, demo in dados.items():
            if tipo == 'mercado':
                continue
                
            lines.extend([
                f"### {tipo.upper()} - Últimos {len(demo.trimestres)} Trimestres",
                "",
                demo.to_dataframe().head(10).to_markdown(),
                "",
            ])
        
        # Conclusão integrada
        lines.extend([
            "---",
            "",
            "## 🎯 Conclusão Integrada",
            "",
            "### Sinais de Compra/Venda",
            "",
            f"- **Bazin:** {calc.recomendacao_bazin} (Margem: {calc.margem_seguranca_bazin_pct:+.1f}%)",
            f"- **Graham:** {calc.recomendacao_graham} (Margem: {calc.margem_seguranca_graham_pct:+.1f}%)",
            "",
            "### Cenários",
            "",
            f"1. **Ambos COMPRA:** Forte sinal de oportunidade. Ação pagando bons dividendos (Bazin) e abaixo do valor intrínseco (Graham).",
            f"2. **Bazin COMPRA + Graham AGUARDAR:** Ação para renda, mas atenção ao preço.",
            f"3. **Bazin VENDA + Graham COMPRA:** Possível armadilha de yield (preço caindo, DY artificialmente alto).",
            f"4. **Ambos VENDA:** Ação sobrevalorizada ou em deterioração.",
            "",
            "**Disclaimer:** Esta análise é baseada em dados históricos e não constitui recomendação de investimento.",
        ])
        
        return "\n".join(lines)

    def batch_analise(self, tickers: List[str]) -> pd.DataFrame:
        """
        Análise em lote de múltiplos tickers. Retorna DataFrame comparativo.
        """
        resultados = []
        
        for ticker in tickers:
            try:
                calc = self.calcular_bazin_graham(ticker)
                resultados.append({
                    'ticker': calc.ticker,
                    'cotacao': float(calc.cotacao) if calc.cotacao else None,
                    'dividend_yield': calc.dividend_yield,
                    'preco_teto_bazin': float(calc.preco_teto_bazin) if calc.preco_teto_bazin else None,
                    'margem_bazin_pct': calc.margem_seguranca_bazin_pct,
                    'recomendacao_bazin': calc.recomendacao_bazin,
                    'lpa': float(calc.lpa) if calc.lpa else None,
                    'vpa': float(calc.vpa) if calc.vpa else None,
                    'preco_justo_graham': float(calc.preco_justo_graham) if calc.preco_justo_graham else None,
                    'margem_graham_pct': calc.margem_seguranca_graham_pct,
                    'recomendacao_graham': calc.recomendacao_graham,
                    'dados_completos': calc.dados_completos
                })
            except Exception as e:
                print(f"❌ Erro em {ticker}: {e}")
                resultados.append({
                    'ticker': ticker,
                    'erro': str(e),
                    'dados_completos': False
                })
        
        return pd.DataFrame(resultados)


# Funções helper mantidas para compatibilidade
async def scrape_yahoo(ticker: str, incluir_mercado: bool = True) -> Dict[str, Demonstrativo]:
    """Função helper assíncrona."""
    api = YahooFinanceAPI()
    return api.get_all_demonstrativos(ticker, incluir_mercado=incluir_mercado)


def analisar_valuation(ticker: str) -> str:
    """Gera análise Markdown pronta para NotebookLM."""
    api = YahooFinanceAPI()
    return api.gerar_markdown_analise(ticker)


def comparar_tickers(tickers: List[str]) -> pd.DataFrame:
    """Compara múltiplos tickers em DataFrame."""
    api = YahooFinanceAPI()
    return api.batch_analise(tickers)
