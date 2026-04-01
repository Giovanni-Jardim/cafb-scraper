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
    dividend_yield: Optional[Decimal] = None
    lpa: Optional[Decimal] = None
    vpa: Optional[Decimal] = None
    shares_outstanding: Optional[int] = None  # Número total de ações
    
    # NOVO: Histórico de dividendos
    historico_dividendos: Optional[pd.DataFrame] = None
    media_dividendos_5anos: Optional[Decimal] = None
    tendencia_dividendos: Optional[str] = None  # 'CRESCENTE', 'ESTAVEL', 'DECRESCENTE'
    
    # Resultados Bazin
    preco_teto_bazin: Optional[Decimal] = None
    preco_teto_bazin_5anos: Optional[Decimal] = None  # Baseado na média de 5 anos
    margem_seguranca_bazin_pct: Optional[float] = None
    recomendacao_bazin: Optional[str] = None
    
    # Resultados Graham
    preco_justo_graham: Optional[Decimal] = None
    margem_seguranca_graham_pct: Optional[float] = None
    recomendacao_graham: Optional[str] = None
    
    # Validação
    dados_completos: bool = False
    tem_historico_dividendos: bool = False


class YahooFinanceAPI:
    """
    API wrapper para Yahoo Finance com cálculos Bazin e Graham + histórico de dividendos.
    """

    YIELD_DESEJADO_BAZIN = Decimal('0.06')  # 6% (conservador para histórico)
    YIELD_DESEJADO_BAZIN_AGRESSIVO = Decimal('0.10')  # 10%
    MULTIPLICADOR_GRAHAM = Decimal('22.5')

    def __init__(self, cache_dir: Optional[str] = None):
        self.scraper = YahooFinanceScraper(cache_dir=cache_dir)

    def get_all_demonstrativos(self, ticker: str, incluir_mercado: bool = True, incluir_dividendos: bool = True) -> Dict[str, Demonstrativo]:
        """Busca demonstrativos, dados de mercado e histórico de dividendos."""
        import asyncio
        return asyncio.run(self.scraper.scrape_ticker(
            ticker, 
            incluir_mercado=incluir_mercado,
            incluir_dividendos=incluir_dividendos
        ))

    def get_historical_data(self, ticker: str, period: str = "1y") -> pd.DataFrame:
        """Dados históricos de preço."""
        import yfinance as yf
        
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker = f"{ticker}.SA"
            
        stock = yf.Ticker(ticker)
        return stock.history(period=period)

    def calcular_bazin_graham(self, ticker: str) -> CalculosBazinGraham:
        """
        Calcula Preço Teto (Bazin) e Preço Justo (Graham) com histórico de dividendos.
        """
        # Busca dados incluindo histórico de dividendos
        dados = self.get_all_demonstrativos(ticker, incluir_mercado=True, incluir_dividendos=True)
        
        # Extrai dados de mercado
        mercado = dados.get('mercado')
        if not mercado:
            return CalculosBazinGraham(ticker=ticker, dados_completos=False)
        
        contas = mercado.contas
        
        # Helper para extrair valor
        def get_val(key: str) -> Optional[float]:
            val = contas.get(key, [None])[0]
            return val if val is not None else None
        
        # Extrai inputs básicos
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
        
        # NOVO: Processa histórico de dividendos
        historico_df = None
        media_5anos = None
        tendencia = None
        tem_historico = False
        
        if 'dividendos' in dados:
            div_demo = dados['dividendos']
            tem_historico = True
            
            # Converte para DataFrame
            hist_data = {
                'Ano': div_demo.trimestres,
                'Dividendo_Total': div_demo.contas.get('Dividendo Total Anual', []),
                'Media': div_demo.contas.get('Media Anual', []),
                'Pagamentos': div_demo.contas.get('Quantidade de Pagamentos', []),
            }
            historico_df = pd.DataFrame(hist_data)
            
            # Calcula média dos últimos 5 anos (ou menos se não tiver)
            valores = hist_data['Dividendo_Total']
            if valores:
                media_5anos = Decimal(str(sum(valores) / len(valores)))
                
                # Analisa tendência (último vs primeiro)
                if len(valores) >= 2:
                    if valores[-1] > valores[0] * 1.1:
                        tendencia = "CRESCENTE 📈"
                    elif valores[-1] < valores[0] * 0.9:
                        tendencia = "DECRESCENTE 📉"
                    else:
                        tendencia = "ESTÁVEL ➡️"
                else:
                    tendencia = "INSUFICIENTE"
        
        # ========== CÁLCULO BAZIN ==========
        preco_teto = None
        preco_teto_5anos = None
        margem_bazin = None
        rec_bazin = None
        
        # Bazin com dividendo atual (TTM)
        if dividendo and dividendo > 0:
            preco_teto = dividendo / self.YIELD_DESEJADO_BAZIN_AGRESSIVO
            
            if cotacao:
                margem_bazin = float((preco_teto / cotacao - 1) * 100)
                if margem_bazin >= 10:
                    rec_bazin = "COMPRA"
                elif margem_bazin <= -10:
                    rec_bazin = "VENDA"
                else:
                    rec_bazin = "AGUARDAR"
        
        # Bazin com média de 5 anos (mais conservador)
        if media_5anos and media_5anos > 0:
            preco_teto_5anos = media_5anos / self.YIELD_DESEJADO_BAZIN  # 6% yield
        
        # ========== CÁLCULO GRAHAM ==========
        preco_justo = None
        margem_graham = None
        rec_graham = None
        
        if lpa and vpa and lpa > 0 and vpa > 0:
            try:
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
            historico_dividendos=historico_df,
            media_dividendos_5anos=media_5anos,
            tendencia_dividendos=tendencia,
            preco_teto_bazin=preco_teto,
            preco_teto_bazin_5anos=preco_teto_5anos,
            margem_seguranca_bazin_pct=margem_bazin,
            recomendacao_bazin=rec_bazin,
            preco_justo_graham=preco_justo,
            margem_seguranca_graham_pct=margem_graham,
            recomendacao_graham=rec_graham,
            dados_completos=all([cotacao, dividendo or (cotacao and dy), lpa, vpa]),
            tem_historico_dividendos=tem_historico
        )

    def gerar_markdown_analise(self, ticker: str) -> str:
        """
        Gera análise em Markdown para NotebookLM com Bazin, Graham e histórico de dividendos.
        """
        calc = self.calcular_bazin_graham(ticker)
        
        if not calc.dados_completos:
            return f"# Análise {ticker}: Dados incompletos\n\nNão foi possível obter dados de mercado suficientes."
        
        # Busca também dados fundamentalistas para contexto
        dados = self.get_all_demonstrativos(ticker, incluir_mercado=False, incluir_dividendos=False)
        
        lines = [
            f"# Análise de Valuation: {calc.ticker}",
            "",
            f"**Data da análise:** {pd.Timestamp.now().strftime('%d/%m/%Y')}**",
            "",
            "## 📊 Resumo Executivo",
            "",
            f"| Métrica | Valor |",
            f"|---------|-------|",
            f"| Cotação Atual | R$ {calc.cotacao:.2f} |",
            f"| Nº Total de Ações | {calc.shares_outstanding:,.0f} |" if calc.shares_outstanding else "| Nº Total de Ações | N/A |",
            f"| Dividendo/Ação (12m) | R$ {calc.dividendo_por_acao:.2f} |" if calc.dividendo_por_acao else "",
            f"| Dividend Yield | {calc.dividend_yield:.2f}% |" if calc.dividend_yield else "",
            f"| LPA (TTM) | R$ {calc.lpa:.2f} |" if calc.lpa else "",
            f"| VPA | R$ {calc.vpa:.2f} |" if calc.vpa else "",
            "",
            "---",
            "",
            "## 💰 Histórico de Dividendos (Últimos 5 Anos)",
            "",
        ]
        
        # Adiciona tabela de histórico se disponível
        if calc.tem_historico_dividendos and calc.historico_dividendos is not None:
            lines.append(calc.historico_dividendos.to_markdown(index=False))
            lines.append("")
            lines.append(f"**Média anual (5 anos):** R$ {calc.media_dividendos_5anos:.2f}")
            lines.append(f"**Tendência:** {calc.tendencia_dividendos}")
            lines.append("")
        else:
            lines.append("*Histórico de dividendos não disponível*")
            lines.append("")
        
        lines.extend([
            "---",
            "",
            "## 🎯 Estratégia Bazin (Preço Teto)",
            "",
            "**Fórmula:** Dividendo por Ação / Yield Desejado",
            "",
            f"### Com Dividendo Atual (TTM) - Yield 10%",
            f"- **Dividendo utilizado:** R$ {calc.dividendo_por_acao:.2f}",
            f"- **Preço Teto calculado:** R$ {calc.preco_teto_bazin:.2f}" if calc.preco_teto_bazin else "",
            f"- **Margem de Segurança:** {calc.margem_seguranca_bazin_pct:+.1f}%" if calc.margem_seguranca_bazin_pct else "",
            f"- **Recomendação:** {calc.recomendacao_bazin}" if calc.recomendacao_bazin else "",
            "",
        ])
        
        # Adiciona cálculo com média de 5 anos se disponível
        if calc.preco_teto_bazin_5anos:
            margem_5anos = float((calc.preco_teto_bazin_5anos / calc.cotacao - 1) * 100) if calc.cotacao else 0
            lines.extend([
                f"### Com Média 5 Anos (Conservador) - Yield 6%",
                f"- **Dividendo médio:** R$ {calc.media_dividendos_5anos:.2f}",
                f"- **Preço Teto calculado:** R$ {calc.preco_teto_bazin_5anos:.2f}",
                f"- **Margem de Segurança:** {margem_5anos:+.1f}%",
                "",
            ])
        
        lines.extend([
            f"**Interpretação:** A cotação atual está {'abaixo' if calc.margem_seguranca_bazin_pct and calc.margem_seguranca_bazin_pct > 0 else 'acima'} do preço teto.",
            "",
            "---",
            "",
            "## 📐 Fórmula de Graham (Preço Justo)",
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
        ])
        
        # Adiciona BP, DRE, DFC se disponíveis
        for tipo, demo in dados.items():
            if tipo in ['mercado', 'dividendos']:
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
            f"- **Bazin (Atual):** {calc.recomendacao_bazin} (Margem: {calc.margem_seguranca_bazin_pct:+.1f}%)" if calc.recomendacao_bazin else "- **Bazin:** Sem dados",
        ])
        
        if calc.preco_teto_bazin_5anos:
            margem_5 = float((calc.preco_teto_bazin_5anos / calc.cotacao - 1) * 100) if calc.cotacao else 0
            rec_5 = "COMPRA" if margem_5 >= 10 else ("VENDA" if margem_5 <= -10 else "AGUARDAR")
            lines.append(f"- **Bazin (Média 5a):** {rec_5} (Margem: {margem_5:+.1f}%)")
        
        lines.extend([
            f"- **Graham:** {calc.recomendacao_graham} (Margem: {calc.margem_seguranca_graham_pct:+.1f}%)",
            "",
            "### Cenários",
            "",
            "1. **Ambos COMPRA:** Forte sinal de oportunidade. Ação pagando bons dividendos (Bazin) e abaixo do valor intrínseco (Graham).",
            "2. **Bazin COMPRA + Graham AGUARDAR:** Ação para renda, mas atenção ao preço.",
            "3. **Bazin VENDA + Graham COMPRA:** Possível armadilha de yield (preço caindo, DY artificialmente alto).",
            "4. **Ambos VENDA:** Ação sobrevalorizada ou em deterioração.",
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
                    'shares_outstanding': calc.shares_outstanding,
                    'dividend_yield': calc.dividend_yield,
                    'dividendo_12m': float(calc.dividendo_por_acao) if calc.dividendo_por_acao else None,
                    'media_dividendos_5a': float(calc.media_dividendos_5anos) if calc.media_dividendos_5anos else None,
                    'tendencia_divs': calc.tendencia_dividendos,
                    'preco_teto_bazin': float(calc.preco_teto_bazin) if calc.preco_teto_bazin else None,
                    'preco_teto_bazin_5a': float(calc.preco_teto_bazin_5anos) if calc.preco_teto_bazin_5anos else None,
                    'margem_bazin_pct': calc.margem_seguranca_bazin_pct,
                    'recomendacao_bazin': calc.recomendacao_bazin,
                    'lpa': float(calc.lpa) if calc.lpa else None,
                    'vpa': float(calc.vpa) if calc.vpa else None,
                    'preco_justo_graham': float(calc.preco_justo_graham) if calc.preco_justo_graham else None,
                    'margem_graham_pct': calc.margem_seguranca_graham_pct,
                    'recomendacao_graham': calc.recomendacao_graham,
                    'dados_completos': calc.dados_completos,
                    'tem_historico_divs': calc.tem_historico_dividendos
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
async def scrape_yahoo(ticker: str, incluir_mercado: bool = True, incluir_dividendos: bool = True) -> Dict[str, Demonstrativo]:
    """Função helper assíncrona."""
    api = YahooFinanceAPI()
    return api.get_all_demonstrativos(ticker, incluir_mercado=incluir_mercado, incluir_dividendos=incluir_dividendos)


def analisar_valuation(ticker: str) -> str:
    """Gera análise Markdown pronta para NotebookLM."""
    api = YahooFinanceAPI()
    return api.gerar_markdown_analise(ticker)


def comparar_tickers(tickers: List[str]) -> pd.DataFrame:
    """Compara múltiplos tickers em DataFrame."""
    api = YahooFinanceAPI()
    return api.batch_analise(tickers)
