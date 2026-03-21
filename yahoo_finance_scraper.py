from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class Demonstrativo:
    tipo: str  # 'bp', 'dre', 'dfc', 'mercado', 'dividendos' (NOVO)
    ticker: str
    trimestres: List[str]
    contas: Dict[str, List[Optional[float]]]
    unidade: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.contas, index=self.trimestres).T
        df.index.name = "Conta"
        return df


@dataclass
class MarketData:
    """Estrutura auxiliar para dados de mercado"""
    cotacao: Optional[Decimal] = None
    dividend_yield: Optional[float] = None  # %
    dividend_rate: Optional[Decimal] = None  # R$ por ação
    shares_outstanding: Optional[int] = None  # Número total de ações
    eps_ttm: Optional[Decimal] = None  # LPA
    book_value_per_share: Optional[Decimal] = None  # VPA
    market_cap: Optional[Decimal] = None


@dataclass
class DividendoHistorico:
    """Estrutura para histórico de dividendos"""
    data: datetime
    valor: Decimal
    tipo: str  # 'DIVIDENDO', 'JCP', 'OUTRO'


class YahooFinanceScraper:
    """
    Scraper de dados fundamentalistas, de mercado E histórico de dividendos.
    """

    TIPO_MAP = {
        "bp": "quarterly_balance_sheet",
        "dre": "quarterly_income_stmt",
        "dfc": "quarterly_cashflow",
    }

    CONTA_MAP = {
        # BP - Ativo
        "Total Assets": "Ativo Total",
        "Current Assets": "Ativo Circulante",
        "Cash And Cash Equivalents": "Caixa e Equivalentes",
        "Inventory": "Estoques",
        "Property Plant Equipment": "Imobilizado",
        "Total Non Current Assets": "Ativo Não Circulante",
        
        # BP - Passivo
        "Total Liabilities Net Minority Interest": "Passivo Total",
        "Current Liabilities": "Passivo Circulante",
        "Total Non Current Liabilities Net Minority Interest": "Passivo Não Circulante",
        "Total Debt": "Dívida Bruta",
        "Net Debt": "Dívida Líquida",
        
        # BP - PL
        "Stockholders Equity": "Patrimônio Líquido",
        "Common Stock": "Capital Social",
        "Retained Earnings": "Reservas de Lucro",
        
        # DRE
        "Total Revenue": "Receita Líquida",
        "Gross Profit": "Lucro Bruto",
        "Operating Income": "EBIT",
        "EBITDA": "EBITDA",
        "Net Income": "Lucro Líquido",
        "Basic EPS": "LPA",
        "Diluted EPS": "LPA Diluído",
        "Cost Of Revenue": "Custo dos Produtos",
        "Operating Expense": "Despesas Operacionais",
        "Research Development": "Despesas com P&D",
        "Selling General Administrative": "Despesas Administrativas",
        
        # DFC
        "Operating Cash Flow": "FCO",
        "Free Cash Flow": "FCF",
        "Capital Expenditure": "Capex",
        "Investing Cash Flow": "FCI",
        "Financing Cash Flow": "FCF (Financiamento)",
        "Repurchase Of Capital Stock": "Recompra de Ações",
        "Cash Dividends Paid": "Dividendos Pagos",
        "Depreciation And Amortization": "Depreciação e Amortização",
        "Change In Working Capital": "Variação do Capital de Giro",
        
        # Dados de mercado
        "currentPrice": "Cotação Atual",
        "dividendYield": "Dividend Yield",
        "dividendRate": "Dividendo por Ação",
        "sharesOutstanding": "Número de Ações",
        "trailingEps": "LPA (TTM)",
        "bookValue": "VPA",
        "marketCap": "Valor de Mercado",
    }

    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = Path(cache_dir) if cache_dir else Path("data/raw/yahoo")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_ticker(self, ticker: str) -> yf.Ticker:
        """Busca dados do ticker no Yahoo Finance."""
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker_yahoo = f"{ticker}.SA"
        else:
            ticker_yahoo = ticker
            
        try:
            ticker_obj = yf.Ticker(ticker_yahoo)
            info = ticker_obj.info
            if not info or "symbol" not in info:
                raise ValueError(f"Ticker {ticker_yahoo} não encontrado")
            return ticker_obj
        except Exception as e:
            raise ConnectionError(f"Erro Yahoo Finance: {e}")

    def _extract_market_data(self, info: Dict[str, Any]) -> MarketData:
        """
        Extrai dados de mercado do dict info do Yahoo.
        INCLUI: Cotação, DY, Dividendo, Número de Ações, LPA, VPA
        """
        def safe_decimal(val) -> Optional[Decimal]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return Decimal(str(float(val)))
            except:
                return None
        
        def safe_float(val) -> Optional[float]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return float(val)
            except:
                return None
        
        def safe_int(val) -> Optional[int]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return int(float(str(val).replace(',', '')))
            except:
                return None
        
        # Dividend Yield do Yahoo vem como decimal (0.10 = 10%)
        dy = safe_float(info.get('dividendYield'))
        
        return MarketData(
            cotacao=safe_decimal(info.get('currentPrice') or info.get('regularMarketPrice')),
            dividend_yield=dy * 100 if dy else None,  # Converte para %
            dividend_rate=safe_decimal(info.get('dividendRate')),
            shares_outstanding=safe_int(info.get('sharesOutstanding')),  # NÚMERO TOTAL DE AÇÕES
            eps_ttm=safe_decimal(info.get('trailingEps')),
            book_value_per_share=safe_decimal(info.get('bookValue')),
            market_cap=safe_decimal(info.get('marketCap')),
        )

    def _market_data_to_demonstrativo(
        self, 
        market: MarketData, 
        ticker: str
    ) -> Demonstrativo:
        """Converte MarketData para Demonstrativo."""
        hoje = datetime.now()
        tri = f"{((hoje.month - 1) // 3) + 1}T{hoje.year}"
        
        contas = {
            "Cotação Atual": [float(market.cotacao) if market.cotacao else None],
            "Dividend Yield (%)": [market.dividend_yield],
            "Dividendo por Ação": [float(market.dividend_rate) if market.dividend_rate else None],
            "Número de Ações": [float(market.shares_outstanding) if market.shares_outstanding else None],
            "LPA (TTM)": [float(market.eps_ttm) if market.eps_ttm else None],
            "VPA": [float(market.book_value_per_share) if market.book_value_per_share else None],
            "Valor de Mercado": [float(market.market_cap) if market.market_cap else None],
        }
        
        contas_limpo = {k: v for k, v in contas.items() if v[0] is not None}
        
        return Demonstrativo(
            tipo="mercado",
            ticker=ticker.replace(".SA", ""),
            trimestres=[tri],
            contas=contas_limpo,
            unidade="moeda",
            metadata={
                "tipo_dado": "bazin_graham",
                "data_extracao": hoje.isoformat(),
                "fonte": "yahoo_finance_info",
                "calculos_sugeridos": ["preco_teto_bazin", "preco_justo_graham"]
            }
        )

    def _extract_historico_dividendos(
        self, 
        ticker_obj: yf.Ticker, 
        ticker: str,
        anos: int = 5
    ) -> Optional[Demonstrativo]:
        """
        NOVO: Extrai histórico de dividendos dos últimos N anos.
        Retorna Demonstrativo com dados anuais de dividendos.
        """
        try:
            # Busca ações corporativas (dividendos e splits)
            actions = ticker_obj.actions
            
            if actions is None or actions.empty:
                print(f"⚠️ Sem histórico de dividendos para {ticker}")
                return None
            
            # Filtra apenas dividendos (não splits)
            # Colunas típicas: Dividends, Stock Splits
            if 'Dividends' not in actions.columns:
                print(f"⚠️ Coluna 'Dividends' não encontrada para {ticker}")
                return None
            
            divs = actions[actions['Dividends'] > 0]['Dividends']
            
            if divs.empty:
                print(f"⚠️ Nenhum dividendo encontrado para {ticker}")
                return None
            
            # Agrupa por ano
            divs_df = divs.reset_index()
            divs_df.columns = ['data', 'valor']
            divs_df['data'] = pd.to_datetime(divs_df['data'])
            divs_df['ano'] = divs_df['data'].dt.year
            
            # Filtra últimos N anos
            ano_atual = datetime.now().year
            divs_df = divs_df[divs_df['ano'] >= (ano_atual - anos)]
            
            # Agrupa por ano somando dividendos
            anual = divs_df.groupby('ano')['valor'].sum().reset_index()
            
            # Cria estrutura de Demonstrativo
            anos_list = [f"{int(row['ano'])}" for _, row in anual.iterrows()]
            valores_list = [float(row['valor']) for _, row in anual.iterrows()]
            
            # Calcula média, tendência, etc.
            media = sum(valores_list) / len(valores_list) if valores_list else 0
            cagr = 0
            if len(valores_list) >= 2 and valores_list[0] > 0:
                cagr = ((valores_list[-1] / valores_list[0]) ** (1 / (len(valores_list) - 1)) - 1) * 100
            
            contas = {
                "Dividendo Total Anual": valores_list,
                "Media Anual": [media] * len(anos_list),
                "CAGR (%)": [cagr] * len(anos_list),
            }
            
            # Adiciona contagem de pagamentos por ano
            pagamentos_por_ano = divs_df.groupby('ano').size().tolist()
            contas["Quantidade de Pagamentos"] = [float(x) for x in pagamentos_por_ano]
            
            return Demonstrativo(
                tipo="dividendos",
                ticker=ticker.replace(".SA", ""),
                trimestres=anos_list,
                contas=contas,
                unidade="moeda",
                metadata={
                    "tipo_dado": "historico_dividendos",
                    "anos_coletados": len(anos_list),
                    "periodo": f"{anos_list[0]}-{anos_list[-1]}" if anos_list else "",
                    "total_pagamentos": len(divs_df),
                    "fonte": "yahoo_finance_actions"
                }
            )
            
        except Exception as e:
            print(f"❌ Erro ao extrair histórico de dividendos para {ticker}: {e}")
            return None

    def _convert_to_demonstrativo(
        self, 
        df: pd.DataFrame, 
        ticker: str, 
        tipo: str
    ) -> Demonstrativo:
        """Converte DataFrame do Yahoo para formato Demonstrativo."""
        if df is None or df.empty:
            raise ValueError(f"Sem dados para {ticker}/{tipo}")

        df = df.T
        
        trimestres = []
        for idx in df.index:
            if isinstance(idx, pd.Timestamp):
                ano = idx.year
                mes = idx.month
                tri = ((mes - 1) // 3) + 1
                trimestres.append(f"{tri}T{ano}")
            else:
                trimestres.append(str(idx))

        contas: Dict[str, List[Optional[float]]] = {}
        
        for col_original in df.columns:
            col_clean = str(col_original).strip()
            col_padrao = self.CONTA_MAP.get(col_clean, col_clean)
            
            valores = []
            for val in df[col_original].values:
                if pd.isna(val):
                    valores.append(None)
                else:
                    try:
                        valores.append(float(val))
                    except (ValueError, TypeError):
                        valores.append(None)
            
            if any(v is not None for v in valores):
                contas[col_padrao] = valores

        amostra = [abs(v) for valores in contas.values() for v in valores if v is not None]
        unidade = "milhoes"
        
        if amostra:
            mediana = sorted(amostra)[len(amostra) // 2]
            if mediana >= 1_000_000:
                unidade = "bilhoes"
            elif mediana < 1:
                unidade = "unidade"

        return Demonstrativo(
            tipo=tipo,
            ticker=ticker.replace(".SA", ""),
            trimestres=trimestres,
            contas=contas,
            unidade=unidade,
            metadata={"fonte": "yahoo_finance_financials"}
        )

    def _save_raw(self, df: Optional[pd.DataFrame], ticker: str, tipo: str, data: Dict = None) -> None:
        """Salva dados brutos para debug."""
        if df is not None:
            path = self.cache_dir / f"{ticker}_{tipo}.csv"
            df.to_csv(path)
        if data:
            import json
            path = self.cache_dir / f"{ticker}_{tipo}_info.json"
            with open(path, 'w') as f:
                json.dump(data, f, indent=2, default=str)

    async def scrape_ticker(
        self, 
        ticker: str, 
        tipos: List[str] | None = None,
        incluir_mercado: bool = True,
        incluir_dividendos: bool = True,  # NOVO parâmetro
        anos_dividendos: int = 5  # NOVO: quantos anos de histórico
    ) -> Dict[str, Demonstrativo]:
        """
        Extrai demonstrativos, dados de mercado E histórico de dividendos.
        
        Args:
            ticker: Código da ação (PETR4, VALE3)
            tipos: Lista de demonstrativos ['bp', 'dre', 'dfc']
            incluir_mercado: Se True, inclui dados para Bazin/Graham (cotação, número de ações, etc)
            incluir_dividendos: Se True, inclui histórico de dividendos dos últimos N anos
            anos_dividendos: Quantos anos de histórico de dividendos buscar (padrão: 5)
        """
        tipos = tipos or ["bp", "dre", "dfc"]
        resultados: Dict[str, Demonstrativo] = {}

        try:
            ticker_obj = self._fetch_ticker(ticker)
            
            # 1. Dados de mercado (Cotação, Número de Ações, etc)
            if incluir_mercado:
                try:
                    info = ticker_obj.info
                    self._save_raw(None, ticker, "mercado_raw", info)
                    
                    market_data = self._extract_market_data(info)
                    demo_mercado = self._market_data_to_demonstrativo(market_data, ticker)
                    resultados["mercado"] = demo_mercado
                    
                    shares_str = f"{market_data.shares_outstanding:,.0f}" if market_data.shares_outstanding else "N/A"
                    print(f"✅ {ticker.upper()}/MERCADO | Cotação: R$ {market_data.cotacao} | Ações: {shares_str}")
                    
                except Exception as e:
                    print(f"⚠️ Erro dados de mercado para {ticker}: {e}")
            
            # 2. Histórico de dividendos (NOVO)
            if incluir_dividendos:
                try:
                    demo_divs = self._extract_historico_dividendos(ticker_obj, ticker, anos_dividendos)
                    if demo_divs:
                        resultados["dividendos"] = demo_divs
                        total_divs = sum(demo_divs.contas.get("Dividendo Total Anual", [0]))
                        print(f"✅ {ticker.upper()}/DIVIDENDOS | {demo_divs.metadata['anos_coletados']} anos | Total pago: R$ {total_divs:.2f}")
                except Exception as e:
                    print(f"⚠️ Erro histórico de dividendos para {ticker}: {e}")
            
            # 3. Demonstrativos financeiros (BP, DRE, DFC)
            for tipo in tipos:
                try:
                    metodo_nome = self.TIPO_MAP[tipo]
                    df = getattr(ticker_obj, metodo_nome)
                    
                    if callable(df):
                        df = df()
                    
                    if df is None or df.empty:
                        print(f"⚠️ Sem dados {tipo.upper()} para {ticker}")
                        continue

                    self._save_raw(df, ticker, tipo)
                    demo = self._convert_to_demonstrativo(df, ticker, tipo)
                    resultados[tipo] = demo
                    
                    print(
                        f"✅ {ticker.upper()}/{tipo.upper()} "
                        f"({len(demo.contas)} contas x {len(demo.trimestres)} períodos)"
                    )
                    
                except Exception as e:
                    print(f"❌ Erro em {ticker.upper()}/{tipo.upper()}: {e}")
                    
        except Exception as e:
            print(f"❌ Erro ao buscar ticker {ticker}: {e}")

        return resultados
