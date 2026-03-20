from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class Demonstrativo:
    tipo: str  # 'bp', 'dre', 'dfc', 'mercado' (NOVO)
    ticker: str
    trimestres: List[str]
    contas: Dict[str, List[Optional[float]]]
    unidade: str
    # NOVO: Metadados extras para dados de mercado
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.contas, index=self.trimestres).T
        df.index.name = "Conta"
        return df


@dataclass
class MarketData:
    """Estrutura auxiliar para dados de mercado (não salva no Demonstrativo diretamente)"""
    cotacao: Optional[Decimal] = None
    dividend_yield: Optional[float] = None  # %
    dividend_rate: Optional[Decimal] = None  # R$ por ação
    shares_outstanding: Optional[int] = None
    eps_ttm: Optional[Decimal] = None  # LPA
    book_value_per_share: Optional[Decimal] = None  # VPA
    market_cap: Optional[Decimal] = None


class YahooFinanceScraper:
    """
    Scraper de dados fundamentalistas E de mercado via Yahoo Finance.
    Agora inclui dados para cálculos Bazin (Preço Teto) e Graham (Preço Justo).
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
        
        # NOVOS: Dados de mercado mapeados como "contas" para padronização
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
        Usado para cálculos Bazin e Graham.
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
            shares_outstanding=safe_int(info.get('sharesOutstanding')),
            eps_ttm=safe_decimal(info.get('trailingEps')),
            book_value_per_share=safe_decimal(info.get('bookValue')),
            market_cap=safe_decimal(info.get('marketCap')),
        )

    def _market_data_to_demonstrativo(
        self, 
        market: MarketData, 
        ticker: str
    ) -> Demonstrativo:
        """
        Converte MarketData para Demonstrativo (formato padronizado).
        Isso permite que dados de mercado fluam pelo mesmo pipeline dos fundamentalistas.
        """
        # Data atual como "trimestre"
        from datetime import datetime
        hoje = datetime.now()
        tri = f"{((hoje.month - 1) // 3) + 1}T{hoje.year}"
        
        # Monta contas com valores (ou None se não disponível)
        contas = {
            "Cotação Atual": [float(market.cotacao) if market.cotacao else None],
            "Dividend Yield (%)": [market.dividend_yield],
            "Dividendo por Ação": [float(market.dividend_rate) if market.dividend_rate else None],
            "Número de Ações": [float(market.shares_outstanding) if market.shares_outstanding else None],
            "LPA (TTM)": [float(market.eps_ttm) if market.eps_ttm else None],
            "VPA": [float(market.book_value_per_share) if market.book_value_per_share else None],
            "Valor de Mercado": [float(market.market_cap) if market.market_cap else None],
        }
        
        # Remove Nones para limpeza
        contas_limpo = {k: v for k, v in contas.items() if v[0] is not None}
        
        return Demonstrativo(
            tipo="mercado",  # NOVO tipo
            ticker=ticker.replace(".SA", ""),
            trimestres=[tri],
            contas=contas_limpo,
            unidade="moeda",  # Valores absolutos, não em milhões
            metadata={
                "tipo_dado": "bazin_graham",
                "data_extracao": hoje.isoformat(),
                "fonte": "yahoo_finance_info",
                "calculos_sugeridos": ["preco_teto_bazin", "preco_justo_graham"]
            }
        )

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
        """Salva dados brutos para debug (DataFrame ou dict)."""
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
        incluir_mercado: bool = True  # NOVO parâmetro
    ) -> Dict[str, Demonstrativo]:
        """
        Extrai demonstrativos E dados de mercado do Yahoo Finance.
        
        Args:
            ticker: Código da ação (PETR4, VALE3)
            tipos: Lista de demonstrativos ['bp', 'dre', 'dfc']
            incluir_mercado: Se True, inclui dados para Bazin/Graham
        """
        tipos = tipos or ["bp", "dre", "dfc"]
        resultados: Dict[str, Demonstrativo] = {}

        try:
            ticker_obj = self._fetch_ticker(ticker)
            
            # NOVO: Extrai dados de mercado primeiro (info é mais rápido)
            if incluir_mercado:
                try:
                    info = ticker_obj.info
                    self._save_raw(None, ticker, "mercado_raw", info)
                    
                    market_data = self._extract_market_data(info)
                    demo_mercado = self._market_data_to_demonstrativo(market_data, ticker)
                    resultados["mercado"] = demo_mercado
                    
                    print(f"✅ Yahoo Finance: {ticker.upper()}/MERCADO "
                          f"(Cotação: R$ {market_data.cotacao}, "
                          f"DY: {market_data.dividend_yield:.2f}%)" if market_data.dividend_yield else "")
                    
                except Exception as e:
                    print(f"⚠️ Erro ao extrair dados de mercado para {ticker}: {e}")
            
            # Extrai demonstrativos financeiros (BP, DRE, DFC)
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
                        f"✅ Yahoo Finance: {ticker.upper()}/{tipo.upper()} "
                        f"({len(demo.contas)} contas x {len(demo.trimestres)} períodos)"
                    )
                    
                except Exception as e:
                    print(f"❌ Erro em {ticker.upper()}/{tipo.upper()}: {e}")
                    
        except Exception as e:
            print(f"❌ Erro ao buscar ticker {ticker}: {e}")

        return resultados
