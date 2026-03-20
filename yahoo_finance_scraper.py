from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class Demonstrativo:
    tipo: str  # 'bp', 'dre', 'dfc'
    ticker: str
    trimestres: List[str]
    contas: Dict[str, List[Optional[float]]]
    unidade: str  # Yahoo retorna em milhões/milhares nativamente

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.contas, index=self.trimestres).T
        df.index.name = "Conta"
        return df


class YahooFinanceScraper:
    """
    Scraper de dados fundamentalistas via Yahoo Finance.
    Mapeia BP, DRE e DFC do Yahoo para o formato do projeto.
    """

    # Mapeamento de tipos para métodos do yfinance
    TIPO_MAP = {
        "bp": "quarterly_balance_sheet",      # Balance Sheet = BP
        "dre": "quarterly_income_stmt",       # Income Statement = DRE  
        "dfc": "quarterly_cashflow",          # Cashflow = DFC
    }

    # Mapeamento de nomes de contas do Yahoo para padrão brasileiro
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
    }

    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = Path(cache_dir) if cache_dir else Path("data/raw/yahoo")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_ticker(self, ticker: str) -> yf.Ticker:
        """
        Busca dados do ticker no Yahoo Finance.
        Converte tickers brasileiros (PETR4 -> PETR4.SA)
        """
        # Adiciona .SA para tickers brasileiros (B3)
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker_yahoo = f"{ticker}.SA"
        else:
            ticker_yahoo = ticker
            
        try:
            ticker_obj = yf.Ticker(ticker_yahoo)
            # Testa se ticker existe
            info = ticker_obj.info
            if not info or "symbol" not in info:
                raise ValueError(f"Ticker {ticker_yahoo} não encontrado no Yahoo Finance")
            return ticker_obj
        except Exception as e:
            raise ConnectionError(f"Erro ao conectar Yahoo Finance: {e}")

    def _convert_to_demonstrativo(
        self, 
        df: pd.DataFrame, 
        ticker: str, 
        tipo: str
    ) -> Demonstrativo:
        """
        Converte DataFrame do Yahoo para formato Demonstrativo.
        """
        if df is None or df.empty:
            raise ValueError(f"Sem dados para {ticker}/{tipo}")

        # Transposta: períodos como colunas -> linhas
        df = df.T
        
        # Converte índice para string de trimestre
        trimestres = []
        for idx in df.index:
            if isinstance(idx, pd.Timestamp):
                # Formato: 1T2024, 2T2024, etc.
                ano = idx.year
                mes = idx.month
                tri = ((mes - 1) // 3) + 1
                trimestres.append(f"{tri}T{ano}")
            else:
                trimestres.append(str(idx))

        # Mapeia nomes das colunas (contas)
        contas: Dict[str, List[Optional[float]]] = {}
        
        for col_original in df.columns:
            # Limpa nome
            col_clean = str(col_original).strip()
            
            # Busca mapeamento ou usa nome original
            col_padrao = self.CONTA_MAP.get(col_clean, col_clean)
            
            # Converte valores
            valores = []
            for val in df[col_original].values:
                if pd.isna(val):
                    valores.append(None)
                else:
                    # Yahoo retorna em milhões, converter para float
                    try:
                        valores.append(float(val))
                    except (ValueError, TypeError):
                        valores.append(None)
            
            # Só adiciona se tiver algum valor não-nulo
            if any(v is not None for v in valores):
                contas[col_padrao] = valores

        # Detecta unidade baseada na ordem de grandeza
        amostra = [abs(v) for valores in contas.values() for v in valores if v is not None]
        unidade = "milhoes"  # Yahoo geralmente retorna em milhões
        
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
        )

    def _save_raw(self, df: pd.DataFrame, ticker: str, tipo: str) -> None:
        """Salva dados brutos para debug"""
        path = self.cache_dir / f"{ticker}_{tipo}.csv"
        df.to_csv(path)

    async def scrape_ticker(self, ticker: str, tipos: List[str] | None = None) -> Dict[str, Demonstrativo]:
        """
        Extrai demonstrativos do Yahoo Finance.
        """
        tipos = tipos or ["bp", "dre", "dfc"]
        resultados: Dict[str, Demonstrativo] = {}

        try:
            ticker_obj = self._fetch_ticker(ticker)
            
            for tipo in tipos:
                try:
                    # Obtém método correto (balance_sheet, income_stmt, cashflow)
                    metodo_nome = self.TIPO_MAP[tipo]
                    metodo = getattr(ticker_obj, metodo_nome)
                    df = metodo()
                    
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