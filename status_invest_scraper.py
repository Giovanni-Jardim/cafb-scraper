from __future__ import annotations

import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class Demonstrativo:
    tipo: str  # 'bp', 'dre', 'dfc'
    ticker: str
    trimestres: List[str]
    contas: Dict[str, List[Optional[float]]]
    unidade: str  # 'milhares', 'milhoes', 'bilhoes'

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.contas, index=self.trimestres).T
        df.index.name = "Conta"
        return df


class StatusInvestScraper:
    """
    Mantém o mesmo nome de classe para evitar quebrar o restante do projeto,
    mas agora usa o Fundamentus como fonte de dados.
    """

    URLS = {
        "bp": "https://www.fundamentus.com.br/balancos.php",
        "dre": "https://www.fundamentus.com.br/balancos.php",
        "dfc": "https://www.fundamentus.com.br/balancos.php",
    }

    TIPO_PARAM = {
        "bp": 1,
        "dre": 2,
        "dfc": 3,
    }

    KEYWORDS = {
        "bp": ["ativo", "passivo", "patrim", "caixa", "estoque", "imobilizado"],
        "dre": ["receita", "lucro", "ebit", "resultado", "bruto", "despesa"],
        "dfc": ["caixa", "operac", "invest", "financ", "deprecia", "amort"],
    }

    def __init__(self, headless: bool = True, proxy: Optional[str] = None):
        self.headless = headless
        self.proxy = proxy
        self.data_dir = Path("data/raw/fundamentus")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://www.fundamentus.com.br/",
            }
        )
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _get_page_content(self, ticker: str, tipo: str) -> str:
        url = self.URLS[tipo]
        params = {"papel": ticker.upper(), "tipo": self.TIPO_PARAM[tipo]}
        response = self.session.get(url, params=params, timeout=45)
        response.raise_for_status()

        html = response.text
        if "fundamentus" not in html.lower() and "balan" not in html.lower():
            raise ValueError(f"Resposta inesperada para {ticker}/{tipo}")

        return html

    def _parse_demonstrativo(self, html: str, ticker: str, tipo: str) -> Demonstrativo:
        tables = self._extract_tables(html)
        if not tables:
            raise ValueError(f"Nenhuma tabela encontrada para {ticker}/{tipo}")

        table = self._select_best_table(tables, tipo)
        if table is None:
            raise ValueError(f"Não foi possível localizar a tabela principal para {ticker}/{tipo}")

        demo = self._table_to_demonstrativo(table, ticker=ticker.upper(), tipo=tipo)
        demo.unidade = self._detectar_unidade(html, demo.contas)
        return demo

    def _extract_tables(self, html: str) -> List[pd.DataFrame]:
        dataframes: List[pd.DataFrame] = []

        try:
            for df in pd.read_html(StringIO(html), decimal=",", thousands="."):
                clean = self._clean_dataframe(df)
                if clean is not None and not clean.empty:
                    dataframes.append(clean)
        except ValueError:
            pass

        if dataframes:
            return dataframes

        # Fallback manual se o pandas não conseguir ler corretamente
        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            try:
                df_list = pd.read_html(StringIO(str(table)), decimal=",", thousands=".")
            except ValueError:
                continue
            for df in df_list:
                clean = self._clean_dataframe(df)
                if clean is not None and not clean.empty:
                    dataframes.append(clean)

        return dataframes

    def _clean_dataframe(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(part).strip() for part in col if str(part).strip() and str(part) != "nan").strip()
                for col in df.columns
            ]
        else:
            df.columns = [str(c).strip() for c in df.columns]

        df = df.copy()
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith("unnamed") or len(df.columns) <= 2]]
        df = df.reset_index(drop=True)
        return df

    def _select_best_table(self, tables: List[pd.DataFrame], tipo: str) -> Optional[pd.DataFrame]:
        best_table = None
        best_score = -1
        keywords = self.KEYWORDS[tipo]

        for df in tables:
            flat_text = " ".join(df.astype(str).fillna("").values.flatten()).lower()
            keyword_score = sum(1 for kw in keywords if kw in flat_text)
            structure_score = min(df.shape[0], 20) + min(df.shape[1], 10)
            score = keyword_score * 100 + structure_score

            if score > best_score:
                best_score = score
                best_table = df

        return best_table

    def _table_to_demonstrativo(self, df: pd.DataFrame, ticker: str, tipo: str) -> Demonstrativo:
        # Cenário A: colunas = períodos / linhas = contas
        periods_from_columns = self._extract_periods_from_iterable(df.columns[1:])
        if periods_from_columns and len(df.columns) >= 3:
            period_columns = [col for col in df.columns[1:] if self._is_period_label(col)]
            if period_columns:
                contas: Dict[str, List[Optional[float]]] = {}
                for _, row in df.iterrows():
                    conta = str(row.iloc[0]).strip()
                    if not conta or conta.lower() == "nan":
                        continue
                    valores = [self._parse_valor(row[col]) for col in period_columns]
                    if any(v is not None for v in valores):
                        contas[conta] = valores

                if contas:
                    return Demonstrativo(
                        tipo=tipo,
                        ticker=ticker,
                        trimestres=period_columns,
                        contas=contas,
                        unidade="milhoes",
                    )

        # Cenário B: linhas = períodos / colunas = contas
        period_col = self._find_period_column(df)
        if period_col is not None:
            trimestres = [self._normalize_period_label(v) for v in df[period_col].tolist()]
            account_cols = [c for c in df.columns if c != period_col]
            contas = {}
            for col in account_cols:
                valores = [self._parse_valor(v) for v in df[col].tolist()]
                if any(v is not None for v in valores):
                    contas[str(col).strip()] = valores

            if contas:
                return Demonstrativo(
                    tipo=tipo,
                    ticker=ticker,
                    trimestres=trimestres,
                    contas=contas,
                    unidade="milhoes",
                )

        # Cenário C: tenta usar a primeira coluna como contas e o restante como períodos, mesmo sem detectar label padrão
        if df.shape[1] >= 3:
            trimestres = [self._normalize_period_label(col) for col in df.columns[1:]]
            contas = {}
            for _, row in df.iterrows():
                conta = str(row.iloc[0]).strip()
                if not conta or conta.lower() == "nan":
                    continue
                valores = [self._parse_valor(v) for v in row.iloc[1:].tolist()]
                if any(v is not None for v in valores):
                    contas[conta] = valores

            if contas:
                return Demonstrativo(
                    tipo=tipo,
                    ticker=ticker,
                    trimestres=trimestres,
                    contas=contas,
                    unidade="milhoes",
                )

        raise ValueError("Estrutura de tabela não reconhecida")

    def _find_period_column(self, df: pd.DataFrame) -> Optional[str]:
        for col in df.columns:
            values = df[col].astype(str).tolist()
            matches = sum(1 for v in values if self._is_period_label(v))
            if matches >= max(2, len(values) // 3):
                return col
        return None

    def _extract_periods_from_iterable(self, values) -> List[str]:
        periods = []
        for value in values:
            if self._is_period_label(value):
                periods.append(self._normalize_period_label(value))
        return periods

    def _is_period_label(self, value) -> bool:
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return False

        patterns = [
            r"^\dT\d{4}$",
            r"^\d{2}/\d{4}$",
            r"^\d{4}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{4}-\d{2}-\d{2}$",
        ]
        return any(re.match(p, text) for p in patterns)

    def _normalize_period_label(self, value) -> str:
        text = str(value).strip()
        m = re.match(r"^(\d{2})/(\d{4})$", text)
        if m:
            mes = int(m.group(1))
            ano = m.group(2)
            tri_map = {3: "1T", 6: "2T", 9: "3T", 12: "4T"}
            return f"{tri_map.get(mes, f'{mes:02d}M')}{ano}"
        return text

    def _parse_valor(self, valor) -> Optional[float]:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return None
        if isinstance(valor, (int, float)):
            return float(valor)

        texto = str(valor).strip()
        if not texto or texto.lower() in {"nan", "-", "—", "nd", "n/d"}:
            return None

        negativo = False
        if texto.startswith("(") and texto.endswith(")"):
            negativo = True
            texto = texto[1:-1]

        texto = texto.replace("R$", "").replace("%", "").replace(" ", "")
        texto = texto.replace(".", "").replace(",", ".")
        texto = re.sub(r"[^0-9\-.]", "", texto)

        if texto in {"", ".", "-", "-."}:
            return None

        try:
            valor_float = float(texto)
            return -valor_float if negativo else valor_float
        except ValueError:
            return None

    def _detectar_unidade(self, html: str, contas: Dict[str, List[Optional[float]]]) -> str:
        html_lower = html.lower()
        if "milhões" in html_lower or "milhoes" in html_lower or " em milhões" in html_lower:
            return "milhoes"
        if "bilhões" in html_lower or "bilhoes" in html_lower or " em bilhões" in html_lower:
            return "bilhoes"
        if "milhares" in html_lower or " em milhares" in html_lower:
            return "milhares"

        amostra = [abs(v) for valores in contas.values() for v in valores if v is not None]
        if not amostra:
            return "milhoes"

        mediana = sorted(amostra)[len(amostra) // 2]
        if mediana >= 1_000_000_000:
            return "bilhoes"
        if mediana >= 1_000_000:
            return "milhoes"
        return "milhares"

    def _save_raw(self, html: str, ticker: str, tipo: str) -> None:
        path = self.data_dir / f"{ticker.upper()}_{tipo}.html"
        path.write_text(html, encoding="utf-8")

    async def scrape_ticker(self, ticker: str, tipos: List[str] | None = None) -> Dict[str, Demonstrativo]:
        tipos = tipos or ["bp", "dre", "dfc"]
        resultados: Dict[str, Demonstrativo] = {}

        for tipo in tipos:
            try:
                html = self._get_page_content(ticker, tipo)
                self._save_raw(html, ticker, tipo)
                demo = self._parse_demonstrativo(html, ticker, tipo)
                resultados[tipo] = demo
                print(
                    f"✅ Fundamentus: {ticker.upper()}/{tipo.upper()} "
                    f"({len(demo.contas)} contas x {len(demo.trimestres)} períodos)"
                )
            except Exception as e:
                print(f"❌ Erro em {ticker.upper()}/{tipo.upper()}: {e}")

        return resultados
