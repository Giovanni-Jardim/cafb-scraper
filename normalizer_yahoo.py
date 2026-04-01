# normalizer_yahoo.py
# Adaptado para dados do Yahoo Finance com Proventos, LPA e VPA

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
import math

import numpy as np
import pandas as pd


def normalize_dividend_yield_percent(val):
    """
    Normaliza dividendYield para percentual.
    Yahoo pode retornar tanto fração (0.1381 => 13.81%) quanto percentual (13.81).
    Regra: valores <= 1 são tratados como fração; acima disso, como percentual já pronto.
    """
    if val is None or val == 'N/A' or pd.isna(val):
        return None
    try:
        dy = Decimal(str(val).replace(',', ''))
        return (dy * Decimal('100')) if dy <= Decimal('1') else dy
    except:
        return None

from yahoo_finance_scraper import Demonstrativo, YahooFinanceScraper


@dataclass
class IndicadoresCalculados:
    ticker: str
    trimestre: str
    roe: Optional[float]
    roic: Optional[float]
    margem_liquida: Optional[float]
    margem_bruta: Optional[float]
    ebitda_margin: Optional[float]
    divida_liquida_ebitda: Optional[float]
    pl_ativo: Optional[float]
    capex_receita: Optional[float]
    fco_receita: Optional[float]


@dataclass
class DadosMercado:
    """Dados de mercado incluindo LPA, VPA e Número de Ações"""
    cotacao: Optional[Decimal] = None
    shares_outstanding: Optional[int] = None  # Número total de ações
    lpa: Optional[Decimal] = None  # Lucro Por Ação (TTM)
    vpa: Optional[Decimal] = None  # Valor Patrimonial por Ação
    dividend_yield: Optional[Decimal] = None
    dividendo_12m: Optional[Decimal] = None
    preco_teto_bazin: Optional[Decimal] = None
    preco_justo_graham: Optional[Decimal] = None


@dataclass
class HistoricoDividendos:
    """Histórico de proventos pagos"""
    dados: pd.DataFrame = field(default_factory=pd.DataFrame)
    media_anual: Optional[Decimal] = None
    total_ultimo_ano: Optional[Decimal] = None
    anos_disponiveis: int = 0


class DemonstrativoNormalizer:
    """Padroniza nomes de contas Yahoo, calcula indicadores e extrai proventos"""

    # Mapeamento adicional para variações do Yahoo
    MAPEAMENTO_CONTAS = {
        # Ativo
        "Ativo Total": ["Total Assets", "Ativo Total"],
        "Ativo Circulante": ["Current Assets", "Ativo Circulante"],
        "Caixa e Equivalentes": ["Cash And Cash Equivalents", "Caixa e Equivalentes"],
        
        # Passivo
        "Passivo Total": ["Total Liabilities Net Minority Interest", "Passivo Total"],
        "Passivo Circulante": ["Current Liabilities", "Passivo Circulante"],
        "Dívida Bruta": ["Total Debt", "Dívida Bruta"],
        "Dívida Líquida": ["Net Debt", "Dívida Líquida"],
        
        # PL
        "Patrimônio Líquido": ["Stockholders Equity", "Patrimônio Líquido"],
        
        # DRE
        "Receita Líquida": ["Total Revenue", "Receita Líquida"],
        "Lucro Bruto": ["Gross Profit", "Lucro Bruto"],
        "EBITDA": ["EBITDA", "Ebitda"],
        "EBIT": ["Operating Income", "EBIT"],
        "Lucro Líquido": ["Net Income", "Lucro Líquido"],
        
        # DFC
        "FCO": ["Operating Cash Flow", "FCO"],
        "Capex": ["Capital Expenditure", "Capex"],
    }

    def __init__(self):
        self.contas_padronizadas = {}
        self.dados_mercado: Optional[DadosMercado] = None
        self.historico_dividendos: Optional[HistoricoDividendos] = None

    def extrair_dados_mercado(self, info: Dict[str, Any], ticker: str) -> DadosMercado:
        """
        Extrai LPA, VPA, Cotação e Número de Ações do Yahoo Finance.
        Calcula também Preço Teto (Bazin) e Preço Justo (Graham).
        """
        def safe_decimal(val) -> Optional[Decimal]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return Decimal(str(val).replace(',', ''))
            except:
                return None
        
        def safe_int(val) -> Optional[int]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return int(float(str(val).replace(',', '')))
            except:
                return None
        
        def safe_float(val) -> Optional[float]:
            if val is None or val == 'N/A' or pd.isna(val):
                return None
            try:
                return float(val)
            except:
                return None

        # Extrai valores
        cotacao = safe_decimal(info.get('currentPrice') or info.get('regularMarketPrice'))
        shares = safe_int(info.get('sharesOutstanding'))
        lpa = safe_decimal(info.get('trailingEps'))
        vpa = safe_decimal(info.get('bookValue'))
        
        dy = normalize_dividend_yield_percent(info.get('dividendYield'))
        
        div_rate = safe_decimal(info.get('dividendRate'))
        
        # Calcula Bazin (Yield 6%)
        preco_teto = None
        if div_rate and div_rate > 0:
            preco_teto = div_rate / Decimal('0.06')
        
        # Calcula Graham
        preco_justo = None
        if lpa and vpa and lpa > 0 and vpa > 0:
            try:
                valor_interno = Decimal('22.5') * lpa * vpa
                preco_justo = Decimal(str(math.sqrt(float(valor_interno))))
            except:
                pass
        
        self.dados_mercado = DadosMercado(
            cotacao=cotacao,
            shares_outstanding=shares,
            lpa=lpa,
            vpa=vpa,
            dividend_yield=dy,
            dividendo_12m=div_rate,
            preco_teto_bazin=preco_teto,
            preco_justo_graham=preco_justo
        )
        
        return self.dados_mercado

    def extrair_historico_dividendos(self, ticker_obj, ticker: str, anos: int = 5) -> Optional[HistoricoDividendos]:
        """
        NOVO: Extrai histórico de proventos (dividendos) dos últimos N anos.
        """
        try:
            actions = ticker_obj.actions
            
            if actions is None or actions.empty:
                print(f"⚠️ {ticker}: Sem histórico de dividendos")
                return None
            
            if 'Dividends' not in actions.columns:
                print(f"⚠️ {ticker}: Coluna 'Dividends' não encontrada")
                return None
            
            # Filtra apenas dividendos
            divs = actions[actions['Dividends'] > 0]['Dividends']
            
            if divs.empty:
                print(f"⚠️ {ticker}: Nenhum dividendo encontrado")
                return None
            
            # Processa datas e valores
            df_divs = divs.reset_index()
            df_divs.columns = ['data', 'valor']
            df_divs['data'] = pd.to_datetime(df_divs['data'])
            df_divs['ano'] = df_divs['data'].dt.year
            
            # Filtra últimos N anos
            ano_atual = datetime.now().year
            df_divs = df_divs[df_divs['ano'] >= (ano_atual - anos)]
            
            # Agrupa por ano
            anual = df_divs.groupby('ano').agg({
                'valor': ['sum', 'count']
            }).reset_index()
            anual.columns = ['ano', 'total_anual', 'quantidade']
            
            # Cria DataFrame final
            hist_df = pd.DataFrame({
                'Ano': anual['ano'].astype(int),
                'Total Proventos (R$)': anual['total_anual'].round(4),
                'Nº Pagamentos': anual['quantidade'].astype(int)
            })
            
            # Calcula estatísticas
            valores = hist_df['Total Proventos (R$)'].tolist()
            media = Decimal(str(sum(valores) / len(valores))) if valores else None
            total_ultimo = Decimal(str(valores[-1])) if valores else None
            
            self.historico_dividendos = HistoricoDividendos(
                dados=hist_df,
                media_anual=media,
                total_ultimo_ano=total_ultimo,
                anos_disponiveis=len(hist_df)
            )
            
            print(f"✅ {ticker}: {len(hist_df)} anos de dividendos | Média: R$ {media:.2f}" if media else f"✅ {ticker}: {len(hist_df)} anos de dividendos")
            return self.historico_dividendos
            
        except Exception as e:
            print(f"❌ Erro ao extrair dividendos {ticker}: {e}")
            return None

    def padronizar_contas(self, demo: Demonstrativo) -> Demonstrativo:
        """Mapeia nomes do Yahoo para padrão brasileiro"""
        contas_padrao: Dict[str, List[Optional[float]]] = {}

        for padrao, variantes in self.MAPEAMENTO_CONTAS.items():
            for conta_original, valores in demo.contas.items():
                if any(var.lower() in conta_original.lower() for var in variantes):
                    contas_padrao[padrao] = valores
                    break

        return Demonstrativo(
            tipo=demo.tipo,
            ticker=demo.ticker,
            trimestres=demo.trimestres,
            contas=contas_padrao,
            unidade=demo.unidade,
        )

    def calcular_indicadores(
        self,
        bp: Demonstrativo,
        dre: Demonstrativo,
        dfc: Demonstrativo,
    ) -> List[IndicadoresCalculados]:
        """Calcula indicadores fundamentalistas por trimestre"""
        indicadores: List[IndicadoresCalculados] = []

        n_trimestres = min(len(bp.trimestres), len(dre.trimestres), len(dfc.trimestres))

        for i in range(n_trimestres):
            tri = bp.trimestres[i]

            try:
                pl = self._get_valor(bp, "Patrimônio Líquido", i)
                ll = self._get_valor(dre, "Lucro Líquido", i)
                ebitda = self._get_valor(dre, "EBITDA", i)
                receita = self._get_valor(dre, "Receita Líquida", i)
                lucro_bruto = self._get_valor(dre, "Lucro Bruto", i)
                ativo = self._get_valor(bp, "Ativo Total", i)
                divida_liq = self._get_valor(bp, "Dívida Líquida", i)
                fco = self._get_valor(dfc, "FCO", i)
                capex = self._get_valor(dfc, "Capex", i)

                # Cálculos com proteção contra divisão por zero
                roe = (ll / pl * 100) if pl and pl != 0 and ll else None
                roic = None
                if ebitda:
                    capital_investido = (pl or 0) + (divida_liq or 0)
                    if capital_investido and capital_investido != 0:
                        roic = ebitda / capital_investido * 100
                
                margem_liq = (ll / receita * 100) if receita and receita != 0 and ll else None
                margem_bruta = (lucro_bruto / receita * 100) if receita and receita != 0 and lucro_bruto else None
                ebitda_margin = (ebitda / receita * 100) if receita and receita != 0 and ebitda else None
                div_liq_ebitda = (divida_liq / ebitda) if ebitda and ebitda != 0 and divida_liq else None
                pl_ativo = (pl / ativo) if ativo and ativo != 0 and pl else None
                capex_rec = (abs(capex) / receita * 100) if receita and receita != 0 and capex else None
                fco_rec = (fco / receita * 100) if receita and receita != 0 and fco else None

                indicadores.append(
                    IndicadoresCalculados(
                        ticker=bp.ticker,
                        trimestre=tri,
                        roe=roe,
                        roic=roic,
                        margem_liquida=margem_liq,
                        margem_bruta=margem_bruta,
                        ebitda_margin=ebitda_margin,
                        divida_liquida_ebitda=div_liq_ebitda,
                        pl_ativo=pl_ativo,
                        capex_receita=capex_rec,
                        fco_receita=fco_rec,
                    )
                )

            except Exception as e:
                print(f"Erro calculando indicadores para {tri}: {e}")
                continue

        return indicadores

    def _get_valor(self, demo: Demonstrativo, conta: str, indice: int) -> Optional[float]:
        """Obtém valor de conta com segurança"""
        valores = demo.contas.get(conta, [])
        if indice < len(valores):
            return valores[indice]
        return None

    def gerar_markdown_analitico(
        self,
        bp: Demonstrativo,
        dre: Demonstrativo,
        dfc: Demonstrativo,
        indicadores: List[IndicadoresCalculados],
    ) -> str:
        """Gera relatório em Markdown com Proventos, LPA, VPA e Valuation"""
        
        lines = [
            f"# Análise Fundamentalista: {bp.ticker}",
            "",
            f"**Fonte:** Yahoo Finance | **Gerado:** {pd.Timestamp.now().strftime('%d/%m/%Y')}",
            "",
        ]
        
        # ==================== SEÇÃO DE MERCADO (NOVO) ====================
        if self.dados_mercado:
            dm = self.dados_mercado
            lines.extend([
                "## 📊 Dados de Mercado",
                "",
                "| Métrica | Valor |",
                "|---------|-------|",
                f"| Cotação Atual | R$ {dm.cotacao:.2f} |" if dm.cotacao else "",
                f"| Nº Total de Ações | {dm.shares_outstanding:,.0f} |" if dm.shares_outstanding else "",
                f"| LPA (TTM) | R$ {dm.lpa:.2f} |" if dm.lpa else "",
                f"| VPA | R$ {dm.vpa:.2f} |" if dm.vpa else "",
                f"| Dividend Yield | {dm.dividend_yield:.2f}% |" if dm.dividend_yield else "",
                f"| Proventos 12m | R$ {dm.dividendo_12m:.2f} |" if dm.dividendo_12m else "",
                "",
            ])
            
            # Valuation
            lines.extend([
                "### Valuation",
                "",
                "| Método | Preço Calculado | Margem Segurança |",
                "|--------|-----------------|------------------|",
            ])
            
            if dm.preco_teto_bazin and dm.cotacao:
                margem_bazin = ((dm.preco_teto_bazin / dm.cotacao) - 1) * 100
                rec_bazin = "COMPRA" if margem_bazin >= 10 else ("VENDA" if margem_bazin <= -10 else "AGUARDAR")
                lines.append(f"| Bazin (6%) | R$ {dm.preco_teto_bazin:.2f} | {margem_bazin:+.1f}% ({rec_bazin}) |")
            
            if dm.preco_justo_graham and dm.cotacao:
                margem_graham = ((dm.preco_justo_graham / dm.cotacao) - 1) * 100
                rec_graham = "COMPRA" if margem_graham >= 15 else ("VENDA" if margem_graham <= -15 else "AGUARDAR")
                lines.append(f"| Graham | R$ {dm.preco_justo_graham:.2f} | {margem_graham:+.1f}% ({rec_graham}) |")
            
            lines.append("")
        
        # ==================== HISTÓRICO DE PROVENTOS (NOVO) ====================
        if self.historico_dividendos and not self.historico_dividendos.dados.empty:
            hd = self.historico_dividendos
            lines.extend([
                "---",
                "",
                "## 💰 Histórico de Proventos (Últimos Anos)",
                "",
                hd.dados.to_markdown(index=False),
                "",
            ])
            
            if hd.media_anual:
                lines.append(f"**Média Anual:** R$ {hd.media_anual:.4f}")
            if hd.total_ultimo_ano:
                lines.append(f"**Último Ano:** R$ {hd.total_ultimo_ano:.4f}")
            lines.append("")
        
        # ==================== INDICADORES ====================
        lines.extend([
            "---",
            "",
            "## 📈 Indicadores Fundamentalistas",
            "",
            "### Últimos 4 Trimestres",
            "",
            "| Trimestre | ROE | Margem Líq | DL/EBITDA | FCO/Receita |",
            "|-----------|-----|------------|-----------|-------------|",
        ])

        recentes = indicadores[:4]
        for ind in recentes:
            roe = f"{ind.roe:.1f}%" if ind.roe else "N/A"
            marg = f"{ind.margem_liquida:.1f}%" if ind.margem_liquida else "N/A"
            div = f"{ind.divida_liquida_ebitda:.1f}x" if ind.divida_liquida_ebitda else "N/A"
            fco = f"{ind.fco_receita:.1f}%" if ind.fco_receita else "N/A"
            lines.append(f"| {ind.trimestre} | {roe} | {marg} | {div} | {fco} |")

        lines.extend(["", "## Série Histórica Completa", ""])

        for tipo, demo in [("BP", bp), ("DRE", dre), ("DFC", dfc)]:
            lines.extend([
                f"### {tipo} - {demo.unidade.upper()}",
                "",
                demo.to_dataframe().to_markdown(),
                "",
            ])

        # Análise de tendências
        if len(indicadores) >= 8:
            roe_recente = [i.roe for i in indicadores[:4] if i.roe]
            roe_antigo = [i.roe for i in indicadores[4:8] if i.roe]
            
            if roe_recente and roe_antigo:
                var = ((np.mean(roe_recente) / np.mean(roe_antigo)) - 1) * 100
                tendencia = "melhora" if var > 5 else "deterioração" if var < -5 else "estabilidade"
                lines.extend([
                    "",
                    "## Análise de Tendências",
                    "",
                    f"- **ROE:** {tendencia} de {var:+.1f}% (últimos 4T vs anteriores)"
                ])
        
        # Disclaimer
        lines.extend([
            "",
            "---",
            "",
            "**Disclaimer:** Análise baseada em dados públicos do Yahoo Finance. Não constitui recomendação de investimento.",
        ])

        return "\n".join(lines)


def processar_ticker(ticker: str, output_dir: str = "data/notebooklm"):
    """Pipeline completo: scrape -> normalize -> markdown (com proventos, LPA e VPA)"""
    
    # Importa aqui para evitar circular import
    import yfinance as yf
    
    scraper = YahooFinanceScraper()
    
    # Scrape dos demonstrativos financeiros
    dados = asyncio.run(scraper.scrape_ticker(ticker))

    if not dados or len(dados) < 3:
        faltantes = [t for t in ("bp", "dre", "dfc") if t not in dados]
        raise ValueError(f"Demonstrativos ausentes para {ticker}: {', '.join(faltantes)}")

    normalizer = DemonstrativoNormalizer()
    
    # ==================== NOVO: Extrai dados de mercado e dividendos ====================
    try:
        # Busca ticker no Yahoo
        if not ticker.endswith(".SA") and len(ticker) >= 5 and ticker[-1].isdigit():
            ticker_yahoo = f"{ticker}.SA"
        else:
            ticker_yahoo = ticker
            
        ticker_obj = yf.Ticker(ticker_yahoo)
        info = ticker_obj.info
        
        # Extrai LPA, VPA, Cotação, Número de Ações
        normalizer.extrair_dados_mercado(info, ticker)
        
        # Extrai histórico de proventos
        normalizer.extrair_historico_dividendos(ticker_obj, ticker, anos=5)
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados de mercado para {ticker}: {e}")
    
    # Processa demonstrativos
    bp_pad = normalizer.padronizar_contas(dados["bp"])
    dre_pad = normalizer.padronizar_contas(dados["dre"])
    dfc_pad = normalizer.padronizar_contas(dados["dfc"])

    indicadores = normalizer.calcular_indicadores(bp_pad, dre_pad, dfc_pad)
    markdown = normalizer.gerar_markdown_analitico(bp_pad, dre_pad, dfc_pad, indicadores)

    output_path = Path(output_dir) / f"{ticker}_analise.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")

    print(f"✅ Análise gerada: {output_path}")
    return markdown
