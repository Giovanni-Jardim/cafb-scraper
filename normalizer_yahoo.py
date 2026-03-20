# normalizer_yahoo.py
# Adaptado para dados do Yahoo Finance

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

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


class DemonstrativoNormalizer:
    """Padroniza nomes de contas Yahoo e calcula indicadores"""

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
        """Gera relatório em Markdown"""
        
        lines = [
            f"# Análise Fundamentalista: {bp.ticker}",
            "",
            f"## Resumo Executivo (Fonte: Yahoo Finance | Gerado: {pd.Timestamp.now().strftime('%d/%m/%Y')})",
            "",
            "### Últimos 4 Trimestres",
            "",
            "| Trimestre | ROE | Margem Líq | DL/EBITDA | FCO/Receita |",
            "|-----------|-----|------------|-----------|-------------|",
        ]

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

        return "\n".join(lines)


def processar_ticker(ticker: str, output_dir: str = "data/notebooklm"):
    """Pipeline completo: scrape -> normalize -> markdown"""
    
    scraper = YahooFinanceScraper()
    dados = asyncio.run(scraper.scrape_ticker(ticker))

    if not dados or len(dados) < 3:
        faltantes = [t for t in ("bp", "dre", "dfc") if t not in dados]
        raise ValueError(f"Demonstrativos ausentes para {ticker}: {', '.join(faltantes)}")

    normalizer = DemonstrativoNormalizer()
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