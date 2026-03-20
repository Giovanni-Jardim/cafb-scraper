# normalizer.py
# Padronização de contas e cálculo de indicadores

import pandas as pd
import numpy as np
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class IndicadoresCalculados:
    ticker: str
    trimestre: str
    roe: float
    roic: float
    margem_liquida: float
    margem_bruta: float
    ebitda_margin: float
    divida_liquida_ebitda: float
    pl_ativo: float
    capex_receita: float
    fco_receita: float


class DemonstrativoNormalizer:
    """Padroniza nomes de contas e calcula indicadores"""
    
    # Mapeamento de variações de nomes para padrão
    MAPEAMENTO_CONTAS = {
        # Ativo
        'Ativo Total': ['Ativo Total', 'ATIVO TOTAL', 'Total do Ativo', 'Ativo'],
        'Ativo Circulante': ['Ativo Circulante', 'Circulante', 'Ativo Circulante e Realizável'],
        'Caixa e Equivalentes': ['Caixa e Equivalentes', 'Disponibilidades', 'Caixa', 'Tesouraria'],
        
        # Passivo
        'Passivo Total': ['Passivo Total', 'PASSIVO TOTAL', 'Total do Passivo'],
        'Passivo Circulante': ['Passivo Circulante', 'Circulante Exigível'],
        'Dívida Bruta': ['Dívida Bruta', 'Endividamento', 'Dívida Total'],
        'Dívida Líquida': ['Dívida Líquida', 'DL', 'Net Debt'],
        
        # PL
        'Patrimônio Líquido': ['Patrimônio Líquido', 'PL', 'Capital Próprio', 'Patrimônio'],
        
        # DRE
        'Receita Líquida': ['Receita Líquida', 'Receita de Venda', 'Net Revenue', 'Faturamento'],
        'Lucro Bruto': ['Lucro Bruto', 'Resultado Bruto'],
        'EBITDA': ['EBITDA', 'Lucro antes Depreciação', 'LAJIDA'],
        'Depreciação': ['Depreciação e Amortização', 'D&A', 'Depreciação'],
        'EBIT': ['EBIT', 'Resultado Operacional', 'LO'],
        'Lucro Líquido': ['Lucro Líquido', 'LL', 'Net Income', 'Resultado Líquido'],
        
        # DFC
        'FCO': ['Fluxo de Caixa Operacional', 'FCO', 'CFO', 'Caixa das Operações'],
        'FCI': ['Fluxo de Caixa Investimento', 'FCI', 'CFI', 'Caixa de Investimentos'],
        'Capex': ['Aquisição de Imobilizado', 'Capex', 'Investimentos em Imobilizado'],
        'Dividendos Pagos': ['Dividendos e JCP Pagos', 'Dividendos', 'Pagamento de Dividendos']
    }
    
    def __init__(self):
        self.contas_padronizadas = {}
    
    def padronizar_contas(self, demo: Demonstrativo) -> Demonstrativo:
        """Mapeia nomes variantes para padrão"""
        contas_padrao = {}
        
        for padrao, variantes in self.MAPEAMENTO_CONTAS.items():
            # Procura por qualquer variação nas contas extraídas
            for conta_original, valores in demo.contas.items():
                conta_clean = conta_original.strip().lower()
                
                match = any(var.strip().lower() == conta_clean for var in variantes)
                if match:
                    contas_padrao[padrao] = valores
                    break
            
            # Se não achou, tenta fuzzy matching simples
            if padrao not in contas_padrao:
                for conta_original, valores in demo.contas.items():
                    if any(var.lower() in conta_original.lower() for var in variantes):
                        contas_padrao[padrao] = valores
                        break
        
        return Demonstrativo(
            tipo=demo.tipo,
            ticker=demo.ticker,
            trimestres=demo.trimestres,
            contas=contas_padrao,
            unidade=demo.unidade
        )
    
    def calcular_indicadores(self, bp: Demonstrativo, dre: Demonstrativo, dfc: Demonstrativo) -> List[IndicadoresCalculados]:
        """Calcula indicadores fundamentalistas por trimestre"""
        indicadores = []
        
        n_trimestres = len(bp.trimestres)
        
        for i in range(n_trimestres):
            tri = bp.trimestres[i]
            
            try:
                # Valores base
                pl = self._get_valor(bp, 'Patrimônio Líquido', i)
                ll = self._get_valor(dre, 'Lucro Líquido', i)
                ebitda = self._get_valor(dre, 'EBITDA', i)
                receita = self._get_valor(dre, 'Receita Líquida', i)
                lucro_bruto = self._get_valor(dre, 'Lucro Bruto', i)
                ativo = self._get_valor(bp, 'Ativo Total', i)
                divida_liq = self._get_valor(bp, 'Dívida Líquida', i)
                fco = self._get_valor(dfc, 'FCO', i)
                capex = self._get_valor(dfc, 'Capex', i)
                
                # Cálculos com tratamento de zero/None
                roe = (ll / pl * 100) if pl and pl != 0 else None
                roic = (ebitda / (pl + divida_liq) * 100) if (pl + divida_liq) and (pl + divida_liq) != 0 else None
                margem_liq = (ll / receita * 100) if receita and receita != 0 else None
                margem_bruta = (lucro_bruto / receita * 100) if receita and receita != 0 else None
                ebitda_margin = (ebitda / receita * 100) if receita and receita != 0 else None
                div_liq_ebitda = (divida_liq / ebitda) if ebitda and ebitda != 0 else None
                pl_ativo = (pl / ativo) if ativo and ativo != 0 else None
                capex_rec = (abs(capex) / receita * 100) if receita and receita != 0 and capex else None
                fco_rec = (fco / receita * 100) if receita and receita != 0 and fco else None
                
                indicadores.append(IndicadoresCalculados(
                    ticker=bp.ticker,
                    trimestre=tri,
                    roe=roe, roic=roic,
                    margem_liquida=margem_liq,
                    margem_bruta=margem_bruta,
                    ebitda_margin=ebitda_margin,
                    divida_liquida_ebitda=div_liq_ebitda,
                    pl_ativo=pl_ativo,
                    capex_receita=capex_rec,
                    fco_receita=fco_rec
                ))
                
            except Exception as e:
                print(f"Erro calculando indicadores para {tri}: {e}")
                continue
        
        return indicadores
    
    def _get_valor(self, demo: Demonstrativo, conta: str, indice: int) -> Optional[float]:
        """Safely pega valor de uma conta"""
        valores = demo.contas.get(conta, [])
        if indice < len(valores):
            return valores[indice]
        return None
    
    def gerar_markdown_analitico(self, bp: Demonstrativo, dre: Demonstrativo, dfc: Demonstrativo, 
                                  indicadores: List[IndicadoresCalculados]) -> str:
        """Gera documento final para NotebookLM"""
        
        lines = [
            f"# Análise Fundamentalista: {bp.ticker}",
            "",
            f"## Resumo Executivo (Gerado em {pd.Timestamp.now().strftime('%d/%m/%Y')})",
            "",
            "### Últimos 4 Trimestres",
            ""
        ]
        
        # Tabela de indicadores recentes
        recentes = indicadores[:4]
        lines.append("| Trimestre | ROE | Margem Líq | DL/EBITDA | FCO/Receita |")
        lines.append("|-----------|-----|------------|-----------|-------------|")
        
        for ind in recentes:
            roe = f"{ind.roe:.1f}%" if ind.roe else "N/A"
            marg = f"{ind.margem_liquida:.1f}%" if ind.margem_liquida else "N/A"
            div = f"{ind.divida_liquida_ebitda:.1f}x" if ind.divida_liquida_ebitda else "N/A"
            fco = f"{ind.fco_receita:.1f}%" if ind.fco_receita else "N/A"
            lines.append(f"| {ind.trimestre} | {roe} | {marg} | {div} | {fco} |")
        
        lines.extend([
            "",
            "## Série Histórica Completa (10+ anos)",
            ""
        ])
        
        # Adiciona dados brutos em formato que o NotebookLM pode cruzar
        for tipo, demo in [('BP', bp), ('DRE', dre), ('DFC', dfc)]:
            lines.extend([
                f"### {tipo} - {demo.unidade.upper()}",
                ""
            ])
            
            df = demo.to_dataframe()
            lines.append(df.to_markdown())
            lines.append("")
        
        # Tendências calculadas
        lines.extend([
            "## Análise de Tendências",
            ""
        ])
        
        # Calcula variações
        if len(indicadores) >= 8:
            roe_recente = np.mean([i.roe for i in indicadores[:4] if i.roe])
            roe_antigo = np.mean([i.roe for i in indicadores[4:8] if i.roe])
            if roe_recente and roe_antigo:
                var_roe = ((roe_recente / roe_antigo) - 1) * 100
                tendencia = "melhora" if var_roe > 5 else "estabilidade" if var_roe > -5 else "deterioração"
                lines.append(f"- **ROE:** {tendencia} de {var_roe:+.1f}% comparando últimos 4T vs 4T anteriores")
        
        return "\n".join(lines)


# Uso
def processar_ticker(ticker: str):
    """Pipeline completo: scrape -> normalize -> markdown"""
    
    # 1. Scrape (API ou browser)
    dados = asyncio.run(scrape_hibrido(ticker))
    
    # 2. Normaliza
    normalizer = DemonstrativoNormalizer()
    bp_pad = normalizer.padronizar_contas(dados['bp'])
    dre_pad = normalizer.padronizar_contas(dados['dre'])
    dfc_pad = normalizer.padronizar_contas(dados['dfc'])
    
    # 3. Calcula indicadores
    indicadores = normalizer.calcular_indicadores(bp_pad, dre_pad, dfc_pad)
    
    # 4. Gera markdown para NotebookLM
    markdown = normalizer.gerar_markdown_analitico(bp_pad, dre_pad, dfc_pad, indicadores)
    
    # Salva
    output_path = Path(f"data/notebooklm/{ticker}_analise.md")
    output_path.write_text(markdown, encoding='utf-8')
    
    print(f"✅ Documento gerado: {output_path}")
    return markdown