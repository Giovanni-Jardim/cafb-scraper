# tests/test_scraper.py
import pytest
from status_invest_scraper import StatusInvestScraper, StatusInvestAPI


@pytest.mark.asyncio
async def test_scraper_petr4():
    """Teste end-to-end com PETR4"""
    scraper = StatusInvestScraper(headless=True)
    resultados = await scraper.scrape_ticker('PETR4')
    
    assert 'bp' in resultados
    assert len(resultados['bp'].trimestres) >= 20  # 5+ anos de dados trimestrais
    assert 'Ativo Total' in resultados['bp'].contas or any('Ativo' in k for k in resultados['bp'].contas.keys())


def test_api_response_structure():
    """Valida estrutura da API"""
    api = StatusInvestAPI()
    data = api.get_historical_data('WEGE3', 'dre')
    
    assert 'success' in data
    assert 'periods' in data
    assert 'data' in data
    assert len(data['periods']) == len(list(data['data'].values())[0])


def test_normalizacao_contas():
    """Testa mapeamento de contas"""
    from normalizer import DemonstrativoNormalizer
    
    # Simula dados com nomes variantes
    demo_mock = Demonstrativo(
        tipo='bp',
        ticker='TEST',
        trimestres=['4T2023'],
        contas={
            'Ativo Tot': [1000],  # Variação proposital
            'PL': [500],
            'Dívida Liq': [200]
        },
        unidade='milhoes'
    )
    
    norm = DemonstrativoNormalizer()
    padronizado = norm.padronizar_contas(demo_mock)
    
    assert 'Ativo Total' in padronizado.contas
    assert 'Patrimônio Líquido' in padronizado.contas