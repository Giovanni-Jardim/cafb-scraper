# status_invest_api.py
# API oculta descoberta via DevTools (XHR/Fetch)

import requests
import json
from typing import Dict, List


class StatusInvestAPI:
    """Usa endpoints internos da API (mais estável que scraping)"""
    
    BASE_API = "https://statusinvest.com.br"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://statusinvest.com.br/'
        })
    
    def get_historical_data(self, ticker: str, tipo: str = "bp") -> Dict:
        """
        Endpoint descoberto: /acao/companyindicators
        
        tipo: 'bp' (balanco), 'dre' (resultado), 'dfc' (fluxo)
        """
        # Mapeia tipo para código interno
        type_map = {
            'bp': 1,      # Balanço Patrimonial
            'dre': 2,     # Demonstração Resultado
            'dfc': 3,     # Demonstração Fluxo Caixa
            'dividends': 4,
            'indicators': 5
        }
        
        url = f"{self.BASE_API}/acao/companyindicators"
        
        params = {
            'ticker': ticker,
            'type': type_map.get(tipo, 1),
            'periodType': 2  # 2 = trimestral (1 = anual)
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API falhou para {ticker}/{tipo}: {e}")
            return {}
    
    def parse_api_response(self, data: Dict, ticker: str, tipo: str) -> Demonstrativo:
        """Converte resposta JSON para objeto Demonstrativo"""
        # Estrutura típica da API:
        # {
        #   "success": true,
        #   "indicators": [...],
        #   "periods": ["4T2023", "3T2023", ...],
        #   "data": {
        #       "Ativo Total": [1000000, 950000, ...],
        #       "Passivo Total": [...]
        #   }
        # }
        
        if not data.get('success'):
            raise ValueError(f"API retornou erro: {data.get('message', 'unknown')}")
        
        periods = data.get('periods', [])
        raw_data = data.get('data', {})
        
        # A API já vem estruturada, só precisamos normalizar
        contas = {}
        for conta_nome, valores in raw_data.items():
            # Valores já vêm em formato numérico, mas precisam de ajuste de escala
            contas[conta_nome] = [self._normalize_value(v) for v in valores]
        
        # Detecta unidade pela magnitude
        unidade = self._detect_unit_from_values(contas)
        
        return Demonstrativo(
            tipo=tipo,
            ticker=ticker,
            trimestres=periods,
            contas=contas,
            unidade=unidade
        )
    
    def _normalize_value(self, val) -> Optional[float]:
        """Normaliza valores que podem vir como string ou número"""
        if val is None or val == '-' or val == '':
            return None
        
        if isinstance(val, (int, float)):
            return float(val)
        
        # Se vier como string, aplica parsing
        if isinstance(val, str):
            val = val.replace('.', '').replace(',', '.')
            try:
                return float(val)
            except:
                return None
        
        return None
    
    def _detect_unit_from_values(self, contas: Dict) -> str:
        """Detecta unidade analisando magnitude típica"""
        amostras = []
        for valores in contas.values():
            amostras.extend([abs(v) for v in valores if v is not None])
        
        if not amostras:
            return 'milhoes'
        
        mediana = sorted(amostras)[len(amostras)//2]
        
        if mediana > 1e9:
            return 'bilhoes'
        elif medicana > 1e6:
            return 'milhoes'
        else:
            return 'milhares'
    
    def get_all_demonstrativos(self, ticker: str) -> Dict[str, Demonstrativo]:
        """Busca BP, DRE e DFC via API"""
        resultados = {}
        
        for tipo in ['bp', 'dre', 'dfc']:
            try:
                raw = self.get_historical_data(ticker, tipo)
                if raw:
                    demo = self.parse_api_response(raw, ticker, tipo)
                    resultados[tipo] = demo
                    print(f"✅ API: {ticker}/{tipo.upper()} ({len(demo.trimestres)} períodos)")
            except Exception as e:
                print(f"❌ API falhou para {tipo}: {e}")
        
        return resultados


# Uso híbrido: tenta API primeiro, fallback para scraping
async def scrape_hibrido(ticker: str) -> Dict[str, Demonstrativo]:
    """Estratégia: API primeiro, scraping se falhar"""
    
    # Tenta API (mais rápida, menos detectável)
    api = StatusInvestAPI()
    resultados = api.get_all_demonstrativos(ticker)
    
    # Se faltar algum demonstrativo, tenta scraping
    faltantes = [t for t in ['bp', 'dre', 'dfc'] if t not in resultados]
    
    if faltantes:
        print(f"🔄 Fallback para scraping: {faltantes}")
        scraper = StatusInvestScraper(headless=True)
        scraped = await scraper.scrape_ticker(ticker, faltantes)
        resultados.update(scraped)
    
    return resultados