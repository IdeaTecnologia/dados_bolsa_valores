# acao.py

from datetime import datetime
import pytz
from scrapers.investidor10_scraper import Investidor10Scraper
from scrapers.fundamentus_scraper import FundamentusScraper
from scrapers.statusinvest_scraper import StatusInvestScraper
from scrapers.investsitepassivo_scraper import InvestSitePassivoScraper
from scrapers.investsiteindicadores_scraper import InvestSiteIndicadoresScraper # <-- ADICIONE ESTA LINHA

class Acao:
    def __init__(self, ticker):
        if not isinstance(ticker, str) or not ticker:
            raise ValueError("O ticker deve ser uma string não vazia.")
        self.ticker = ticker

    def get_all_data(self):
        print(f"Coletando dados para {self.ticker}...")

        # Instancia e executa cada scraper
        dados_inv10 = Investidor10Scraper(self.ticker).fetch_data()
        dados_fund = FundamentusScraper(self.ticker).fetch_data()
        dados_statusinvest = StatusInvestScraper(self.ticker).fetch_data()
        dados_investsite_passivo = InvestSitePassivoScraper(self.ticker).fetch_data()
        dados_investsite_indicadores = InvestSiteIndicadoresScraper(self.ticker).fetch_data() # <-- ADICIONE ESTA LINHA

        # Combina os dicionários de dados
        dados_combinados = {
            **dados_inv10,
            **dados_fund,
            **dados_statusinvest,
            **dados_investsite_passivo,
            **dados_investsite_indicadores  # <-- ADICIONE ESTA LINHA
        }
        dados_combinados["ticker"] = self.ticker

        # Adiciona timestamp
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        dados_combinados["atualizado_em"] = datetime.now(brasilia_tz).strftime("%Y-%m-%d %H:%M:%S")

        print(f"Dados de {self.ticker} coletados com sucesso.")
        return dados_combinados