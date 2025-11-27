from datetime import datetime
import pytz
from scrapers.investidor10_scraper import Investidor10Scraper
from scrapers.fundamentus_scraper import FundamentusScraper
from scrapers.statusinvest_scraper import StatusInvestScraper
from scrapers.investsitepassivo_scraper import InvestSitePassivoScraper
from scrapers.investsiteindicadores_scraper import InvestSiteIndicadoresScraper

class Acao:
    def __init__(self, ticker):
        self.ticker = ticker


    def _reorganizar_json(self, dados_desordenados):
        """
        Reconstrói o dicionário garantindo que as chaves fiquem agrupadas
        por fonte (prefixo), independentemente da ordem de inserção.
        """
        ordem_prefixos = [
            "ticker",
            "investidor10",
            "fundamentus",
            "statusInvest",
            "investsitepassivo",
            "investsiteindicadores",
            "atualizado_em"
        ]
        
        dados_ordenados = {}
        
        # 1. Itera sobre a ordem desejada de prefixos
        for prefixo in ordem_prefixos:
            # Pega todas as chaves do dicionário que começam com esse prefixo
            # (Exceto 'ticker' e 'atualizado_em' que são exatos)
            chaves_do_grupo = [k for k in dados_desordenados.keys() if k.startswith(prefixo) or k == prefixo]
            
            # Opcional: ordenar alfabeticamente DENTRO do grupo
            # chaves_do_grupo.sort() 
            
            for k in chaves_do_grupo:
                dados_ordenados[k] = dados_desordenados[k]
        
        # 2. Adiciona chaves que sobraram (se houver alguma fora do padrão)
        for k, v in dados_desordenados.items():
            if k not in dados_ordenados:
                dados_ordenados[k] = v
                
        return dados_ordenados


    def get_all_data(self, dados_existentes=None, apenas_statusinvest=False, use_local_strategy=False):
        """
        :param dados_existentes: Dict com dados antigos (do JSON).
        :param apenas_statusinvest: Se True, NÃO roda Fundamentus/Inv10. Só atualiza StatusInvest.
        :param use_local_strategy: Se True, usa requests direto para StatusInvest.
        """
        
        # --- MODO: APENAS STATUS INVEST (Local) ---
        if apenas_statusinvest:
            if not dados_existentes:
                print(f"⚠️ Alerta: Tentando atualizar apenas StatusInvest para {self.ticker} sem dados prévios.")
                dados_combinados = {"ticker": self.ticker}
            else:
                # Copia os dados antigos (preserva Fundamentus, Inv10, etc.)
                dados_combinados = dados_existentes.copy()
            
            # Executa APENAS o scraper do StatusInvest
            print(f"Coletando APENAS StatusInvest para {self.ticker}...")
            dados_novos_si = StatusInvestScraper(self.ticker).fetch_data(use_local_strategy=True)
            
            # Atualiza/Mescla os dados
            dados_combinados.update(dados_novos_si)
            
            # Atualiza timestamp
            brasilia_tz = pytz.timezone('America/Sao_Paulo')
            dados_combinados["atualizado_em"] = datetime.now(brasilia_tz).strftime("%Y-%m-%d %H:%M:%S")
            
            dados_finais = self._reorganizar_json(dados_combinados)

            print(f"Dados de {self.ticker} processados.")
            return dados_finais # Retorna o organizado


        # --- MODO: COMPLETO (GitHub Actions / Update Geral) ---
        print(f"Coletando DADOS COMPLETOS para {self.ticker}...")

        # Scrapers Leves
        dados_inv10 = Investidor10Scraper(self.ticker).fetch_data()
        dados_fund = FundamentusScraper(self.ticker).fetch_data()
        dados_investsite_passivo = InvestSitePassivoScraper(self.ticker).fetch_data()
        dados_investsite_indicadores = InvestSiteIndicadoresScraper(self.ticker).fetch_data()

        # Lógica StatusInvest
        dados_statusinvest = {}
        
        # Se temos dados antigos injetados (para economizar API)
        if dados_existentes and 'statusInvest_data_atualizacao' in dados_existentes:
             # Aqui extraímos apenas os campos do StatusInvest do dicionário antigo
             dados_statusinvest = {k: v for k, v in dados_existentes.items() if k.startswith('statusInvest')}
             print(f"🔄 Mantendo dados antigos de StatusInvest para {self.ticker}.")
        else:
             # Se não tem cache, usa API
             dados_statusinvest = StatusInvestScraper(self.ticker).fetch_data(use_local_strategy=False)

        dados_combinados = {
            "ticker": self.ticker,
            **dados_inv10,
            **dados_fund,
            **dados_statusinvest,
            **dados_investsite_passivo,
            **dados_investsite_indicadores
        }

        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        dados_combinados["atualizado_em"] = datetime.now(brasilia_tz).strftime("%Y-%m-%d %H:%M:%S")

        print(f"Dados de {self.ticker} processados.")
        return dados_combinados