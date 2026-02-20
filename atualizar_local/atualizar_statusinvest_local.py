#!/usr/bin/env python3
import sys
import os

pasta_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pasta_raiz)

from atualizar_local.core.base_updater import BaseUpdater
from models.acao import Acao


class StatusInvestUpdater(BaseUpdater):
    def get_nome_site(self):
        return "StatusInvest"
    
    def get_scraper_class(self):
        # Não usa, mas precisa implementar (abstrato)
        return None
    
    # ✅ SOBRESCREVE o método de processamento
    def processar_ticker(self, ticker, dados_antigos):
        """Lógica específica do StatusInvest."""
        if not dados_antigos:
            dados_antigos = {"ticker": ticker}
        
        acao = Acao(ticker)
        dados_novos = acao.get_all_data(
            dados_existentes=dados_antigos,
            apenas_statusinvest=True,
            use_local_strategy=True  # SEM API
        )
        
        return dados_novos  # get_all_data já mescla e adiciona timestamp


if __name__ == "__main__":
    updater = StatusInvestUpdater()
    # updater.executar(tickers_teste=["WEGE3", "PETR4"])
    updater.executar()
