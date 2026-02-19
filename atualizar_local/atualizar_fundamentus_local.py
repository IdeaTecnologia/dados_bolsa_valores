#!/usr/bin/env python3
import sys
import os

# Adiciona o diretório raiz ao path
pasta_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pasta_raiz)

from atualizar_local.core.base_updater import BaseUpdater
from scrapers.fundamentus_scraper import FundamentusScraper


class FundamentusUpdater(BaseUpdater):
    def get_nome_site(self):
        return "Fundamentus"
    
    def get_scraper_class(self):
        return FundamentusScraper


if __name__ == "__main__":
    updater = FundamentusUpdater()
    
    updater.executar()