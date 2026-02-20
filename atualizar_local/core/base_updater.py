import sys
import os
from abc import ABC, abstractmethod
from datetime import datetime
import pytz

# Adiciona o diretório raiz ao path
pasta_raiz = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, pasta_raiz)

from utils.listaticker import ListaTicker
from atualizar_local.core.git_manager import GitManager
from atualizar_local.core.json_manager import JsonManager


class BaseUpdater(ABC):
    """Classe base abstrata para todos os atualizadores."""
    
    def __init__(self):
        self.git_manager = GitManager()
        self.json_manager = JsonManager()
        self.nome_site = self.get_nome_site()
    
    @abstractmethod
    def get_nome_site(self):
        """Retorna o nome do site (ex: 'Fundamentus')."""
        pass
    
    @abstractmethod
    def get_scraper_class(self):
        """Retorna a classe do scraper a ser usado."""
        pass


    # Pode ser sobrescrito
    def processar_ticker(self, ticker, dados_antigos):
        """
        Processa um ticker e retorna os dados mesclados.
        Pode ser sobrescrito para lógicas específicas.
        """
        scraper_class = self.get_scraper_class()
        scraper = scraper_class(ticker)
        dados_novos = scraper.fetch_data()
        
        # Adicionar timestamp
        from datetime import datetime
        import pytz
        tz_brasilia = pytz.timezone('America/Sao_Paulo')
        dados_novos['atualizado_em'] = datetime.now(tz_brasilia).strftime('%Y-%m-%d %H:%M:%S')
        
        # Mesclar: dados antigos + novos
        dados_mesclados = {**dados_antigos, **dados_novos}
        return dados_mesclados
    
    
    def executar(self, tickers_teste=None):
        """Executa o fluxo completo de atualização."""
        print("=" * 60)
        print(f"   📊 ATUALIZADOR {self.nome_site.upper()} LOCAL")
        print("=" * 60)
        
        # 1. GIT PULL (obrigatório)
        try:
            self.git_manager.pull()
        except Exception:
            return  # Interrompe se git pull falhar
        
        # 2. CARREGAR DADOS EXISTENTES
        print(f"\n[2/4] Carregando dados existentes...")
        dados_existentes_map = self.json_manager.carregar_dados_existentes()

        # 3. OBTER LISTA DE TICKERS
        if tickers_teste:
            acoes = tickers_teste
            print(f"🧪 Modo teste: {len(acoes)} ações")
        else:
            lista_provider = ListaTicker()
            acoes = lista_provider.obter_lista_ticker()
            print(f"✅ {len(acoes)} ações a processar")
        
        # 4. PROCESSAR CADA AÇÃO
        print(f"\n[3/4] Atualizando {self.nome_site}...")
        dados_finais = []
        scraper_class = self.get_scraper_class()
        
        total = len(acoes)
        for i, ticker in enumerate(acoes):
            print(f"\n--- [{i+1}/{total}] {ticker} ---")
            
            dados_antigos = dados_existentes_map.get(ticker, {})
            
            try:
                dados_mesclados = self.processar_ticker(ticker, dados_antigos)
                dados_finais.append(dados_mesclados)
                
                print(f"✅ {ticker}: {self.nome_site} atualizado")
                
                
            except Exception as e:
                print(f"❌ Erro em {ticker}: {e}")
                if dados_antigos:
                    dados_finais.append(dados_antigos)
                    print(f"⚠️ Mantido dado antigo de {ticker}")
        
        # 5. SALVAR JSON
        print(f"\n[4/4] Salvando arquivos...")
        if not self.json_manager.salvar_dados(dados_finais):
            print("❌ Falha ao salvar JSON. Abortando push.")
            return
        
        self.json_manager.gerar_metadata(dados_finais)
        
        # 6. GIT PUSH
        self.git_manager.push(
            ['dados_acoes.json', 'metadata.json'],
            f"Atualização {self.nome_site} Local"
        )
        
        print(f"\n🎉 Processo concluído!")