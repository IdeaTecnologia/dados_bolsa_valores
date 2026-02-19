import os
import json


class JsonManager:
    """Gerencia leitura e escrita do JSON de dados."""
    
    def __init__(self, json_file='dados_acoes.json', metadata_file='metadata.json'):
        self.json_file = json_file
        self.metadata_file = metadata_file
    
    def carregar_dados_existentes(self):
        """Carrega dados existentes do JSON como dicionário {ticker: dados}."""
        if not os.path.exists(self.json_file):
            print(f"⚠️ {self.json_file} não encontrado. Criando novo...")
            return {}
        
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                lista = json.load(f)
                dados_map = {item['ticker']: item for item in lista}
                print(f"✅ Carregados {len(dados_map)} registros existentes")
                return dados_map
        except Exception as e:
            print(f"❌ Erro ao carregar JSON: {e}")
            return {}
    
    def salvar_dados(self, dados_finais):
        """Salva a lista de dados no JSON."""
        try:
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(dados_finais, f, indent=4, ensure_ascii=False)
            print(f"✅ {self.json_file} salvo ({len(dados_finais)} ações)")
            return True
        except IOError as e:
            print(f"❌ Erro ao salvar JSON: {e}")
            return False
    
    def gerar_metadata(self, dados_finais):
        """Gera arquivo metadata.json com estatísticas."""
        if not dados_finais:
            print("⚠️ Nenhum dado para gerar metadata")
            return
        
        ultima_data = dados_finais[-1].get("atualizado_em", "0000-00-00 00:00:00")
        
        metadata = {
            "ultima_atualizacao": ultima_data,
            "total_acoes": len(dados_finais)
        }
        
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=4)
            print(f"📊 Metadata gerado: {ultima_data}")
            return True
        except IOError as e:
            print(f"❌ Erro ao gerar metadata: {e}")
            return False