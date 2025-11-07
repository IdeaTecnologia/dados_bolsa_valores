import csv
from io import StringIO
from curl_cffi import requests as curl_requests

class ListaTicker:
    def __init__(self):
        """
        Inicializa a classe e carrega imediatamente a lista de tickers da planilha online.
        """
        self._sheet_id = "1LDNmNs-sKXf3qPWCjNqdR_RO9fY7_cLhvJgoN9PKWlU"
        self._url = f"https://docs.google.com/spreadsheets/d/{self._sheet_id}/export?format=csv&gid=0"
        self.lista_ticker = self._carregar_tickers_online()

    def _carregar_tickers_online(self):
        """
        Carrega a lista de tickers de uma planilha pública do Google Sheets.
        """
        print("➤ Conectando com a planilha online para obter a lista de tickers...")
        
        try:
            # Usando curl_cffi para consistência com os outros scrapers
            response = curl_requests.get(self._url, impersonate="chrome110", timeout=30)
            response.raise_for_status()
            
            print("✅ Conexão com a planilha online estabelecida com sucesso.")
            
            # Lê o conteúdo CSV da resposta
            csv_content = response.text
            csv_file = StringIO(csv_content)
            csv_reader = csv.reader(csv_file)
            
            # Processa os tickers, procurando pelo prefixo "BVMF:"
            tickers = []
            # Pula o cabeçalho, se houver
            next(csv_reader, None) 
            for row in csv_reader:
                # Garante que a linha não está vazia e que o primeiro elemento é o ticker
                if row and len(row) > 0 and isinstance(row[0], str) and row[0].strip().upper().startswith("BVMF:"):
                    ticker_limpo = row[0].strip().upper().replace("BVMF:", "")
                    if ticker_limpo: # Adiciona apenas se não for vazio após a limpeza
                        tickers.append(ticker_limpo)
            
            print(f"✅ Lista de tickers carregada com {len(tickers)} ações encontradas.")
            return tickers
            
        except curl_requests.errors.RequestsError as e:
            print(f"❌ Erro de conexão ao carregar tickers: {e}")
            return []
        except Exception as e:
            print(f"❌ Erro inesperado ao carregar tickers: {e}")
            return []
    
    def obter_lista_ticker(self):
        """
        Retorna a lista de tickers que foi carregada durante a inicialização.
        """
        return self.lista_ticker