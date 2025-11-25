import requests
import os
from bs4 import BeautifulSoup
from utils.normalization import normalize_numeric_value

from dotenv import load_dotenv
load_dotenv() # Carrega variáveis do arquivo .env se ele existir


# Dicionários de mapeamento permanecem os mesmos
STATUSINVEST_INDICATORS_MAP = {
    "Valor atual": "statusInvest_cotacao", "Min. 52 semanas": "statusInvest_min_52_semanas", "Máx. 52 semanas": "statusInvest_max_52_semanas",
    "Dividend Yield": "statusInvest_dy_percentual", "Valorização (12m)": "statusInvest_valorizacao_12m_percentual", "D.Y": "statusInvest_dy_percentual",
    "P/L": "statusInvest_pl", "PEG Ratio": "statusInvest_peg_ratio", "P/VP": "statusInvest_pvp", "EV/EBITDA": "statusInvest_ev_ebitda", "EV/EBIT": "statusInvest_ev_ebit",
    "P/EBITDA": "statusInvest_p_ebitda", "P/EBIT": "statusInvest_p_ebit", "VPA": "statusInvest_vpa", "P/Ativo": "statusInvest_p_ativo", "LPA": "statusInvest_lpa",
    "P/SR": "statusInvest_psr", "P/Cap. Giro": "statusInvest_p_cap_giro", "P/Ativo Circ. Liq.": "statusInvest_p_ativo_circ_liq", "Dív. líquida/PL": "statusInvest_div_liq_pl",
    "Dív. líquida/EBITDA": "statusInvest_div_liq_ebitda", "Dív. líquida/EBIT": "statusInvest_div_liq_ebit", "PL/Ativos": "statusInvest_pl_ativos",
    "Passivos/Ativos": "statusInvest_passivos_ativos", "Liq. corrente": "statusInvest_liq_corrente", "Giro ativos": "statusInvest_giro_ativos",
    "M. Bruta": "statusInvest_margem_bruta_percentual", "M. EBITDA": "statusInvest_margem_ebitda_percentual", "M. EBIT": "statusInvest_margem_ebit_percentual",
    "M. Líquida": "statusInvest_margem_liquida_percentual", "ROE": "statusInvest_roe_percentual", "ROA": "statusInvest_roa_percentual",
    "ROIC": "statusInvest_roic_percentual", "CAGR Receitas 5 anos": "statusInvest_cagr_rec_5anos_percentual", "CAGR Lucros 5 anos": "statusInvest_cagr_lucros_5anos_percentual",
    "Patrimônio líquido": "statusInvest_patrimonio_liquido", "Ativos": "statusInvest_ativos", "Ativo circulante": "statusInvest_ativo_circulante",
    "Dívida bruta": "statusInvest_divida_bruta", "Dívida líquida": "statusInvest_divida_liquida", "Valor de mercado": "statusInvest_valor_mercado",
    "Valor de firma": "statusInvest_valor_firma", "Nº total de papéis": "statusInvest_nro_papeis", "Free Float": "statusInvest_free_float_percentual",
    "Tag Along": "statusInvest_tag_along_percentual", "Liquidez média diária": "statusInvest_liquidez_media_diaria", "Setor de Atuação": "statusInvest_setor",
    "Subsetor de Atuação": "statusInvest_subsetor", "Segmento de Atuação": "statusInvest_segmento",
}

NON_NUMERIC_KEYS = {"statusInvest_setor", "statusInvest_subsetor", "statusInvest_segmento"}

class StatusInvestScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.target_url = f"https://statusinvest.com.br/acoes/{self.ticker.lower()}"

    def _get_all_possible_keys(self):
        return list(STATUSINVEST_INDICATORS_MAP.values())

    def _process_and_store_data(self, dados, key, raw_value, overwrite=True):
        if not overwrite and (key in dados and dados[key] is not None): return
        if key in NON_NUMERIC_KEYS:
            dados[key] = raw_value.strip() if isinstance(raw_value, str) else raw_value
        else:
            normalized_value = normalize_numeric_value(raw_value)
            if normalized_value is not None: dados[key] = normalized_value

    
    def fetch_data(self):
        all_keys = self._get_all_possible_keys()
        dados = {key: None for key in all_keys}
        dados["ticker"] = self.ticker
        dados["erro_statusinvest"] = ""

        # O site Status Invest tem proteções contra scraping direto.
        # Usaremos a API do ScrapeNinja via RapidAPI para contornar isso.
        # O limite gratuito é de 100 requisições por mês, conforme site https://apiroad.net/marketplace/apis/scrapeninja

        # Tenta pegar a chave da API ScrapeNinja salva (os.getenv funciona tanto com .env local quanto com GitHub Secrets)
        api_key = os.getenv('RAPIDAPI_KEY')
        
        if not api_key:
            error_msg = "Chave da API ScrapeNinja não encontrada."
            print(f"!!! ERRO: {error_msg}")
            dados["erro_statusinvest"] = error_msg
            return dados

        api_url = 'https://scrapeninja.p.rapidapi.com/scrape'
        
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "scrapeninja.p.rapidapi.com"
        }
        
        payload = {
            "url": self.target_url,
            "retryNum": 1,
            "geo": "br",
            # Instruções para esperar o site carregar o JS
            "renderJs": True, 
            "wait": 5000, # Espera 5 segundos antes de pegar o HTML
            "headers": [
                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
        }

        try:
            print(f"Buscando dados para {self.ticker} via ScrapeNinja...")
            response = requests.post(api_url, json=payload, headers=headers)
            response.raise_for_status()

            response_json = response.json()
            html_content = response_json.get('body', '')

            if not html_content:
                raise ValueError("A resposta da API não contém o corpo HTML.")

            # --- Salvar HTML para DEBUG ---
            # Isso vai criar um arquivo tipo "debug_EGIE3.html" na pasta.
            # Abra esse arquivo no navegador para ver se os dados estão lá.
            # debug_filename = f"debug_{self.ticker}.html"
            # with open(debug_filename, "w", encoding="utf-8") as f:
            #     f.write(html_content)
            # ------------------------------------------

            soup = BeautifulSoup(html_content, 'html.parser')
            
            # O StatusInvest costuma colocar os indicadores em blocos com 'title'
            # Ex: <div title="Dividend Yield"> ... <strong class="value">10%</strong> </div>
            
            # Vamos iterar sobre o MAPA de indicadores
            for nome_indicador, chave_json in STATUSINVEST_INDICATORS_MAP.items():
                try:
                    # Tenta encontrar pelo título do indicador (estratégia comum no StatusInvest)
                    # Procura qualquer elemento que contenha o texto do indicador
                    elementos = soup.find_all(string=lambda text: text and nome_indicador in text)
                    
                    valor_encontrado = None
                    
                    for elem in elementos:
                        # Tenta achar o valor próximo (geralmente num strong com class 'value')
                        # Sobe para o pai (div ou container) e procura a classe value
                        parent = elem.parent
                        while parent and parent.name != 'body':
                            valor_tag = parent.find(class_='value')
                            if valor_tag:
                                valor_encontrado = valor_tag.get_text(strip=True)
                                break
                            parent = parent.parent
                        
                        if valor_encontrado:
                            break
                    
                    # Se achou algo, processa
                    if valor_encontrado:
                        self._process_and_store_data(dados, chave_json, valor_encontrado)
                    
                    # TENTATIVA EXTRA: Para cotação atual (que as vezes tem estrutura diferente)
                    if chave_json == "statusInvest_cotacao" and dados[chave_json] is None:
                        cotacao_elem = soup.find("div", title="Valor atual")
                        if cotacao_elem:
                            val = cotacao_elem.find("strong", class_="value")
                            if val:
                                self._process_and_store_data(dados, chave_json, val.get_text())

                except Exception as e:
                    # Apenas ignora erros de parsing individuais para não travar tudo
                    continue

            print(f"Extração para {self.ticker} concluída.")
            return dados
            
        except requests.exceptions.HTTPError as e:
            dados["erro_statusinvest"] = f"Erro API: {e.response.status_code}"
            return dados
        except Exception as e:
            print(f"Erro inesperado: {e}")
            dados["erro_statusinvest"] = f"Erro scraper: {e}"
            return dados