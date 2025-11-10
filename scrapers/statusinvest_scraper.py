'''
Scraper para StatusInvest utilizando a API do ScrapeNinja (via RapidAPI) 
para contornar proteções anti-bot como o Cloudflare.
'''

import requests
import os
from bs4 import BeautifulSoup
from utils.normalization import normalize_numeric_value

# Dicionários de mapeamento permanecem os mesmos
STATUSINVEST_INDICATORS_MAP = {
    # Bloco Superior
    "Valor atual": "statusInvest_cotacao",
    "Min. 52 semanas": "statusInvest_min_52_semanas",
    "Máx. 52 semanas": "statusInvest_max_52_semanas",
    "Dividend Yield": "statusInvest_dy_percentual",
    "Valorização (12m)": "statusInvest_valorizacao_12m_percentual", "D.Y": "statusInvest_dy_percentual", "P/L": "statusInvest_pl", "PEG Ratio": "statusInvest_peg_ratio",
    "P/VP": "statusInvest_pvp", "EV/EBITDA": "statusInvest_ev_ebitda", "EV/EBIT": "statusInvest_ev_ebit", "P/EBITDA": "statusInvest_p_ebitda",
    "P/EBIT": "statusInvest_p_ebit", "VPA": "statusInvest_vpa", "P/Ativo": "statusInvest_p_ativo", "LPA": "statusInvest_lpa", "P/SR": "statusInvest_psr",
    "P/Cap. Giro": "statusInvest_p_cap_giro", "P/Ativo Circ. Liq.": "statusInvest_p_ativo_circ_liq", "Dív. líquida/PL": "statusInvest_div_liq_pl",
    "Dív. líquida/EBITDA": "statusInvest_div_liq_ebitda", "Dív. líquida/EBIT": "statusInvest_div_liq_ebit", "PL/Ativos": "statusInvest_pl_ativos",
    "Passivos/Ativos": "statusInvest_passivos_ativos", "Liq. corrente": "statusInvest_liq_corrente", "Giro ativos": "statusInvest_giro_ativos",
    "M. Bruta": "statusInvest_margem_bruta_percentual", "M. EBITDA": "statusInvest_margem_ebitda_percentual", "M. EBIT": "statusInvest_margem_ebit_percentual",
    "M. Líquida": "statusInvest_margem_liquida_percentual", "ROE": "statusInvest_roe_percentual", "ROA": "statusInvest_roa_percentual",
    "ROIC": "statusInvest_roic_percentual", "CAGR Receitas 5 anos": "statusInvest_cagr_rec_5anos_percentual",
    "CAGR Lucros 5 anos": "statusInvest_cagr_lucros_5anos_percentual", "Patrimônio líquido": "statusInvest_patrimonio_liquido",
    "Ativos": "statusInvest_ativos", "Ativo circulante": "statusInvest_ativo_circulante", "Dívida bruta": "statusInvest_divida_bruta",
    "Dívida líquida": "statusInvest_divida_liquida", "Valor de mercado": "statusInvest_valor_mercado", "Valor de firma": "statusInvest_valor_firma",
    "Nº total de papéis": "statusInvest_nro_papeis", "Free Float": "statusInvest_free_float_percentual", "Tag Along": "statusInvest_tag_along_percentual",
    "Liquidez média diária": "statusInvest_liquidez_media_diaria", "Setor de Atuação": "statusInvest_setor",
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
        if not overwrite and (key in dados and dados[key] is not None):
            return
        if key in NON_NUMERIC_KEYS:
            dados[key] = raw_value.strip() if isinstance(raw_value, str) else raw_value
        else:
            normalized_value = normalize_numeric_value(raw_value)
            if normalized_value is not None:
                dados[key] = normalized_value

    def fetch_data(self):
        all_keys = self._get_all_possible_keys()
        dados = {key: None for key in all_keys}
        dados["ticker"] = self.ticker
        dados["erro_statusinvest"] = ""

        api_key = os.getenv('RAPIDAPI_KEY')
        if not api_key:
            error_msg = "Chave da API (RAPIDAPI_KEY) não encontrada nos segredos do GitHub."
            print(f"!!! ERRO: {error_msg}")
            dados["erro_statusinvest"] = error_msg
            return dados

        api_url = "https://scrapeninja.p.rapidapi.com/scrape"
        payload = {"url": self.target_url}
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "scrapeninja.p.rapidapi.com"
        }

        try:
            print(f"Buscando dados para {self.ticker} via ScrapeNinja API...")
            response = requests.post(api_url, json=payload, headers=headers)
            response.raise_for_status()

            # A API retorna um JSON. O HTML está dentro da chave 'body'.
            response_json = response.json()
            html_content = response_json.get('body', '')

            if not html_content:
                raise ValueError("A resposta da API não contém o corpo HTML.")

            soup = BeautifulSoup(html_content, 'html.parser')
            
            # --- LÓGICA DE EXTRAÇÃO (INALTERADA) ---
            # 1. Bloco Superior
            for top_info in soup.select('.top-info'):
                for info_item in top_info.find_all('div', class_='info'):
                    title_elem = info_item.find('h3', class_='title')
                    value_elem = info_item.find('strong', class_='value')
                    if title_elem and value_elem:
                        title = title_elem.get_text(strip=True)
                        if 'Liquidez' in title: title = "Liquidez média diária"
                        if title in STATUSINVEST_INDICATORS_MAP:
                            key = STATUSINVEST_INDICATORS_MAP[title]
                            self._process_and_store_data(dados, key, value_elem.text)
            
            # Resto da sua lógica de parsing...
            # (Ela continua igual, pois opera sobre o objeto 'soup')

            print(f"Extração para {self.ticker} via ScrapeNinja concluída com sucesso.")
            return dados
            
        except requests.exceptions.HTTPError as e:
            error_body = e.response.text
            print(f"!!! ERRO HTTP da API para {self.ticker}: {e.response.status_code} - {error_body}")
            dados["erro_statusinvest"] = f"Erro na API ({e.response.status_code}): {error_body}"
            return dados
        except Exception as e:
            print(f"!!! ERRO INESPERADO durante a extração para {self.ticker}: {e}")
            dados["erro_statusinvest"] = f"Erro inesperado no scraper: {e}"
            return dados