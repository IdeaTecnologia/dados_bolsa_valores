from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from utils.normalization import normalize_numeric_value

INVESTIDOR10_INDICATORS_MAP = {
    # Indicadores Numéricos
    "P/L": "investidor10_pl",
    "P/VP": "investidor10_pvp",
    "P/RECEITA (PSR)": "investidor10_psr",
    "EV/EBITDA": "investidor10_ev_ebitda",
    "EV/EBIT": "investidor10_ev_ebit",
    "P/EBITDA": "investidor10_p_ebitda",
    "P/EBIT": "investidor10_p_ebit",
    "VPA": "investidor10_vpa",
    "LPA": "investidor10_lpa",
    "DÍVIDA LÍQUIDA / PATRIMÔNIO": "investidor10_divida_liquida_patrimonio",
    "DÍVIDA LÍQUIDA / EBITDA": "investidor10_divida_liquida_ebitda",
    "DÍVIDA LÍQUIDA / EBIT": "investidor10_divida_liquida_ebit",
    "LIQUIDEZ CORRENTE": "investidor10_liquidez_corrente",
    "PATRIMÔNIO / ATIVOS": "investidor10_patrimonio_ativos",
    "PASSIVOS / ATIVOS": "investidor10_passivos_ativos",
    "GIRO ATIVOS": "investidor10_giro_ativos",
    "VALOR DE MERCADO": "investidor10_valor_mercado",
    "VALOR DE FIRMA": "investidor10_valor_firma",
    "PATRIMÔNIO LÍQUIDO": "investidor10_patrimonio_liquido",
    "Nº TOTAL DE PAPEIS": "investidor10_nro_total_papeis",
    "ATIVOS": "investidor10_ativos",
    "ATIVO CIRCULANTE": "investidor10_ativo_circulante",
    "DÍVIDA BRUTA": "investidor10_divida_bruta",
    "DÍVIDA LÍQUIDA": "investidor10_divida_liquida",
    "DISPONIBILIDADE": "investidor10_disponibilidade",
    "LIQUIDEZ MÉDIA DIÁRIA": "investidor10_liquidez_media_diaria",
    "NÚMERO DE FUNCIONÁRIOS": "investidor10_nro_funcionarios",

    # Indicadores Percentuais
    "DIVIDEND YIELD": "investidor10_dy_percentual",
    "PAYOUT": "investidor10_payout_percentual",
    "MARGEM LÍQUIDA": "investidor10_margem_liquida_percentual",
    "MARGEM BRUTA": "investidor10_margem_bruta_percentual",
    "MARGEM EBIT": "investidor10_margem_ebit_percentual",
    "MARGEM EBITDA": "investidor10_margem_ebitda_percentual",
    "ROE": "investidor10_roe_percentual",
    "ROIC": "investidor10_roic_percentual",
    "ROA": "investidor10_roa_percentual",
    "CAGR RECEITAS 5 ANOS": "investidor10_cagr_receitas_5anos_percentual",
    "CAGR LUCROS 5 ANOS": "investidor10_cagr_lucros_5anos_percentual",
    "FREE FLOAT": "investidor10_free_float_percentual",
    "TAG ALONG": "investidor10_tag_along_percentual",

    # Dados de Texto
    "SEGMENTO DE LISTAGEM": "investidor10_segmento_listagem",
    "SETOR": "investidor10_setor",
    "SEGMENTO": "investidor10_segmento",
    "NOME DA EMPRESA": "investidor10_nome_empresa",
    "CNPJ": "investidor10_cnpj",
    "ANO DE ESTREIA NA BOLSA": "investidor10_ano_estreia_bolsa",
    "ANO DE FUNDAÇÃO": "investidor10_ano_fundacao"
}

NON_NUMERIC_KEYS = {
    "investidor10_segmento_listagem", "investidor10_setor", "investidor10_segmento",
    "investidor10_nome_empresa", "investidor10_cnpj", "investidor10_ano_estreia_bolsa",
    "investidor10_ano_fundacao"
}


class Investidor10Scraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://investidor10.com.br/acoes/{self.ticker.lower()}/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def _process_and_store_data(self, dados, key, raw_value):
        """Função auxiliar para normalizar e armazenar dados."""
        if key in NON_NUMERIC_KEYS:
            dados[key] = raw_value
        else:
            normalized_value = normalize_numeric_value(raw_value)
            if normalized_value is not None:
                dados[key] = normalized_value

    def fetch_data(self):
        dados = {"ticker": self.ticker}
        
        try:
            response = curl_requests.get(self.url, headers=self.headers, impersonate="chrome110", timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 1. Extrai cotação
            if cotacao_div := soup.find("div", class_="_card cotacao"):
                if value_span := cotacao_div.find("span", class_="value"):
                    raw_value = value_span.get_text(strip=True)
                    self._process_and_store_data(dados, "investidor10_cotacao", raw_value)

            # 2. Extrai variação 12 meses
            for card in soup.find_all("div", class_="_card"):
                header = card.find("div", class_="_card-header")
                if header and "VARIAÇÃO (12M)" in header.get_text(strip=True).upper():
                    if card_body := card.find("div", class_="_card-body"):
                        if span := card_body.find("span"):
                            raw_value = span.get_text(strip=True)
                            self._process_and_store_data(dados, "investidor10_variacao_12m_percentual", raw_value)
                            break
            
            # 3. Extrai indicadores da seção principal
            if indicators_section := soup.find("div", id="indicators"):
                for cell in indicators_section.find_all("div", class_="cell"):
                    title_element = cell.find('span') or cell.find('div', class_='title')
                    value_element = cell.select_one('.value span')

                    if title_element and value_element:
                        title_text = title_element.get_text(strip=True).upper()
                        raw_value = value_element.get_text(strip=True)
                        
                        if "DIVIDEND YIELD" in title_text: title_text = "DIVIDEND YIELD"
                        
                        if title_text in INVESTIDOR10_INDICATORS_MAP:
                            key = INVESTIDOR10_INDICATORS_MAP[title_text]
                            self._process_and_store_data(dados, key, raw_value)

            # 4. Extrai dados da seção "Sobre a Empresa"
            if about_section := soup.find("div", id="about-company"):
                
                # 4.1. Tabela de Informações Básicas (CNPJ, Fundação, etc.)
                if basic_info_table := about_section.find("div", class_="basic_info"):
                    for row in basic_info_table.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) == 2:
                            title_text = cells[0].get_text(strip=True).upper().replace(':', '')
                            raw_value = cells[1].get_text(strip=True)
                            if title_text in INVESTIDOR10_INDICATORS_MAP:
                                key = INVESTIDOR10_INDICATORS_MAP[title_text]
                                self._process_and_store_data(dados, key, raw_value)

                # 4.2. Tabela de Valores (Valor de Firma, Ativos, Free Float, etc.)
                if info_table := about_section.find("div", id="table-indicators-company"):
                    for cell in info_table.find_all("div", class_="cell"):
                        title_element = cell.find('span', class_='title')
                        value_element = cell.find('span', class_='value')
                        
                        if title_element and value_element:
                            title_text = title_element.get_text(strip=True).upper()
                            
                            value_simple = value_element.find('div', class_='simple-value')
                            raw_value = value_simple.get_text(strip=True) if value_simple else value_element.get_text(strip=True)
                            
                            if title_text in INVESTIDOR10_INDICATORS_MAP:
                                key = INVESTIDOR10_INDICATORS_MAP[title_text]
                                self._process_and_store_data(dados, key, raw_value)
            return dados
            
        except Exception as e:
            print(f"Erro ao buscar dados de {self.ticker} no Investidor10: {e}")
            dados["erro_investidor10"] = f"Investidor10: {str(e)}"
            return dados