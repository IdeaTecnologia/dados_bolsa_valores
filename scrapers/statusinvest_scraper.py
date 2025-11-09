import time
from bs4 import BeautifulSoup
from seleniumbase import Driver
from utils.normalization import normalize_numeric_value

STATUSINVEST_INDICATORS_MAP = {
    # Bloco Superior
    "Valor atual": "statusInvest_cotacao",
    "Min. 52 semanas": "statusInvest_min_52_semanas",
    "Máx. 52 semanas": "statusInvest_max_52_semanas",
    "Dividend Yield": "statusInvest_dy_percentual",
    "Valorização (12m)": "statusInvest_valorizacao_12m_percentual",
    
    # Seção de Indicadores Principais
    "D.Y": "statusInvest_dy_percentual",
    "P/L": "statusInvest_pl",
    "PEG Ratio": "statusInvest_peg_ratio",
    "P/VP": "statusInvest_pvp",
    "EV/EBITDA": "statusInvest_ev_ebitda",
    "EV/EBIT": "statusInvest_ev_ebit",
    "P/EBITDA": "statusInvest_p_ebitda",
    "P/EBIT": "statusInvest_p_ebit",
    "VPA": "statusInvest_vpa",
    "P/Ativo": "statusInvest_p_ativo",
    "LPA": "statusInvest_lpa",
    "P/SR": "statusInvest_psr",
    "P/Cap. Giro": "statusInvest_p_cap_giro",
    "P/Ativo Circ. Liq.": "statusInvest_p_ativo_circ_liq",
    "Dív. líquida/PL": "statusInvest_div_liq_pl",
    "Dív. líquida/EBITDA": "statusInvest_div_liq_ebitda",
    "Dív. líquida/EBIT": "statusInvest_div_liq_ebit",
    "PL/Ativos": "statusInvest_pl_ativos",
    "Passivos/Ativos": "statusInvest_passivos_ativos",
    "Liq. corrente": "statusInvest_liq_corrente",
    "Giro ativos": "statusInvest_giro_ativos",
    "M. Bruta": "statusInvest_margem_bruta_percentual",
    "M. EBITDA": "statusInvest_margem_ebitda_percentual",
    "M. EBIT": "statusInvest_margem_ebit_percentual",
    "M. Líquida": "statusInvest_margem_liquida_percentual",
    "ROE": "statusInvest_roe_percentual",
    "ROA": "statusInvest_roa_percentual",
    "ROIC": "statusInvest_roic_percentual",
    "CAGR Receitas 5 anos": "statusInvest_cagr_rec_5anos_percentual",
    "CAGR Lucros 5 anos": "statusInvest_cagr_lucros_5anos_percentual",

    # Seção "Geral" da empresa
    "Patrimônio líquido": "statusInvest_patrimonio_liquido",
    "Ativos": "statusInvest_ativos",
    "Ativo circulante": "statusInvest_ativo_circulante",
    "Dívida bruta": "statusInvest_divida_bruta",
    "Dívida líquida": "statusInvest_divida_liquida",
    "Valor de mercado": "statusInvest_valor_mercado",
    "Valor de firma": "statusInvest_valor_firma",
    "Nº total de papéis": "statusInvest_nro_papeis",
    "Free Float": "statusInvest_free_float_percentual",
    
    # Outras informações
    "Tag Along": "statusInvest_tag_along_percentual",
    "Liquidez média diária": "statusInvest_liquidez_media_diaria",
    "Setor de Atuação": "statusInvest_setor",
    "Subsetor de Atuação": "statusInvest_subsetor",
    "Segmento de Atuação": "statusInvest_segmento",
}

NON_NUMERIC_KEYS = {
    "statusInvest_setor", "statusInvest_subsetor", "statusInvest_segmento",
}

class StatusInvestScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://statusinvest.com.br/acoes/{self.ticker.lower()}"
        # O User-Agent agora será gerenciado pelo SeleniumBase

    def _get_all_possible_keys(self):
        """Gera uma lista com todas as chaves de dados possíveis para este scraper."""
        return list(STATUSINVEST_INDICATORS_MAP.values())

    def _process_and_store_data(self, dados, key, raw_value, overwrite=True):
        """Função auxiliar para normalizar e armazenar dados."""
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

        max_tentativas = 3
        ultimo_erro = ""
        driver = None  # Inicializa o driver fora do loop

        for tentativa in range(1, max_tentativas + 1):
            try:
                # uc=True: Ativa o modo Undetected-Chromedriver para evitar detecção
                # headless=True: Roda o navegador em segundo plano, sem interface gráfica (essencial para o GitHub Actions)
                driver = Driver(uc=True, headless=True)
                
                print(f"Tentativa {tentativa} para {self.ticker} no StatusInvest usando SeleniumBase...")
                driver.get(self.url)
                
                # Aguarda um pouco para o JS da página carregar (se necessário)
                time.sleep(3) 

                # Pega o HTML da página após a execução do JavaScript
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # --- A LÓGICA DE EXTRAÇÃO ABAIXO PERMANECE EXATAMENTE A MESMA ---

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

                # 2. Seção de Indicadores Principais
                if indicators_section := soup.select_one('#indicators-section'):
                    for item in indicators_section.select('.indicator-today-container .item'):
                        title_elem = item.find('h3', class_='title')
                        value_elem = item.find('strong', class_='value')
                        if title_elem and value_elem:
                            title = title_elem.get_text(strip=True)
                            if title in STATUSINVEST_INDICATORS_MAP:
                                key = STATUSINVEST_INDICATORS_MAP[title]
                                overwrite = key != "statusInvest_dy_percentual"
                                self._process_and_store_data(dados, key, value_elem.text, overwrite=overwrite)

                # 3. Informações da Empresa
                if company_section := soup.select_one('#company-section'):
                    for info_div in company_section.select('.top-info .info'):
                        value_elem = info_div.find('strong', class_='value')
                        if not value_elem: continue
                        
                        raw_value = value_elem.get_text(strip=True)
                        title_elem = info_div.select_one('h3.title span') or \
                                     info_div.select_one('a h3.title') or \
                                     info_div.find('h3', class_='title')
                        
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            if title in STATUSINVEST_INDICATORS_MAP:
                                key = STATUSINVEST_INDICATORS_MAP[title]
                                self._process_and_store_data(dados, key, raw_value)
                    
                    # 4. Outras Infos
                    if other_info := company_section.find('div', class_='company-other-info'):
                        if tag_along_div := other_info.find('h3', string=lambda t: t and 'Tag Along' in t):
                            if value_elem := tag_along_div.find_next_sibling('div').find('strong', class_='value'):
                                self._process_and_store_data(dados, 'statusInvest_tag_along_percentual', value_elem.text)
                        
                        if atuacao_div := other_info.find('h3', class_='title', string='Atuação'):
                            if container := atuacao_div.find_next_sibling('div', class_='scroll'):
                                for item in container.find_all('div', class_='item'):
                                    if strong := item.find('strong'):
                                        if a_tag := item.find('a'):
                                            title, value = strong.get_text(strip=True), a_tag.get_text(strip=True)
                                            if title in STATUSINVEST_INDICATORS_MAP:
                                                key = STATUSINVEST_INDICATORS_MAP[title]
                                                self._process_and_store_data(dados, key, value)

                # Se chegou até aqui, a extração foi bem-sucedida.
                return dados

            except Exception as e:
                print(f"Tentativa {tentativa} para {self.ticker} no StatusInvest falhou: {e}")
                ultimo_erro = str(e)
            
            finally:
                # Garante que o navegador seja fechado mesmo se ocorrer um erro
                if driver:
                    driver.quit()
        
        dados["erro_statusinvest"] = f"Status Invest: Falha após {max_tentativas} tentativas: {ultimo_erro}"
        return dados