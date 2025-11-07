import time
from bs4 import BeautifulSoup
# Remova a importação do curl_cffi e adicione a do cloudscraper
import cloudscraper 
from utils.normalization import normalize_numeric_value

# O MAPA DE INDICADORES E AS CONSTANTES PERMANECEM IGUAIS
STATUSINVEST_INDICATORS_MAP = {
    # ... (seu mapa de indicadores completo aqui, sem alterações)
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
        # Agora, criamos uma instância do scraper. Ele gerencia os headers e o desafio JS.
        # A opção 'browser' ajuda a simular um navegador específico, o que é sempre uma boa prática.
        self.scraper = cloudscraper.create_scraper(
            interpreter='nodejs',  # Usa o motor Node.js nativo (muito mais rápido)
            delay=10,              # Um delay generoso para garantir que o desafio resolva
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            debug=True # Mantenha True para a primeira execução no Actions para ver os logs!
        )

    def _get_all_possible_keys(self):
        """Gera uma lista com todas as chaves de dados possíveis para este scraper."""
        return list(STATUSINVEST_INDICATORS_MAP.values())

    def _process_and_store_data(self, dados, key, raw_value, overwrite=True):
        """Função auxiliar para normalizar e armazenar dados. (Sem alterações)"""
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
        for tentativa in range(1, max_tentativas + 1):
            try:
                # O cloudscraper já tem um delay embutido para a primeira requisição 
                # para resolver o desafio Cloudflare, mas um pequeno delay adicional pode ajudar.
                time.sleep(1 * tentativa)
                
                # AQUI ESTÁ A MUDANÇA PRINCIPAL:
                # Usamos o objeto scraper em vez de curl_requests.
                # Não precisamos mais passar 'headers' ou 'impersonate',
                # pois o scraper gerencia isso automaticamente.
                response = self.scraper.get(self.url, timeout=20)
                
                print(f"Tentativa {tentativa} para {self.ticker}: Status Code {response.status_code}")
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # TODA A LÓGICA DE EXTRAÇÃO COM BEAUTIFULSOUP PERMANECE IDÊNTICA.
                # Ela já é robusta e bem escrita.
                
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
                    # Ajustei um pouco a lógica para ser mais resiliente caso a estrutura mude
                    for info_item in soup.select('.top-info.sm .info'):
                        title_elem = info_item.find('h3', class_='title')
                        value_elem = info_item.find('strong', class_='value')
                        
                        if title_elem and value_elem:
                            title = title_elem.get_text(strip=True).replace('PARTICIPAÇÃO NO', '').strip()
                            if title in STATUSINVEST_INDICATORS_MAP:
                                key = STATUSINVEST_INDICATORS_MAP[title]
                                self._process_and_store_data(dados, key, value_elem.text)
                
                # Lógica para Setor, Subsetor e Segmento (parece estar faltando no seu código original)
                atuacao_items = soup.select('.company-description-container .info-value')
                if len(atuacao_items) >= 3:
                    self._process_and_store_data(dados, 'statusInvest_setor', atuacao_items[0].text)
                    self._process_and_store_data(dados, 'statusInvest_subsetor', atuacao_items[1].text)
                    self._process_and_store_data(dados, 'statusInvest_segmento', atuacao_items[2].text)

                # Se chegou até aqui, a extração foi bem-sucedida.
                return dados

            except Exception as e:
                print(f"Tentativa {tentativa} para {self.ticker} no StatusInvest falhou: {e}")
                ultimo_erro = str(e)
        
        # Se o loop terminar sem sucesso, preenche a mensagem de erro.
        dados["erro_statusinvest"] = f"Status Invest: Falha após {max_tentativas} tentativas: {ultimo_erro}"
        return dados