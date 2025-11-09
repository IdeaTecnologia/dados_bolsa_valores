'''
Pesquisar por python web scraping cloudflare

Proteções de StatusInvest:
1 Cloudflare Bot Management - Scripts da Cloudflare no cabeçalho
2 Google Tag Manager - Múltiplos scripts de rastreamento
3 Akamai Bot Protection - Scripts de proteção
4 Fingerprinting de navegador - Diversos scripts coletando informações do client
5 Request de origem - Verificação de cabeçalhos HTTP
6 reCAPTCHA Enterprise - Proteção contra bots do Google
'''

import time
from bs4 import BeautifulSoup
from seleniumbase import Driver
from selenium.common.exceptions import TimeoutException # Importar exceção de Timeout
from utils.normalization import normalize_numeric_value

# Dicionários de mapeamento (sem alterações)
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
        self.url = f"https://statusinvest.com.br/acoes/{self.ticker.lower()}"

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
        driver = None

        try:
            print(f"Iniciando scraper para {self.ticker} no StatusInvest...")
            driver = Driver(uc=True, headless=True, block_images=True)
            
            # Adiciona um timeout generoso para o carregamento inicial da página
            driver.set_page_load_timeout(60)

            print(f"Acessando URL: {self.url}")
            driver.get(self.url)

            # Lógica para lidar com o desafio Cloudflare
            try:
                print("Procurando por desafio Cloudflare (timeout de 15s)...")
                iframe_selector = 'iframe[title="Widget containing a Cloudflare security challenge"]'
                driver.wait_for_element(iframe_selector, timeout=15)
                driver.switch_to.frame(iframe_selector)
                driver.click('input[type="checkbox"]')
                print(">>> Desafio Cloudflare encontrado e clicado! Aguardando redirecionamento...")
                driver.switch_to.default_content()
                time.sleep(5) # Espera extra para a página recarregar
            except TimeoutException:
                print("Nenhum desafio Cloudflare detectado, prosseguindo diretamente.")
            
            print("Aguardando conteúdo principal da página (timeout de 30s)...")
            driver.wait_for_element("#indicators-section", timeout=30)
            print(">>> Conteúdo principal encontrado. Iniciando extração.")

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Lógica de extração (inalterada)
            # ... (Toda a sua lógica de parsing com BeautifulSoup continua aqui)
            
            print(f"Extração para {self.ticker} concluída com sucesso.")
            return dados

        except TimeoutException as te:
            print(f"!!! ERRO DE TIMEOUT para {self.ticker}: A página ou um elemento chave não carregou a tempo. {te}")
            dados["erro_statusinvest"] = f"Status Invest: Timeout - A página não carregou a tempo."
            return dados
        except Exception as e:
            print(f"!!! ERRO INESPERADO para {self.ticker}: {e}")
            dados["erro_statusinvest"] = f"Status Invest: Erro inesperado - {str(e)}"
            return dados
        
        finally:
            if driver:
                print(f"Finalizando scraper para {self.ticker}. Salvando artefatos de debug...")
                try:
                    # Garante que os artefatos sejam salvos para análise
                    driver.save_screenshot(f"{self.ticker}_screenshot.png")
                    with open(f"{self.ticker}_pagina.html", "w", encoding='utf-8') as f:
                        f.write(driver.page_source)
                    print("Artefatos de debug salvos com sucesso.")
                except Exception as e:
                    print(f"!!! Falha ao salvar artefatos de debug para {self.ticker}: {e}")
                driver.quit()
                print("Navegador fechado.")