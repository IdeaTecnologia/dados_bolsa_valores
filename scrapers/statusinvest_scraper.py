# statusinvest_scraper.py

import time
import os
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils.normalization import normalize_numeric_value

# ... (constantes STATUSINVEST_INDICATORS_MAP e NON_NUMERIC_KEYS permanecem as mesmas)

class StatusInvestScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://statusinvest.com.br/acoes/{self.ticker.lower()}"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"

    # ... (_get_all_possible_keys e _process_and_store_data permanecem os mesmos)

    def fetch_data(self):
        all_keys = self._get_all_possible_keys()
        dados = {key: None for key in all_keys}
        dados["ticker"] = self.ticker
        dados["erro_statusinvest"] = ""

        html_content = None
        max_tentativas = 3
        ultimo_erro = ""

        for tentativa in range(1, max_tentativas + 1):
            browser = None # Inicializa fora do try para garantir que possamos fechá-lo no finally
            try:
                print(f"Tentativa {tentativa} para {self.ticker} no StatusInvest usando Playwright...")
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent=self.user_agent,
                        # Emula um viewport de desktop comum
                        viewport={'width': 1920, 'height': 1080}
                    )
                    page = context.new_page()
                    
                    # --- MELHORIA 1: AUMENTAR TIMEOUTS E MUDAR ESTRATÉGIA DE ESPERA ---
                    # Aumentamos o timeout geral de navegação para 90 segundos
                    page.goto(self.url, timeout=90000)
                    
                    # Em vez de esperar por um seletor, esperamos a rede ficar ociosa.
                    # Isso é mais robusto, pois aguarda o fim das requisições de fundo (JS, etc).
                    print(f"Página de {self.ticker} navegada, aguardando carregamento completo...")
                    page.wait_for_load_state('networkidle', timeout=60000)
                    
                    html_content = page.content()
                    browser.close()
                
                break # Sai do loop se teve sucesso

            except PlaywrightTimeoutError as e:
                ultimo_erro = f"TimeoutError: {str(e)}"
                print(f"Tentativa {tentativa} falhou para {self.ticker}: {ultimo_erro}")
                
                # --- MELHORIA 2: DEBUG COM SCREENSHOT ---
                if 'page' in locals() and not page.is_closed():
                    screenshot_path = f"error_screenshot_{self.ticker}_tentativa_{tentativa}.png"
                    page.screenshot(path=screenshot_path, full_page=True)
                    print(f"!!! Screenshot do erro salvo em: {screenshot_path} !!!")

            except Exception as e:
                ultimo_erro = str(e)
                print(f"Tentativa {tentativa} falhou para {self.ticker}: {ultimo_erro}")
            
            finally:
                if browser and browser.is_connected():
                    browser.close()

            time.sleep(5) # Aumenta o intervalo entre tentativas para 5 segundos

        if not html_content:
            dados["erro_statusinvest"] = f"Status Invest: Falha ao carregar a página após {max_tentativas} tentativas: {ultimo_erro}"
            return dados

        try:
            # A partir daqui, a lógica de parsing continua a mesma
            soup = BeautifulSoup(html_content, 'html.parser')
            # ... (cole aqui sua lógica de parsing com BeautifulSoup)
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
            # ... (resto da lógica)

        except Exception as e:
            dados["erro_statusinvest"] = f"Status Invest: Página carregada, mas falha ao extrair dados: {e}"
            print(dados["erro_statusinvest"])

        return dados