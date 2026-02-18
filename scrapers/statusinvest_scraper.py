import re
import requests
import os
import html
from datetime import datetime
from bs4 import BeautifulSoup
from utils.normalization import normalize_numeric_value
from dotenv import load_dotenv
import pytz

load_dotenv()

# --- MAPA DE INDICADORES ---
STATUSINVEST_INDICATORS_MAP = {
    "Valor atual": "statusInvest_cotacao", "Min. 52 semanas": "statusInvest_min_52_semanas", "Máx. 52 semanas": "statusInvest_max_52_semanas",
    "Dividend Yield": "statusInvest_dy_percentual", "Valorização (12m)": "statusInvest_valorizacao_12m_percentual", "D.Y": "statusInvest_dy_percentual",
    "P/L": "statusInvest_pl", "PEG Ratio": "statusInvest_peg_ratio", "P/VP": "statusInvest_pvp", "EV/EBITDA": "statusInvest_ev_ebitda", "EV/EBIT": "statusInvest_ev_ebit",
    "P/EBITDA": "statusInvest_p_ebitda", "P/EBIT": "statusInvest_p_ebit", "VPA": "statusInvest_vpa", "P/Ativo": "statusInvest_p_ativo", "LPA": "statusInvest_lpa",
    "P/SR": "statusInvest_psr", "P/Cap. Giro": "statusInvest_p_cap_giro", "P/Ativo Circ. Liq.": "statusInvest_p_ativo_circ_liq", 
    "Dív. líquida/PL": "statusInvest_div_liq_pl", "D&#xED;v. l&#xED;quida/PL": "statusInvest_div_liq_pl",  # Versão com HTML entities
    "Dív. líquida/EBITDA": "statusInvest_div_liq_ebitda", "D&#xED;v. l&#xED;quida/EBITDA": "statusInvest_div_liq_ebitda",  # Versão com HTML entities
    "Dív. líquida/EBIT": "statusInvest_div_liq_ebit", "D&#xED;v. l&#xED;quida/EBIT": "statusInvest_div_liq_ebit",  # Versão com HTML entities
    "PL/Ativos": "statusInvest_pl_ativos",
    "Passivos/Ativos": "statusInvest_passivos_ativos", "Liq. corrente": "statusInvest_liq_corrente", "Giro ativos": "statusInvest_giro_ativos",
    "M. Bruta": "statusInvest_margem_bruta_percentual", "M. EBITDA": "statusInvest_margem_ebitda_percentual", "M. EBIT": "statusInvest_margem_ebit_percentual",
    "M. Líquida": "statusInvest_margem_liquida_percentual", "M. L&#xED;quida": "statusInvest_margem_liquida_percentual",  # Versão com HTML entities
    "ROE": "statusInvest_roe_percentual", "ROA": "statusInvest_roa_percentual",
    "ROIC": "statusInvest_roic_percentual", "CAGR Receitas 5 anos": "statusInvest_cagr_rec_5anos_percentual", "CAGR Lucros 5 anos": "statusInvest_cagr_lucros_5anos_percentual",
    "Patrimônio líquido": "statusInvest_patrimonio_liquido", "Ativos": "statusInvest_ativos", "Ativo circulante": "statusInvest_ativo_circulante",
    "Dívida bruta": "statusInvest_divida_bruta", "Dívida líquida": "statusInvest_divida_liquida", "Valor de mercado": "statusInvest_valor_mercado",
    "Valor de firma": "statusInvest_valor_firma", "Nº total de papéis": "statusInvest_nro_papeis", "Free Float": "statusInvest_free_float_percentual",
    "Tag Along": "statusInvest_tag_along_percentual", "Liquidez média diária": "statusInvest_liquidez_media_diaria", "Setor de Atuação": "statusInvest_setor",
    "Subsetor de Atuação": "statusInvest_subsetor", "Segmento de Atuação": "statusInvest_segmento",
    "Data Início Recompra": "statusInvest_recompra_inicio", 
    "Data Fim Recompra": "statusInvest_recompra_fim", 
    "Quantidade Recompra": "statusInvest_recompra_quantidade",
}

NON_NUMERIC_KEYS = {
    "statusInvest_setor", "statusInvest_subsetor", "statusInvest_segmento",
    "statusInvest_recompra_inicio", "statusInvest_recompra_fim"
}

class StatusInvestScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.target_url = f"https://statusinvest.com.br/acoes/{self.ticker.lower()}"

    def _get_all_possible_keys(self):
        keys = list(STATUSINVEST_INDICATORS_MAP.values())
        keys.append("statusInvest_data_atualizacao")
        keys.append("statusInvest_fonte")
        return keys

    def _process_and_store_data(self, dados, key, raw_value, overwrite=True):
        if not overwrite and (key in dados and dados[key] is not None): 
            return
        
        # Decodificar HTML entities (ex: &#xE1; -> á)
        if isinstance(raw_value, str):
            raw_value = html.unescape(raw_value)
        
        if key in NON_NUMERIC_KEYS:
            dados[key] = raw_value.strip() if isinstance(raw_value, str) else raw_value
        else:
            normalized_value = normalize_numeric_value(raw_value)
            if normalized_value is not None: 
                dados[key] = normalized_value

    def _buscar_valor_indicador(self, html_content, nome_indicador):
        """
        MÉTODO CORRIGIDO: Busca robusta para múltiplos formatos do StatusInvest
        """
        
        # ESTRATÉGIA 1: <h3 class="title">Nome</h3> ... <strong class="value">VALOR</strong>
        # Usado para: Ativos, Dívida Bruta, Patrimônio Líquido, etc.
        pattern1 = rf'<h3[^>]*class="[^"]*title[^"]*"[^>]*>{re.escape(nome_indicador)}</h3>.*?<strong[^>]*class="[^"]*value[^"]*"[^>]*>([^<]+)</strong>'
        match = re.search(pattern1, html_content, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # ESTRATÉGIA 2: <h3 class="title uppercase">NOME</h3> ... <strong>VALOR</strong>
        pattern2 = rf'<h3[^>]*class="[^"]*title[^"]*uppercase[^"]*"[^>]*>{re.escape(nome_indicador)}</h3>.*?<strong[^>]*>([^<]+)</strong>'
        match = re.search(pattern2, html_content, re.IGNORECASE | re.DOTALL)
        if match:
            valor = match.group(1).strip()
            if valor and not valor.startswith('<'):
                return valor
        
        # ESTRATÉGIA 3: Para indicadores em formato de tabela/grid (P/L, ROE, etc.)
        pattern3 = rf'<div[^>]*title="[^"]*{re.escape(nome_indicador)}[^"]*"[^>]*>.*?<strong[^>]*>([^<]+)</strong>'
        match = re.search(pattern3, html_content, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # ESTRATÉGIA 4: Link com termo-indicator
        pattern4 = rf'<h3[^>]*class="[^"]*title[^"]*"[^>]*>{re.escape(nome_indicador)}</h3>.*?<span[^>]*class="[^"]*material-icons[^"]*"[^>]*>.*?</span>.*?</a>.*?<strong[^>]*>([^<]+)</strong>'
        match = re.search(pattern4, html_content, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # ESTRATÉGIA 5: sub-value (para Tag Along, Free Float, etc.)
        pattern5 = rf'<span[^>]*class="[^"]*sub-value[^"]*"[^>]*>.*?{re.escape(nome_indicador)}.*?</span>.*?<strong[^>]*>([^<]+)</strong>'
        match = re.search(pattern5, html_content, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        
        # ESTRATÉGIA 6: Busca contextual ampla
        title_pattern = rf'>{re.escape(nome_indicador)}<'
        for match_title in re.finditer(title_pattern, html_content, re.IGNORECASE):
            start_pos = match_title.end()
            end_pos = min(len(html_content), start_pos + 800)
            contexto = html_content[start_pos:end_pos]
            
            # Evita tooltips
            if 'data-tooltip' in html_content[max(0, match_title.start()-200):match_title.start()]:
                continue
                
            # Procura strong com valor
            value_match = re.search(r'<strong[^>]*>([^<]+)</strong>', contexto)
            if value_match:
                valor_candidato = value_match.group(1).strip()
                # Validar se parece um valor
                if valor_candidato and (
                    re.match(r'^[\d.,\-\s%]+$', valor_candidato) or
                    re.match(r'^\d{2}/\d{2}/\d{4}$', valor_candidato) or
                    valor_candidato in ['ON', 'PN', 'UNIT', '--'] or
                    re.match(r'^[A-ZÀ-Ú\s\.]+$', valor_candidato, re.IGNORECASE)
                ):
                    return valor_candidato
        
        return None

    def _extrair_dados_recompra(self, soup):
        dados_recompra = {}
        try:
            div_programa_recompra = soup.find('div', class_='buyback card')
            if not div_programa_recompra: 
                return dados_recompra
            div_primeira_linha = div_programa_recompra.find('div', class_='line')
            if div_primeira_linha:
                status_span = div_primeira_linha.find('span', class_='badge')
                if status_span and 'ativo' in status_span.get_text(strip=True).lower():
                    mapa_interno = {
                        "DATA DE INÍCIO": "statusInvest_recompra_inicio",
                        "DATA DE FIM": "statusInvest_recompra_fim",
                        "QUANTIDADE": "statusInvest_recompra_quantidade"
                    }
                    spans_info = div_primeira_linha.find_all('span', class_=['fs-2', 'fw-700', 'fs-4'])
                    chave_atual = None
                    for span in spans_info:
                        classes = span.get('class', [])
                        texto = span.get_text(strip=True)
                        if 'fs-2' in classes: 
                            chave_atual = texto.upper()
                        elif chave_atual and ('fw-700' in classes or 'fs-4' in classes):
                            if chave_atual in mapa_interno: 
                                dados_recompra[mapa_interno[chave_atual]] = texto
                            chave_atual = None
        except Exception as e:
            print(f"    [AVISO] Erro extração recompra {self.ticker}: {e}")
        return dados_recompra

    def fetch_data(self, use_local_strategy=False):
        """
        :param use_local_strategy: Se True, usa requests local direto (rápido).
                                   Se False, usa API ScrapeNinja (remoto).
        """
        if use_local_strategy:
            return self._fetch_local_requests()
        else:
            return self._fetch_api_scrapeninja()

    def _fetch_local_requests(self):
        """
        Estratégia Local Rápida: Requests com Headers específicos.
        """
        dados = {key: None for key in self._get_all_possible_keys()}
        dados["ticker"] = self.ticker
        dados["statusInvest_erro"] = ""

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'close'
        }

        print(f"  > [LOCAL] Request direto para {self.ticker}...", end="", flush=True)
        try:
            response = requests.get(self.target_url, headers=headers, timeout=10)
            
            if response.status_code in [403, 429]:
                print(" ❌ Bloqueado (403/429).")
                dados["statusInvest_erro"] = "Blocked"
                return dados
            
            if response.status_code != 200:
                print(f" ❌ Erro HTTP {response.status_code}")
                dados["statusInvest_erro"] = f"HTTP {response.status_code}"
                return dados

            html_content = response.text
            print(" ✅ Sucesso!")
            return self._parse_html(html_content, dados, fonte="Atualização Manual Local")

        except Exception as e:
            print(f" ❌ Erro: {e}")
            dados["statusInvest_erro"] = str(e)
            return dados

    def _fetch_api_scrapeninja(self):
        dados = {key: None for key in self._get_all_possible_keys()}
        dados["ticker"] = self.ticker
        dados["statusInvest_erro"] = ""

        api_keys_str = os.getenv('RAPIDAPI_KEYS')
        if not api_keys_str: 
            api_keys_str = os.getenv('RAPIDAPI_KEY', '')
        if not api_keys_str:
            dados["statusInvest_erro"] = "Sem Chaves API"
            return dados

        api_keys_list = [k.strip() for k in api_keys_str.split(',') if k.strip()]
        api_url = 'https://scrapeninja.p.rapidapi.com/scrape'
        
        sucesso = False
        html_content = ""
        chave_usada = ""

        for i, api_key in enumerate(api_keys_list):
            key_masked = f"...{api_key[-6:]}"
            headers = {
                "Content-Type": "application/json", 
                "x-rapidapi-key": api_key, 
                "x-rapidapi-host": "scrapeninja.p.rapidapi.com"
            }
            payload = {
                "url": self.target_url, "retryNum": 1, "geo": "br", "renderJs": True, "wait": 5000,
                "headers": ["User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"]
            }

            try:
                print(f"    -> API {i+1} ({key_masked})... ", end="", flush=True)
                response = requests.post(api_url, json=payload, headers=headers)
                if response.status_code == 429:
                    print("❌ 429.")
                    continue
                if response.status_code == 200 and response.json().get('body'):
                    html_content = response.json().get('body')
                    chave_usada = f"API Ninja ({key_masked})"
                    print("✅")
                    sucesso = True
                    break
            except: 
                pass

        if not sucesso:
            dados["statusInvest_erro"] = "ALL_KEYS_EXHAUSTED"
            return dados

        return self._parse_html(html_content, dados, fonte=chave_usada)

    def _parse_html(self, html_content, dados, fonte):
        """
        MÉTODO CORRIGIDO: Usa o novo método de busca robusto
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Processa cada indicador usando o método corrigido
            for nome_indicador, chave_json in STATUSINVEST_INDICATORS_MAP.items():
                try:
                    valor_encontrado = self._buscar_valor_indicador(html_content, nome_indicador)
                    
                    if valor_encontrado:
                        self._process_and_store_data(dados, chave_json, valor_encontrado)
                    
                except Exception as e_ind:
                    continue

            # Extração de recompra
            dados_recompra = self._extrair_dados_recompra(soup)
            for k, v in dados_recompra.items():
                self._process_and_store_data(dados, k, v, overwrite=True)

            # Define timezone BR
            br_tz = pytz.timezone('America/Sao_Paulo')
            dados["statusInvest_data_atualizacao"] = datetime.now(br_tz).strftime("%Y-%m-%d %H:%M:%S")
            dados["statusInvest_fonte"] = fonte

            return dados
        except Exception as e:
            dados["statusInvest_erro"] = f"Erro parsing: {e}"
            return dados
