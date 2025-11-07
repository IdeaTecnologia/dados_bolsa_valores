import time
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from utils.normalization import normalize_numeric_value

FUNDAMENTUS_INDICATORS_MAP = {
    # Dados da Empresa (Texto)
    "Papel": "fundamentus_papel",
    "Tipo": "fundamentus_tipo",
    "Empresa": "fundamentus_empresa",
    "Setor": "fundamentus_setor",
    "Subsetor": "fundamentus_subsetor",
    "Data últ cot": "fundamentus_data_ult_cotacao",
    "Últ balanço processado": "fundamentus_data_ult_balanco",

    # Dados Numéricos
    "Cotação": "fundamentus_cotacao",
    "Min 52 sem": "fundamentus_min_52_semanas",
    "Max 52 sem": "fundamentus_max_52_semanas",
    "Vol $ méd (2m)": "fundamentus_volume_medio_2m",
    "Valor de mercado": "fundamentus_valor_mercado",
    "Valor da firma": "fundamentus_valor_firma",
    "Nro. Ações": "fundamentus_nro_acoes",
    "P/L": "fundamentus_pl",
    "P/VP": "fundamentus_pvp",
    "PSR": "fundamentus_psr",
    "P/Ativos": "fundamentus_p_ativos",
    "P/Cap. Giro": "fundamentus_p_cap_giro",
    "P/Ativ Circ Liq": "fundamentus_p_ativ_circ_liq",
    "EV / EBITDA": "fundamentus_ev_ebitda",
    "EV / EBIT": "fundamentus_ev_ebit",
    "P/EBIT": "fundamentus_p_ebit",
    "Liquidez Corr": "fundamentus_liquidez_corr",
    "Div Br/ Patrim": "fundamentus_div_bruta_patrim",
    "Giro Ativos": "fundamentus_giro_ativos",
    "LPA": "fundamentus_lpa",
    "VPA": "fundamentus_vpa",
    "Ativo": "fundamentus_ativo",
    "Dív. Bruta": "fundamentus_divida_bruta",
    "Disponibilidades": "fundamentus_disponibilidades",
    "Dív. Líquida": "fundamentus_divida_liquida",
    "Ativo Circulante": "fundamentus_ativo_circulante",
    "Patrim. Líq": "fundamentus_patrimonio_liquido",
    "Receita Líquida": "fundamentus_receita_liquida_12m",
    "EBIT": "fundamentus_ebit_12m",
    "Lucro Líquido": "fundamentus_lucro_liquido_12m",

    # Percentuais
    "Div. Yield": "fundamentus_dy_percentual",
    "Marg. Bruta": "fundamentus_margem_bruta_percentual",
    "Marg. EBIT": "fundamentus_margem_ebit_percentual",
    "Marg. Líquida": "fundamentus_margem_liquida_percentual",
    "EBIT / Ativo": "fundamentus_ebit_ativo_percentual",
    "ROIC": "fundamentus_roic_percentual",
    "ROE": "fundamentus_roe_percentual",
    "Cres. Rec (5a)": "fundamentus_crescimento_rec_5anos_percentual",
    "Dia": "fundamentus_oscilacao_dia_percentual",
    "Mês": "fundamentus_oscilacao_mes_percentual",
    "30 dias": "fundamentus_oscilacao_30d_percentual",
    "12 meses": "fundamentus_oscilacao_12m_percentual",

    # DRE 3 meses
    "fundamentus_receita_liquida_3m": "fundamentus_receita_liquida_3m",
    "fundamentus_ebit_3m": "fundamentus_ebit_3m",
    "fundamentus_lucro_liquido_3m": "fundamentus_lucro_liquido_3m",
}

# Conjunto de chaves que são intencionalmente não numéricas
NON_NUMERIC_KEYS = {
    "fundamentus_papel", "fundamentus_tipo", "fundamentus_empresa",
    "fundamentus_setor", "fundamentus_subsetor", "fundamentus_data_ult_cotacao",
    "fundamentus_data_ult_balanco"
}


class FundamentusScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://www.fundamentus.com.br/detalhes.php?papel={self.ticker.upper()}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def fetch_data(self):
        dados = {"ticker": self.ticker}
        max_tentativas = 3
            
        for tentativa in range(max_tentativas):
            try:
                time.sleep(1 * (tentativa + 1))
                response = curl_requests.get(self.url, headers=self.headers, impersonate="chrome110", timeout=20)
                if "captcha" in response.text.lower(): raise Exception("Bloqueado por CAPTCHA")
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                ano_atual = datetime.now().year

                # LÓGICA DE EXTRAÇÃO E NORMALIZAÇÃO
                for table in soup.find_all("table", class_="w728"):
                    for row in table.find_all("tr"):
                        cells = row.find_all("td")
                        i = 0
                        while i < len(cells) - 1:
                            label_span = cells[i].find('span', class_='txt')
                            if 'label' in cells[i].get('class', []) and label_span:
                                label_text = label_span.get_text(strip=True)
                                
                                value_cell = cells[i+1]
                                value_element = value_cell.find('span') or value_cell
                                raw_value = (value_element.find('a').get_text(strip=True) if value_element.find('a') 
                                             else value_element.get_text(strip=True)).strip()

                                # --- Início da Lógica de Normalização ---
                                
                                # Processa indicadores principais
                                if label_text in FUNDAMENTUS_INDICATORS_MAP:
                                    key = FUNDAMENTUS_INDICATORS_MAP[label_text]
                                    
                                    # Lógica para não sobrescrever dados do DRE (ex: Receita 12m vs 3m)
                                    # Apenas insere se a chave não existir
                                    if key not in dados:
                                        if key in NON_NUMERIC_KEYS:
                                            dados[key] = raw_value
                                        else:
                                            normalized_value = normalize_numeric_value(raw_value)
                                            if normalized_value is not None:
                                                dados[key] = normalized_value
                                
                                # Processa oscilações anuais
                                if label_text.isdigit():
                                    ano = int(label_text)
                                    diff_ano = ano_atual - ano
                                    
                                    normalized_value = normalize_numeric_value(raw_value)
                                    if normalized_value is not None:
                                        if diff_ano == 0:
                                            dados["fundamentus_oscilacao_ano_atual_percentual"] = normalized_value
                                        elif 1 <= diff_ano <= 5:
                                            dados[f"fundamentus_oscilacao_ano_menos_{diff_ano}_percentual"] = normalized_value
                                
                                # --- Fim da Lógica de Normalização ---
                                
                                i += 2
                                continue
                            i += 1
                
                # Processa dados do DRE (últimos 3 meses)
                dre_header = soup.find('td', class_='nivel1', string='Dados demonstrativos de resultados')
                if dre_header:
                    dre_table = dre_header.find_parent('table')
                    rows = dre_table.find_all('tr')
                    if len(rows) >= 5:
                        data_map = {
                            'fundamentus_receita_liquida_3m': rows[2].find_all('td'),
                            'fundamentus_ebit_3m': rows[3].find_all('td'),
                            'fundamentus_lucro_liquido_3m': rows[4].find_all('td'),
                        }
                        for key, cells in data_map.items():
                            if len(cells) > 3:
                                raw_value = cells[3].get_text(strip=True)
                                normalized_value = normalize_numeric_value(raw_value)
                                if normalized_value is not None:
                                    dados[key] = normalized_value
                
                return dados

            except Exception as e:
                print(f"Tentativa {tentativa+1} para {self.ticker} no Fundamentus falhou: {e}")
                ultimo_erro = str(e)
        
        return {"ticker": self.ticker, "erro_fundamentus": f"Fundamentus: Falha após {max_tentativas} tentativas: {ultimo_erro}"}