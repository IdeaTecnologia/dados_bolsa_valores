import time
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from utils.normalization import normalize_numeric_value

INVESTSITE_INDICADORES_MAP = {
    # Dados Básicos (Texto)
    "Empresa": "investsiteindicadores_empresa",
    "Razão Social": "investsiteindicadores_razao_social",
    "Situação Registro": "investsiteindicadores_situacao_registro",
    "Situação Emissor": "investsiteindicadores_situacao_emissor",
    "Segmento de Listagem": "investsiteindicadores_segmento_de_listagem",
    "Atividade": "investsiteindicadores_atividade",
    "Ação": "investsiteindicadores_acao",
    "Data da Cotação": "investsiteindicadores_data_da_cotacao",
    "Tipo de Ação": "investsiteindicadores_tipo_de_acao",
    "Fator de Cotação": "investsiteindicadores_fator_de_cotacao",
    "Último Demonstrativo Financeiro": "investsiteindicadores_ultimo_demonstrativo_financeiro",
    "Setor": "investsiteindicadores_setor",
    "Subsetor": "investsiteindicadores_subsetor",
    "Segmento": "investsiteindicadores_segmento",
    "Participação em Índices": "investsiteindicadores_participacao_em_indices",

    # Dados Numéricos
    "Último Preço de Fechamento": "investsiteindicadores_ultimo_preco_de_fechamento",
    "Volume Financeiro Transacionado": "investsiteindicadores_volume_financeiro_transacionado",
    "Preço/Lucro": "investsiteindicadores_preco_lucro",
    "Preço/VPA": "investsiteindicadores_preco_vpa",
    "Preço/Receita Líquida": "investsiteindicadores_preco_receita_liquida",
    "Preço/FCO": "investsiteindicadores_preco_fco",
    "Preço/FCF": "investsiteindicadores_preco_fcf",
    "Preço/Ativo Total": "investsiteindicadores_preco_ativo_total",
    "Preço/EBIT": "investsiteindicadores_preco_ebit",
    "Preço/Capital Giro": "investsiteindicadores_preco_capital_giro",
    "Preço/NCAV": "investsiteindicadores_preco_ncav",
    "EV/EBIT": "investsiteindicadores_ev_ebit",
    "EV/EBITDA": "investsiteindicadores_ev_ebitda",
    "EV/Receita Líquida": "investsiteindicadores_ev_receita_liquida",
    "EV/FCO": "investsiteindicadores_ev_fco",
    "EV/FCF": "investsiteindicadores_ev_fcf",
    "EV/Ativo Total": "investsiteindicadores_ev_ativo_total",
    "Market Cap Empresa": "investsiteindicadores_market_cap_empresa",
    "Enterprise Value": "investsiteindicadores_enterprise_value",
    "Menor Preço 52 semanas": "investsiteindicadores_menor_preco_52_semanas",
    "Maior Preço 52 semanas": "investsiteindicadores_maior_preco_52_semanas",
    "Volume Diário Médio (3 meses)": "investsiteindicadores_volume_diario_medio_3_meses",
    "Giro do Ativo Inicial": "investsiteindicadores_giro_do_ativo_inicial",
    "Alavancagem Financeira": "investsiteindicadores_alavancagem_financeira",
    "Passivo/Patrimônio Líquido": "investsiteindicadores_passivo_patrimonio_liquido",
    "Dívida Líquida/EBITDA": "investsiteindicadores_divida_liquida_ebitda",
    "Caixa e Equivalentes de Caixa": "investsiteindicadores_caixa_e_equivalentes_de_caixa",
    "Ativo Total": "investsiteindicadores_ativo_total",
    "Dívida de Curto Prazo": "investsiteindicadores_divida_de_curto_prazo",
    "Dívida de Longo Prazo": "investsiteindicadores_divida_de_longo_prazo",
    "Dívida Bruta": "investsiteindicadores_divida_bruta",
    "Dívida Líquida": "investsiteindicadores_divida_liquida",
    "Patrimônio Líquido": "investsiteindicadores_patrimonio_liquido",
    "Valor Patrimonial da Ação": "investsiteindicadores_valor_patrimonial_da_acao",
    "Receita Líquida": "investsiteindicadores_receita_liquida",
    "Resultado Bruto": "investsiteindicadores_resultado_bruto",
    "EBIT": "investsiteindicadores_ebit",
    "Depreciação e Amortização": "investsiteindicadores_depreciacao_e_amortizacao",
    "EBITDA": "investsiteindicadores_ebitda",
    "Lucro Líquido": "investsiteindicadores_lucro_liquido",
    "Lucro/Ação": "investsiteindicadores_lucro_por_acao",
    "Fluxo de Caixa Operacional": "investsiteindicadores_fluxo_de_caixa_operacional",
    "Fluxo de Caixa de Investimentos": "investsiteindicadores_fluxo_de_caixa_de_investimentos",
    "Fluxo de Caixa de Financiamentos": "investsiteindicadores_fluxo_de_caixa_de_financiamentos",
    "Aumento (Redução) de Caixa e Equivalentes": "investsiteindicadores_aumento_reducao_de_caixa_e_equivalentes",
    "CAPEX 3 meses": "investsiteindicadores_capex_3_meses",
    "Fluxo de Caixa Livre 3 meses": "investsiteindicadores_fluxo_de_caixa_livre_3_meses",
    "CAPEX 12 meses": "investsiteindicadores_capex_12_meses",
    "Fluxo de Caixa Livre 12 meses": "investsiteindicadores_fluxo_de_caixa_livre_12_meses",

    # Percentuais
    "Dividend Yield": "investsiteindicadores_dividend_yield_percentual",
    "Variação 2025": "investsiteindicadores_variacao_2025_percentual",
    "Variação 1 ano": "investsiteindicadores_variacao_1_ano_percentual",
    "Variação 2 anos(total)": "investsiteindicadores_variacao_2_anos_total_percentual",
    "Variação 2 anos(anual)": "investsiteindicadores_variacao_2_anos_anual_percentual",
    "Variação 3 anos(total)": "investsiteindicadores_variacao_3_anos_total_percentual",
    "Variação 3 anos(anual)": "investsiteindicadores_variacao_3_anos_anual_percentual",
    "Variação 4 anos(total)": "investsiteindicadores_variacao_4_anos_total_percentual",
    "Variação 4 anos(anual)": "investsiteindicadores_variacao_4_anos_anual_percentual",
    "Variação 5 anos(total)": "investsiteindicadores_variacao_5_anos_total_percentual",
    "Variação 5 anos(anual)": "investsiteindicadores_variacao_5_anos_anual_percentual",
    "Retorno s/ Capital Tangível Inicial": "investsiteindicadores_retorno_s_capital_tangivel_inicial_percentual",
    "Retorno s/ Capital Investido Inicial": "investsiteindicadores_retorno_s_capital_investido_inicial_percentual",
    "Retorno s/ Capital Tangível Inicial Pré-Impostos": "investsiteindicadores_retorno_s_capital_tangivel_inicial_pre_impostos_percentual",
    "Retorno s/ Capital Investido Inicial Pré-Impostos": "investsiteindicadores_retorno_s_capital_investido_inicial_pre_impostos_percentual",
    "Retorno s/ Patrimônio Líquido Inicial": "investsiteindicadores_retorno_s_patrimonio_liquido_inicial_percentual",
    "Retorno s/ Ativo Inicial": "investsiteindicadores_retorno_s_ativo_inicial_percentual",
    "Margem Bruta": "investsiteindicadores_margem_bruta_percentual",
    "Margem Líquida": "investsiteindicadores_margem_liquida_percentual",
    "Margem EBIT": "investsiteindicadores_margem_ebit_percentual",
    "Margem EBITDA": "investsiteindicadores_margem_ebitda_percentual",
}

NON_NUMERIC_KEYS = {
    "investsiteindicadores_empresa", "investsiteindicadores_razao_social", "investsiteindicadores_situacao_registro",
    "investsiteindicadores_situacao_emissor", "investsiteindicadores_segmento_de_listagem", "investsiteindicadores_atividade",
    "investsiteindicadores_acao", "investsiteindicadores_data_da_cotacao", "investsiteindicadores_tipo_de_acao",
    "investsiteindicadores_fator_de_cotacao", "investsiteindicadores_ultimo_demonstrativo_financeiro", "investsiteindicadores_setor",
    "investsiteindicadores_subsetor", "investsiteindicadores_segmento", "investsiteindicadores_participacao_em_indices",
}


class InvestSiteIndicadoresScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://www.investsite.com.br/principais_indicadores.php?cod_negociacao={self.ticker.upper()}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def _get_all_possible_keys(self):
        """Gera uma lista com todas as chaves de dados possíveis para este scraper."""
        return list(INVESTSITE_INDICADORES_MAP.values())

    def fetch_data(self):
        # Inicializa o dicionário com todas as chaves possíveis e valor None.
        all_keys = self._get_all_possible_keys()
        dados = {key: None for key in all_keys}
        dados["ticker"] = self.ticker
        # Garante que o campo de erro sempre exista.
        dados["investsiteindicadores_erro"] = ""

        max_tentativas = 3
        ultimo_erro = ""
        
        for tentativa in range(max_tentativas):
            try:
                time.sleep(1 * (tentativa + 1))
                response = curl_requests.get(self.url, headers=self.headers, impersonate="chrome110", timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                tables = soup.select('table[id^="tabela_resumo_empresa"]')
                if not tables:
                    raise Exception("Nenhuma tabela de indicadores encontrada.")

                # LÓGICA DE EXTRAÇÃO E NORMALIZAÇÃO
                for table in tables:
                    tbody = table.find('tbody')
                    if not tbody: continue
                    
                    for row in tbody.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) == 2:
                            label = cells[0].get_text(strip=True)
                            value_element = cells[1].find('a') or cells[1]
                            raw_value = value_element.get_text(strip=True)

                            if label in INVESTSITE_INDICADORES_MAP:
                                key = INVESTSITE_INDICADORES_MAP[label]
                                
                                if key not in dados or dados[key] is None:
                                    if key in NON_NUMERIC_KEYS:
                                        dados[key] = raw_value
                                    else:
                                        normalized_value = normalize_numeric_value(raw_value)
                                        if normalized_value is not None:
                                            dados[key] = normalized_value
                
                # Se a extração foi bem-sucedida, retorna os dados.
                return dados

            except Exception as e:
                print(f"Tentativa {tentativa+1} para {self.ticker} no InvestSite (Indicadores) falhou: {e}")
                ultimo_erro = str(e)
        
        # Se o loop terminar sem sucesso, preenche a mensagem de erro.
        dados["investsiteindicadores_erro"] = f"InvestSite (Indicadores): Falha após {max_tentativas} tentativas: {ultimo_erro}"
        return dados