import time
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from utils.normalization import normalize_numeric_value

INVESTSITE_PASSIVO_MAP = {
    "Passivo Total": "investsitepassivo_passivo_total",
    "Passivo Circulante": "investsitepassivo_passivo_circulante",
    "Obrigações Sociais e Trabalhistas": "investsitepassivo_obrigacoes_sociais_e_trabalhistas",
    "Obrigações Sociais": "investsitepassivo_obrigacoes_sociais",
    "Obrigações Trabalhistas": "investsitepassivo_obrigacoes_trabalhistas",
    "Fornecedores": "investsitepassivo_fornecedores",
    "Fornecedores Nacionais": "investsitepassivo_fornecedores_nacionais",
    "Fornecedores Estrangeiros": "investsitepassivo_fornecedores_estrangeiros",
    "Obrigações Fiscais": "investsitepassivo_obrigacoes_fiscais",
    "Obrigações Fiscais Federais": "investsitepassivo_obrigacoes_fiscais_federais",
    "Imposto de Renda e Contribuição Social a Pagar": "investsitepassivo_imposto_de_renda_e_contribuicao_social_a_pagar",
    "Demais Tributos e Contribuições Federais": "investsitepassivo_demais_tributos_e_contribuicoes_federais",
    "Diferimento de Impostos sobre Vendas": "investsitepassivo_diferimento_de_impostos_sobre_vendas",
    "Obrigações Fiscais Estaduais": "investsitepassivo_obrigacoes_fiscais_estaduais",
    "Imposto sobre Circulação de Mercadorias": "investsitepassivo_imposto_sobre_circulacao_de_mercadorias",
    "Obrigações Fiscais Municipais": "investsitepassivo_obrigacoes_fiscais_municipais",
    "Empréstimos e Financiamentos": "investsitepassivo_emprestimos_e_financiamentos",
    "Em Moeda Nacional": "investsitepassivo_em_moeda_nacional",
    "Em Moeda Estrangeira": "investsitepassivo_em_moeda_estrangeira",
    "Debêntures": "investsitepassivo_debentures",
    "Financiamento por Arrendamento": "investsitepassivo_financiamento_por_arrendamento",
    "Outras Obrigações": "investsitepassivo_outras_obrigacoes",
    "Passivos com Partes Relacionadas": "investsitepassivo_passivos_com_partes_relacionadas",
    "Débitos com Coligadas": "investsitepassivo_debitos_com_coligadas",
    "Débitos com Controladores": "investsitepassivo_debitos_com_controladores",
    "Débitos com Outras Partes Relacionadas": "investsitepassivo_debitos_com_outras_partes_relacionadas",
    "Outros": "investsitepassivo_outros",
    "Dividendos e JCP a Pagar": "investsitepassivo_dividendos_e_jcp_a_pagar",
    "Dividendo Mínimo Obrigatório a Pagar": "investsitepassivo_dividendo_minimo_obrigatorio_a_pagar",
    "Obrigações por Pagamentos Baseados em Ações": "investsitepassivo_obrigacoes_por_pagamentos_baseados_em_acoes",
    "Instrumentos Financeiros Derivativos": "investsitepassivo_instrumentos_financeiros_derivativos",
    "Conta Garantida": "investsitepassivo_conta_garantida",
    "Opção de Venda Concedida sobre Participação em Controlada": "investsitepassivo_opcao_de_venda_concedida_sobre_participacao_em_controlada",
    "Juros a Pagar": "investsitepassivo_juros_a_pagar",
    "Outros Passivos": "investsitepassivo_outros_passivos",
    "Provisões": "investsitepassivo_provisoes",
    "Provisões Fiscais Previdenciárias Trabalhistas e Cíveis": "investsitepassivo_provisoes_fiscais_previdenciarias_trabalhistas_e_civeis",
    "Provisões Fiscais": "investsitepassivo_provisoes_fiscais",
    "Provisões Previdenciárias e Trabalhistas": "investsitepassivo_provisoes_previdenciarias_e_trabalhistas",
    "Provisões para Benefícios a Empregados": "investsitepassivo_provisoes_para_beneficios_a_empregados",
    "Provisões Cíveis": "investsitepassivo_provisoes_civeis",
    "Provisões Outras": "investsitepassivo_provisoes_outras",
    "Outras Provisões": "investsitepassivo_outras_provisoes",
    "Provisões para Garantias": "investsitepassivo_provisoes_para_garantias",
    "Provisões para Reestruturação": "investsitepassivo_provisoes_para_reestruturacao",
    "Provisões para Passivos Ambientais e de Desativação": "investsitepassivo_provisoes_para_passivos_ambientais_e_de_desativacao",
    "Passivos sobre Ativos Não-Correntes a Venda e Descontinuados": "investsitepassivo_passivos_sobre_ativos_nao_correntes_a_venda_e_descontinuados",
    "Passivos sobre Ativos Não-Correntes a Venda": "investsitepassivo_passivos_sobre_ativos_nao_correntes_a_venda",
    "Passivos sobre Ativos de Operações Descontinuadas": "investsitepassivo_passivos_sobre_ativos_de_operacoes_descontinuadas",
    "Passivo Não Circulante": "investsitepassivo_passivo_nao_circulante",
    "Tributos Diferidos": "investsitepassivo_tributos_diferidos",
    "Imposto de Renda e Contribuição Social Diferidos": "investsitepassivo_imposto_de_renda_e_contribuicao_social_diferidos",
    "Lucros e Receitas a Apropriar": "investsitepassivo_lucros_e_receitas_a_apropriar",
    "Patrimônio Líquido Consolidado": "investsitepassivo_patrimonio_liquido_consolidado",
    "Capital Social Realizado": "investsitepassivo_capital_social_realizado",
    "Reservas de Capital": "investsitepassivo_reservas_de_capital",
    "Reservas de Lucros": "investsitepassivo_reservas_de_lucros",
    "Lucros/Prejuízos Acumulados": "investsitepassivo_lucros_prejuizos_acumulados",
    "Ajustes de Avaliação Patrimonial": "investsitepassivo_ajustes_de_avaliacao_patrimonial",
    "Participação dos Acionistas Não Controladores": "investsitepassivo_participacao_dos_acionistas_nao_controladores",
}

class InvestSitePassivoScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.url = f"https://www.investsite.com.br/balanco_patrimonial_passivo.php?cod_negociacao={self.ticker.upper()}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def _get_all_possible_keys(self):
        """Gera uma lista com todas as chaves de dados possíveis para este scraper."""
        return list(INVESTSITE_PASSIVO_MAP.values())

    def fetch_data(self):
        # Inicializa o dicionário com todas as chaves possíveis e valor None.
        all_keys = self._get_all_possible_keys()
        dados = {key: None for key in all_keys}
        dados["ticker"] = self.ticker
        # Garante que o campo de erro sempre exista.
        dados["erro_investsitepassivo"] = ""

        max_tentativas = 3
        ultimo_erro = ""
        
        for tentativa in range(max_tentativas):
            try:
                time.sleep(1 * (tentativa + 1))
                response = curl_requests.get(self.url, headers=self.headers, impersonate="chrome110", timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                table = soup.find('table', id='balanco_empresa_itr')
                if not table:
                    raise Exception("Tabela de balanço patrimonial não encontrada.")

                tbody = table.find('tbody')
                if not tbody:
                    raise Exception("Corpo da tabela de balanço não encontrado.")

                # LÓGICA DE EXTRAÇÃO E NORMALIZAÇÃO
                for row in tbody.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        raw_label = cells[1].get_text(strip=True)
                        raw_value = cells[2].get_text(strip=True)

                        if raw_label in INVESTSITE_PASSIVO_MAP:
                            key = INVESTSITE_PASSIVO_MAP[raw_label]
                            base_value = normalize_numeric_value(raw_value)
                            
                            if (key not in dados or dados[key] is None) and isinstance(base_value, (int, float)):
                                final_value = int(base_value * 1000)
                                dados[key] = final_value
                
                # Se a extração foi bem-sucedida, retorna os dados.
                return dados

            except Exception as e:
                print(f"Tentativa {tentativa+1} para {self.ticker} no InvestSite (Passivo) falhou: {e}")
                ultimo_erro = str(e)
        
        # Se o loop terminar sem sucesso, preenche a mensagem de erro.
        dados["erro_investsitepassivo"] = f"InvestSite (Passivo): Falha após {max_tentativas} tentativas: {ultimo_erro}"
        return dados