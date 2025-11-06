from datetime import datetime
import time
import pytz
from bs4 import BeautifulSoup
import json
from curl_cffi import requests as curl_requests


def buscar_dados_acao_investidor10(ticker):
    """
    Scraper para Investidor10 com curl_cffi, simulando navegador para evitar bloqueios
    """
    url = f"https://investidor10.com.br/acoes/{ticker.lower()}/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

    try:
        response = curl_requests.get(
            url,
            headers=headers,
            impersonate="chrome110",
            timeout=20
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        dados = {
            "ticker": ticker,
            "cotacao": "N/A",
            "pl": "N/A",
            "pvp": "N/A", 
            "dy": "N/A",
            "psr": "N/A",
            "payout": "N/A",
            "margem_liquida": "N/A",
            "margem_bruta": "N/A",
            "ev_ebitda": "N/A",
            "ev_ebit": "N/A",
            "vpa": "N/A",
            "roe": "N/A",
            "divida_liquida_patrimonio": "N/A",
            "divida_liquida_ebitda": "N/A",
            "liquidez_corrente": "N/A",
            "cagr_receitas_5anos": "N/A",
            "cagr_lucros_5anos": "N/A",
            "variacao_12m": "N/A"
        }

        # Extrai cotação
        cotacao_div = soup.find("div", class_="_card cotacao")
        if cotacao_div:
            value_span = cotacao_div.find("span", class_="value")
            if value_span:
                dados["cotacao"] = value_span.get_text(strip=True)

        # Extrai variação 12 meses
        variacao_cards = soup.find_all("div", class_="_card")
        for card in variacao_cards:
            header = card.find("div", class_="_card-header")
            if header:
                header_text = header.get_text(strip=True).upper()
                if "VARIAÇÃO" in header_text and "12M" in header_text:
                    card_body = card.find("div", class_="_card-body")
                    if card_body:
                        span = card_body.find("span")
                        if span:
                            dados["variacao_12m"] = span.get_text(strip=True)
                            break

        # Extrai indicadores da seção de indicadores fundamentalistas
        indicators_section = soup.find("div", id="indicators")
        if indicators_section:
            cells = indicators_section.find_all("div", class_="cell")
            
            for cell in cells:
                cell_text = cell.get_text().upper()
                value_div = cell.find("div", class_="value")
                if value_div:
                    value_span = value_div.find("span")
                    if value_span:
                        valor = value_span.get_text(strip=True)

                        if "P/L" in cell_text and "P/LP" not in cell_text:
                            dados["pl"] = valor
                        elif "P/VP" in cell_text:
                            dados["pvp"] = valor
                        elif "DIVIDEND YIELD" in cell_text:
                            dados["dy"] = valor
                        elif "P/RECEITA" in cell_text:
                            dados["psr"] = valor
                        elif "PAYOUT" in cell_text:
                            dados["payout"] = valor
                        elif "MARGEM LÍQUIDA" in cell_text:
                            dados["margem_liquida"] = valor
                        elif "MARGEM BRUTA" in cell_text:
                            dados["margem_bruta"] = valor
                        elif "EV/EBITDA" in cell_text:
                            dados["ev_ebitda"] = valor
                        elif "EV/EBIT" in cell_text:
                            dados["ev_ebit"] = valor
                        elif "VPA" in cell_text:
                            dados["vpa"] = valor
                        elif "ROE" in cell_text:
                            dados["roe"] = valor
                        elif "DÍVIDA LÍQUIDA / PATRIMÔNIO" in cell_text or "DIVIDA LIQUIDA / PATRIMONIO" in cell_text:
                            dados["divida_liquida_patrimonio"] = valor
                        elif "DÍVIDA LÍQUIDA / EBITDA" in cell_text or "DIVIDA LIQUIDA / EBITDA" in cell_text or "DL/EBITDA" in cell_text:
                            dados["divida_liquida_ebitda"] = valor
                        elif "LIQUIDEZ CORRENTE" in cell_text:
                            dados["liquidez_corrente"] = valor
                        elif "CAGR RECEITAS 5 ANOS" in cell_text or "CAGR RECEITA" in cell_text:
                            dados["cagr_receitas_5anos"] = valor
                        elif "CAGR LUCROS 5 ANOS" in cell_text or "CAGR LUCRO" in cell_text:
                            dados["cagr_lucros_5anos"] = valor

        return dados

    except Exception as e:
        return {"ticker": ticker, "erro": str(e)}

def buscar_dados_acao_fundamentus(ticker):
    """
    Scraper para Fundamentus com curl_cffi, simulando navegador real para evitar bloqueios (403)
    """
    url = f"https://www.fundamentus.com.br/detalhes.php?papel={ticker.upper()}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://www.fundamentus.com.br/",
        "Cache-Control": "max-age=0"
    }

    max_tentativas = 3
    tentativa = 0

    while tentativa < max_tentativas:
        try:
            time.sleep(2 * (tentativa + 1))  # atraso progressivo

            response = curl_requests.get(
                url,
                headers=headers,
                impersonate="chrome110",  # parâmetro suportado atualmente
                timeout=20
            )

            if "captcha" in response.text.lower():
                raise Exception("Bloqueado por CAPTCHA")

            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            ano_atual = datetime.now().year

            dados = {
                "ticker": ticker,
                "oscilacao_ano_atual": "N/A",
                "oscilacao_ano_menos_1": "N/A",
                "oscilacao_ano_menos_2": "N/A",
                "oscilacao_ano_menos_3": "N/A",
                "oscilacao_ano_menos_4": "N/A",
                "oscilacao_ano_menos_5": "N/A"
            }

            # Localiza todas as linhas da tabela que contenham oscilações
            linhas = soup.find_all("tr")
            for linha in linhas:
                colunas = linha.find_all("td")
                if len(colunas) >= 2:
                    texto_ano = colunas[0].get_text(strip=True)
                    texto_valor = colunas[1].get_text(strip=True)

                    if texto_ano.isdigit():
                        ano = int(texto_ano)
                        if ano == ano_atual:
                            dados["oscilacao_ano_atual"] = texto_valor
                        elif ano == ano_atual - 1:
                            dados["oscilacao_ano_menos_1"] = texto_valor
                        elif ano == ano_atual - 2:
                            dados["oscilacao_ano_menos_2"] = texto_valor
                        elif ano == ano_atual - 3:
                            dados["oscilacao_ano_menos_3"] = texto_valor
                        elif ano == ano_atual - 4:
                            dados["oscilacao_ano_menos_4"] = texto_valor
                        elif ano == ano_atual - 5:
                            dados["oscilacao_ano_menos_5"] = texto_valor

            return dados

        except Exception as e:
            tentativa += 1
            ultimo_erro = str(e)
            if tentativa == max_tentativas:
                return {
                    "ticker": ticker,
                    "erro": f"Falha após {max_tentativas} tentativas: {ultimo_erro}"
                }


# Lista de ações para consulta
acoes = ["AALR3", "ABCB4", "ABEV3", "AERI3", "AFLT3", "AGRO3", "AGXY3", 
         "YDUQ3"
         ]

print("Iniciando extração de dados...")

# Busca os dados de todas as ações usando ambos os métodos
dados_acoes = []
for acao in acoes:
    print(f"Extraindo dados de {acao}...")
    
    # Dados do Investidor10
    dados_inv10 = buscar_dados_acao_investidor10(acao)
    
    # Dados do Fundamentus
    dados_fund = buscar_dados_acao_fundamentus(acao)
    
    # Combina os dados
    dados_combinados = {**dados_inv10, **dados_fund}
    # Remove duplicata do ticker
    if "ticker" in dados_fund and "ticker" in dados_inv10:
        dados_combinados["ticker"] = dados_inv10["ticker"]
    
    # Força o uso da timezone de São Paulo (que representa o horário de Brasília)
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    dados_combinados["atualizado_em"] = datetime.now(brasilia_tz).strftime("%Y-%m-%d %H:%M:%S")
    
    dados_acoes.append(dados_combinados)


# Salva na área de trabalho
# desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
# json_path = os.path.join(desktop_path, 'investbr.json')


json_path = 'dados_acoes.json'

with open(json_path, 'w', encoding='utf-8') as json_file:
    json.dump(dados_acoes, json_file, indent=4, ensure_ascii=False)

print(f"Arquivo salvo em: {json_path}")

# Exibe um resumo dos dados extraídos
print("\n=== RESUMO DOS DADOS EXTRAÍDOS ===")
for dado in dados_acoes:
    if "erro" not in dado:
        print(f"\n{dado['ticker']}:")
        print(f"  Cotação: {dado['cotacao']}")
        print(f"  P/L: {dado['pl']}")
        print(f"  P/VP: {dado['pvp']}")
        print(f"  DY: {dado['dy']}")
        print(f"  PSR: {dado['psr']}")
        print(f"  EV/EBIT: {dado['ev_ebit']}")
        print(f"  VPA: {dado['vpa']}")
        print(f"  ROE: {dado['roe']}")
        print(f"  Dívida Líq./Patrimônio: {dado['divida_liquida_patrimonio']}")
        print(f"  Dívida Líq./EBITDA: {dado['divida_liquida_ebitda']}")
        print(f"  Liquidez Corrente: {dado['liquidez_corrente']}")
        print(f"  CAGR Receitas 5a: {dado['cagr_receitas_5anos']}")
        print(f"  CAGR Lucros 5a: {dado['cagr_lucros_5anos']}")
        print(f"  Variação 12m: {dado['variacao_12m']}")
        print(f"  Oscilação {datetime.now().year}: {dado.get('oscilacao_ano_atual', 'N/A')}")
        print(f"  Oscilação {datetime.now().year-1}: {dado.get('oscilacao_ano_menos_1', 'N/A')}")
        print(f"  Oscilação {datetime.now().year-2}: {dado.get('oscilacao_ano_menos_2', 'N/A')}")
        print(f"  Oscilação {datetime.now().year-3}: {dado.get('oscilacao_ano_menos_3', 'N/A')}")
        print(f"  Oscilação {datetime.now().year-4}: {dado.get('oscilacao_ano_menos_4', 'N/A')}")
        print(f"  Oscilação {datetime.now().year-5}: {dado.get('oscilacao_ano_menos_5', 'N/A')}")
    else:
        print(f"\n{dado['ticker']}: ERRO - {dado['erro']}")