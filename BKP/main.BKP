import json
from models.acao import Acao
from utils.listaticker import ListaTicker

def main():
    lista_provider = ListaTicker()
    # acoes_a_consultar = lista_provider.obter_lista_ticker()

    # Apenas para testes rápidos, descomente a linha abaixo:
    acoes_a_consultar = ["ABEV3","ITSA4","EGIE3","FLRY3"]

    # VERIFICA SE A LISTA FOI CARREGADA COM SUCESSO
    if not acoes_a_consultar:
        print("Nenhuma ação para consultar. Verifique a conexão ou a planilha. Encerrando o programa.")
        return

    print("\nIniciando extração de dados das ações...")

    dados_totais = []
    total_acoes = len(acoes_a_consultar)
    for i, ticker in enumerate(acoes_a_consultar):
        print(f"\n--- Processando {i+1}/{total_acoes} ---")
        try:
            acao = Acao(ticker)
            dados_da_acao = acao.get_all_data()
            dados_totais.append(dados_da_acao)
        except Exception as e:
            print(f"Ocorreu um erro inesperado ao processar {ticker}: {e}")
            dados_totais.append({
                "ticker": ticker,
                "erro_geral": f"Falha no processamento principal: {str(e)}"
            })

    json_path = 'dados_acoes.json'
    try:
        with open(json_path, 'w', encoding='utf-8') as json_file:
            json.dump(dados_totais, json_file, indent=4, ensure_ascii=False)
        print(f"\nExtração concluída! Arquivo salvo em: {json_path}")
    except IOError as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")

if __name__ == "__main__":
    main()