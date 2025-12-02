import json
import os
from datetime import datetime
from models.acao import Acao
from utils.listaticker import ListaTicker

JSON_FILE = 'dados_acoes.json'
DIAS_VALIDADE_CACHE = 5

def carregar_dados_existentes():
    if not os.path.exists(JSON_FILE):
        return {}
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            lista = json.load(f)
            # Cria um dicionário indexado pelo ticker para busca rápida
            return {item['ticker']: item for item in lista}
    except Exception as e:
        print(f"Erro ao ler JSON existente: {e}")
        return {}

def extrair_apenas_statusinvest(dados_completos):
    """
    Filtra do JSON completo apenas as chaves que começam com 'statusInvest'.
    """
    if not dados_completos: return None
    return {k: v for k, v in dados_completos.items() if k.startswith('statusInvest')}

def main():
    lista_provider = ListaTicker()
    # acoes_a_consultar = ["ABEV3","ITSA4","EGIE3","FLRY3"] # Para teste
    acoes_a_consultar = lista_provider.obter_lista_ticker()

    if not acoes_a_consultar:
        print("Nenhuma ação para consultar.")
        return

    print("\nIniciando atualização inteligente...")
    
    # 1. Carrega o estado atual do banco de dados (JSON)
    mapa_dados_existentes = carregar_dados_existentes()
    
    dados_finais = []
    
    # Flag global: Se virar True, paramos de tentar o StatusInvest para TODOS
    status_invest_esgotado = False 

    total = len(acoes_a_consultar)
    for i, ticker in enumerate(acoes_a_consultar):
        print(f"\n--- Processando {i+1}/{total}: {ticker} ---")
        
        dados_antigos = mapa_dados_existentes.get(ticker)
        dados_status_invest_para_injetar = None
        usar_scraper_status = True

        # --- LÓGICA DE DECISÃO: RODAR STATUSINVEST OU NÃO? ---
        
        # Cenário A: As chaves já acabaram em iterações anteriores
        if status_invest_esgotado:
            print("⚠️ Cota de API esgotada anteriormente. Usando dados antigos.")
            usar_scraper_status = False
        
        # Cenário B: Temos dados antigos. Vamos ver se são recentes.
        elif dados_antigos:
            data_att_str = dados_antigos.get('statusInvest_data_atualizacao')
            if data_att_str:
                try:
                    # CORREÇÃO AQUI: Pegamos apenas os 10 primeiros caracteres (YYYY-MM-DD)
                    # Isso funciona se tiver hora ("2025-12-01 10:00:00") ou não ("2025-12-01")
                    data_str_limpa = data_att_str[:10] 
                    data_att = datetime.strptime(data_str_limpa, "%Y-%m-%d")
                    
                    dias_passados = (datetime.now() - data_att).days
                    
                    if dias_passados < DIAS_VALIDADE_CACHE:
                        print(f"ℹ️ Dados StatusInvest recentes ({dias_passados} dias). Mantendo cache.")
                        usar_scraper_status = False
                    else:
                        print(f"Old Dados StatusInvest antigos ({dias_passados} dias). Tentando atualizar...")
                except ValueError:
                    # Se data estiver bugada, tenta atualizar
                    pass

        # --- EXECUÇÃO ---
        
        try:
            # Se decidimos NÃO rodar o scraper (ou porque esgotou ou porque é recente)
            if not usar_scraper_status:
                # Extrai os dados do JSON antigo para passar para a classe
                dados_status_invest_para_injetar = extrair_apenas_statusinvest(dados_antigos)
                
                # Se não tinha dados antigos (ex: ação nova na lista) e a cota acabou,
                # infelizmente vai ficar null, mas evitamos crash.
            
            acao = Acao(ticker)
            
            # Chama o método. Se passar o segundo argumento, ele PULA o request caro.
            dados_novos = acao.get_all_data(
                dados_existentes=dados_status_invest_para_injetar,
                use_local_strategy=False # No GitHub Actions usa API e não local (False)
            )

            # Verifica se houve erro fatal de chaves durante a execução dessa ação
            # O scraper retorna erro_statusinvest = "ALL_KEYS_EXHAUSTED" se falhar tudo
            if usar_scraper_status:
                erro_si = dados_novos.get('statusInvest_erro')
                if erro_si == "ALL_KEYS_EXHAUSTED":
                    print("⛔ LIMITE DE API ATINGIDO (Todas as chaves).")
                    status_invest_esgotado = True
                    
                    # Tenta salvar o que deu (recupera o antigo se falhou agora)
                    if dados_antigos:
                        print("Recuperando dados antigos do StatusInvest para este ticker...")
                        dados_recuperados = extrair_apenas_statusinvest(dados_antigos)
                        dados_novos.update(dados_recuperados)
            
            dados_finais.append(dados_novos)

        except Exception as e:
            print(f"❌ Erro fatal em {ticker}: {e}")
            # Em caso de erro geral, tenta manter o dado antigo no JSON final
            if dados_antigos:
                dados_finais.append(dados_antigos)

    # SALVAMENTO
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(dados_finais, json_file, indent=4, ensure_ascii=False)
        print(f"\n✅ Processo concluído! Arquivo salvo: {JSON_FILE}")
    except IOError as e:
        print(f"Erro crítico ao salvar JSON: {e}")

if __name__ == "__main__":
    main()