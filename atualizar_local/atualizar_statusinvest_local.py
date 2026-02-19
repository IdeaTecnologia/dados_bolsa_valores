import os
import json
import subprocess
import sys
from models.acao import Acao
from utils.listaticker import ListaTicker
import pytz

JSON_FILE = 'dados_acoes.json'

def executar_comando_git(comando, mensagem_erro):
    try:
        print(f"CMD: {' '.join(comando)}")
        # capture_output=False permite que você interaja com o terminal (senha)
        subprocess.run(comando, check=True, text=True) 
    except subprocess.CalledProcessError:
        print(f"❌ {mensagem_erro}")
        return False
    return True

def carregar_dados_existentes():
    if not os.path.exists(JSON_FILE): return {}
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            lista = json.load(f)
            return {item['ticker']: item for item in lista}
    except: return {}

def gerar_metadata(dados_finais):
    """
    Gera um arquivo minúsculo com a data da última atualização e o total de ações.
    Isso evita que o App Android baixe o JSON de 7MB sem necessidade.
    """
    if not dados_finais:
        return
    
    # Pega o timestamp da última ação da lista (que é a atualização mais recente)
    ultima_data = dados_finais[-1].get("atualizado_em", "0000-00-00 00:00:00")
    
    metadata = {
        "ultima_atualizacao": ultima_data,
        "total_acoes": len(dados_finais)
    }
    
    with open('metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
    print(f"📊 Metadata gerado com sucesso: {ultima_data}")


def main():
    print("="*60)
    print("   🚀 ATUALIZADOR STATUSINVEST LOCAL (Requests)")
    print("="*60)

    # 1. GIT PULL
    print("\n[1/3] Baixando JSON atual do GitHub...")
    if not executar_comando_git(["git", "pull"], "Falha no Git Pull"):
        print("Resolva os conflitos antes de rodar.")
        return

    # 2. SCRAPING LOCAL
    print("\n[2/3] Atualizando StatusInvest...")
    lista_provider = ListaTicker()
    acoes = lista_provider.obter_lista_ticker()
    # acoes = ["WEGE3"] # <--- Para testar rápido
    
    # Cache atual (contém dados do Inv10, Fundamentus, etc)
    mapa_dados = carregar_dados_existentes()
    dados_finais = []

    for i, ticker in enumerate(acoes):
        print(f"\n--- {i+1}/{len(acoes)}: {ticker} ---")
        
        dado_existente = mapa_dados.get(ticker)
        
        try:
            acao = Acao(ticker)
            # A mágica acontece aqui: 
            # Passamos o dado existente e pedimos para atualizar SÓ o StatusInvest
            dados_atualizados = acao.get_all_data(
                dados_existentes=dado_existente, 
                apenas_statusinvest=True, # Não roda outros scrapers
                use_local_strategy=True   # Usa requests headers
            )
            dados_finais.append(dados_atualizados)
            
        except Exception as e:
            print(f"❌ Erro grave em {ticker}: {e}")
            if dado_existente: dados_finais.append(dado_existente)

    # SALVAR
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(dados_finais, json_file, indent=4, ensure_ascii=False)
        
        # Gera o arquivo de assinatura para o App Android
        gerar_metadata(dados_finais)
        
        print(f"\n✅ Processo concluído! Arquivo salvo: {JSON_FILE}")
    except IOError as e:
        print(f"Erro crítico ao salvar JSON: {e}")

    # 3. GIT PUSH
    print("\n[3/3] Enviando para GitHub...")
    # Adiciona o JSON e o METADATA
    if not executar_comando_git(["git", "add", JSON_FILE, "metadata.json"], "Erro no Git Add"): 
        return
    
    # Verifica se tem algo para commitar
    try:
        subprocess.run(["git", "diff-index", "--quiet", "HEAD"], check=True)
        print("Nenhuma alteração para enviar.")
        return
    except subprocess.CalledProcessError:
        # Se falhou, é porque tem mudanças (código de saída 1), então prossegue
        pass

    msg = f"Update StatusInvest Local {os.getlogin()}"
    if not executar_comando_git(["git", "commit", "-m", msg], "Erro no Commit"): return
    
    print("Enviando... (Digite a senha se solicitado)")
    if executar_comando_git(["git", "push"], "Erro no Push"):
        print("\n✨ SUCESSO! ✨")

if __name__ == "__main__":
    main()