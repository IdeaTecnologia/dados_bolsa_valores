import os
import json
import subprocess
import sys
from models.acao import Acao
from utils.listaticker import ListaTicker

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
        with open(JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(dados_finais, f, indent=4, ensure_ascii=False)
        print(f"\n✅ Arquivo salvo com {len(dados_finais)} registros.")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return

    # 3. GIT PUSH
    print("\n[3/3] Enviando para GitHub...")
    if not executar_comando_git(["git", "add", JSON_FILE], "Erro no Add"): return
    
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