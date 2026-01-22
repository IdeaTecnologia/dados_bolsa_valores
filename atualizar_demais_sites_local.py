import os
import json
import subprocess
import sys
from models.acao import Acao
from utils.listaticker import ListaTicker

# Nome do arquivo de dados
JSON_FILE = 'dados_acoes.json'

def executar_comando_git(comando, mensagem_erro):
    """
    Executa comandos GIT no terminal local.
    """
    try:
        print(f"CMD: {' '.join(comando)}")
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

def extrair_apenas_statusinvest(dados_completos):
    """
    Recupera os dados do StatusInvest do JSON antigo para não perdê-los
    caso a gente atualize apenas o Investidor10/Fundamentus.
    """
    if not dados_completos: return None
    return {k: v for k, v in dados_completos.items() if k.startswith('statusInvest')}

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
    print("   🛡️ ATUALIZADOR GERAL (SEM STATUS INVEST) LOCAL (FALLBACK / SEM API) 🛡️")
    print("   Atualiza Investidor10, Fundamentus e preserva SI.")
    print("="*60)

    # 1. GIT PULL (Para garantir que temos a versão mais recente do StatusInvest ou do Actions)
    print("\n[1/3] Sincronizando com o GitHub (Git Pull)...")
    if not executar_comando_git(["git", "pull"], "Falha no Git Pull. Resolva conflitos manuais."):
        return

    # 2. CARGA E PROCESSAMENTO
    print("\n[2/3] Iniciando Scraping Geral (Sem Status Invest) (Local Strategy)...")
    
    lista_provider = ListaTicker()
    acoes_a_consultar = lista_provider.obter_lista_ticker()
    # acoes_a_consultar = ["ABEV3"] # Descomente para testes rápidos
    
    mapa_dados_existentes = carregar_dados_existentes()
    dados_finais = []

    total = len(acoes_a_consultar)
    
    for i, ticker in enumerate(acoes_a_consultar):
        print(f"\n--- Processando {i+1}/{total}: {ticker} ---")
        
        dados_antigos = mapa_dados_existentes.get(ticker)
        
        # Estratégia de Preservação:
        # Recuperamos os dados do StatusInvest do arquivo atual para injetá-los
        # na chamada. Assim, a classe Acao não precisa rodar o scraper pesado do SI
        # se os dados já estiverem lá, e se rodar, rodará localmente.
        dados_si_preservados = extrair_apenas_statusinvest(dados_antigos)
        
        try:
            acao = Acao(ticker)
            
            # use_local_strategy=True é CRUCIAL aqui.
            # Garante que NÃO tente usar a API (que falharia localmente sem secrets).
            # Se for necessário atualizar SI, ele usará requests local.
            dados_novos = acao.get_all_data(
                dados_existentes=dados_si_preservados,
                use_local_strategy=True 
            )
            
            dados_finais.append(dados_novos)

        except Exception as e:
            print(f"❌ Erro ao processar {ticker}: {e}")
            # Em caso de erro, tenta salvar o dado antigo para não criar buraco no JSON
            if dados_antigos:
                print("   -> Mantendo dados antigos para este ticker.")
                dados_finais.append(dados_antigos)

    # SALVAMENTO
    try:
        with open(JSON_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(dados_finais, json_file, indent=4, ensure_ascii=False)
        
        # Gera o arquivo de assinatura para o App Android
        gerar_metadata(dados_finais)
        
        print(f"\n✅ Processo concluído! Arquivo salvo: {JSON_FILE}")
    except IOError as e:
        print(f"Erro crítico ao salvar JSON: {e}")

    # 3. GIT PUSH
    print("\n[3/3] Enviando atualização para GitHub...")
    # Adiciona o JSON e o METADATA
    if not executar_comando_git(["git", "add", JSON_FILE, "metadata.json"], "Erro no Git Add"): 
        return
    
    # Confere se houve mudança real
    try:
        subprocess.run(["git", "diff-index", "--quiet", "HEAD"], check=True)
        print("ℹ️ Nenhuma alteração detectada nos dados. Nada a enviar.")
        return
    except subprocess.CalledProcessError:
        pass # Segue o baile, tem mudança

    # Commit e Push
    msg = f"Update Geral Local (Sem Status Invest) (Fallback) {os.getlogin()}"
    if not executar_comando_git(["git", "commit", "-m", msg], "Erro no Git Commit"): return
    
    print("Subindo alterações... (Autentique se necessário)")
    if executar_comando_git(["git", "push"], "Erro no Git Push"):
        print("\n✨ SUCESSO! Repositório atualizado manualmente. ✨")

if __name__ == "__main__":
    main()