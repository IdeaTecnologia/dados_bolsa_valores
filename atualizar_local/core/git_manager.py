import subprocess
import sys
import pytz
from datetime import datetime


class GitManager:
    """Gerencia operações Git (pull, commit, push)."""
    
    @staticmethod
    def executar_comando(comando, mensagem_erro, mostrar_output=False):
        """Executa um comando Git."""
        try:
            print(f"CMD: {' '.join(comando)}")
            
            if mostrar_output:
                subprocess.run(comando, check=True, text=True, encoding='utf-8')
            else:
                subprocess.run(comando, check=True, text=True, 
                            capture_output=True, encoding='utf-8')
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ {mensagem_erro}")
            if not mostrar_output and e.stderr:
                print(f"   Detalhe: {e.stderr}")
            return False
    
    def pull(self):
        """Faz git pull. Se falhar, interrompe a execução."""
        print("\n🔄 Sincronizando com GitHub (Git Pull)...")
        
        # Verificar conexão primeiro
        try:
            subprocess.run(["git", "ls-remote", "origin"], 
                        check=True, 
                        capture_output=True, 
                        timeout=10)
        except:
            print("❌ ERRO CRÍTICO: Sem conexão com GitHub!")
            print("   Git Pull é OBRIGATÓRIO para não perder dados!")
            sys.exit(1)  # ← FORÇA SAÍDA do programa
        
        if not self.executar_comando(["git", "pull", "origin", "main"], 
                                    "Falha no Git Pull", 
                                    mostrar_output=True):
            print("❌ ERRO CRÍTICO: Git Pull falhou!")
            sys.exit(1)  # ← FORÇA SAÍDA
        
        print("✅ Sincronizado com GitHub")

    
    def push(self, arquivos, mensagem_commit):
        """Faz git add, commit e push se houver mudanças."""
        # Verificar se há mudanças (silenciosamente)
        try:
            subprocess.run(["git", "diff", "--exit-code"] + arquivos, 
                          check=True, 
                          capture_output=True)  # ← Silencia output
            print("📝 Sem mudanças nos arquivos")
            return False
        except subprocess.CalledProcessError:
            # Há mudanças, fazer commit e push
            print("\n📤 Enviando alterações para o GitHub...")
            
            tz_brasilia = pytz.timezone('America/Sao_Paulo')
            timestamp = datetime.now(tz_brasilia).strftime('%Y-%m-%d %H:%M:%S')
            mensagem_completa = f"{mensagem_commit}: {timestamp}"
            
            # Git add (silencioso - evita mostrar diff gigante)
            if not self.executar_comando(["git", "add"] + arquivos, 
                                        "Erro no git add",
                                        mostrar_output=False):  # ← Silencioso
                return False
            
            # Git commit (silencioso)
            if not self.executar_comando(["git", "commit", "-m", mensagem_completa], 
                                        "Erro no commit",
                                        mostrar_output=False):  # ← Silencioso
                return False
            
            # Git push (mostra output - pode pedir senha)
            if not self.executar_comando(["git", "push", "origin", "main"],
                                        "Erro no push",
                                        mostrar_output=True):
                print("⚠️ ATENÇÃO: Dados salvos localmente (commit feito)")
                print("   Execute 'git push origin main' manualmente quando a conexão voltar.")
                return False
            
            print("✅ Alterações enviadas com sucesso!")
            return True