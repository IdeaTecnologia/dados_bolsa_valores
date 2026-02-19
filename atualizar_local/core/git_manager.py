import subprocess
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
                # Permite interação (ex: senha do git push)
                subprocess.run(comando, check=True, text=True)
            else:
                # Silencia a saída (evita looping de JSON gigante)
                subprocess.run(comando, check=True, text=True, 
                             capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ {mensagem_erro}")
            if not mostrar_output and e.stderr:
                print(f"   Detalhe: {e.stderr}")
            return False
    
    def pull(self):
        """Faz git pull. Se falhar, interrompe a execução."""
        print("\n🔄 Sincronizando com GitHub (Git Pull)...")
        if not self.executar_comando(["git", "pull"], 
                                     "Falha no Git Pull", 
                                     mostrar_output=True):  # Mostra output do pull
            print("❌ ERRO CRÍTICO: Não foi possível sincronizar com GitHub.")
            print("   Isso é obrigatório para não perder dados!")
            print("   Verifique sua conexão e tente novamente.")
            raise Exception("Git Pull falhou")
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
            if not self.executar_comando(["git", "push"], 
                                        "Erro no push",
                                        mostrar_output=True):  # ← Mostra output
                return False
            
            print("✅ Alterações enviadas com sucesso!")
            return True