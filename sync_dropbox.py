"""Script utilitário para agendar a sincronização do Dropbox sem subir o servidor Flask."""
from datetime import datetime
from app import sincronizar_programacao_dropbox

if __name__ == "__main__":
    print(f"[SYNC] Iniciando sincronização às {datetime.now():%Y-%m-%d %H:%M:%S}")
    resultado = sincronizar_programacao_dropbox()
    status = "SUCESSO" if resultado["sucesso"] else "FALHA"
    print(f"[SYNC] Status: {status}")
    print(f"[SYNC] Mensagem: {resultado['mensagem']}")
    if resultado["erros"]:
        for erro in resultado["erros"]:
            print(f"[SYNC][ERRO] {erro}")
    print(f"[SYNC] Total de registros carregados: {len(resultado['registros'])}")
