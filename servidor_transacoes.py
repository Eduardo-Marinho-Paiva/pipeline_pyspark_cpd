import socket
import time
import csv
import sys

HOST = 'localhost'
PORT = 9999
ARQUIVO_CSV = 'transações_2000.csv' 

def iniciar_servidor():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
    except OSError:
        print(f"Erro: A porta {PORT} já está em uso. Espere um minuto ou mude a porta.")
        sys.exit(1)
        
    s.listen(1)
    print(f"--- SERVIDOR INICIADO ---")
    print(f"Lendo dados de: {ARQUIVO_CSV}")
    print(f"Aguardando conexão do Spark na porta {PORT}...")
    
    conn, addr = s.accept()
    print(f"Conectado por: {addr}")
    
    try:
        with open(ARQUIVO_CSV, 'r', encoding='utf-8') as f:
            leitor = csv.reader(f)
            next(leitor)
            
            for linha in leitor:
                
                valor_limpo = linha[1].replace('.', '').replace(',', '.')
                linha[1] = valor_limpo
                
                mensagem = ";".join(linha)
                
                conn.send((mensagem + '\n').encode('utf-8'))
                print(f"Enviado: {mensagem}")
                time.sleep(0.5)
                
    except BrokenPipeError:
        print("O cliente (Spark) encerrou a conexão.")
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{ARQUIVO_CSV}' não foi encontrado na pasta.")
    finally:
        conn.close()
        s.close()

if __name__ == "__main__":
    iniciar_servidor()