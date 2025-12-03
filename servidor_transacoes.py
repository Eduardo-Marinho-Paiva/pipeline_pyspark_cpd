import socket
import time
import csv
import sys


# Configurações do Servidor
HOST = 'localhost'
PORT = 9999
ARQUIVO_CSV = 'transações_2000.csv' 

def iniciar_servidor():
    # Cria o socket TCP
    # AF_INET indica IPv4, SOCK_STREAM indica TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    
    # Tenta vincular o socket à porta especificada
    try: 
        s.bind((HOST, PORT))
    except OSError:
        print(f"Erro: A porta {PORT} já está em uso. Espere um minuto ou mude a porta.")
        sys.exit(1)
        
    s.listen(1) # Começa a escutar conexões
    print(f"--- SERVIDOR INICIADO ---")
    print(f"Lendo dados de: {ARQUIVO_CSV}")
    print(f"Aguardando conexão do Spark na porta {PORT}...")
    
    conn, addr = s.accept() # Aceita a conexão do cliente
    print(f"Conectado por: {addr}") # Endereço do cliente conectado
    
    # Envia os dados do CSV linha por linha
    try:
        with open(ARQUIVO_CSV, 'r', encoding='utf-8') as f:
            leitor = csv.reader(f)
            next(leitor)
            
            for linha in leitor:
                
                valor_limpo = linha[1].replace('.', '').replace(',', '.') # Ajusta o formato do valor
                linha[1] = valor_limpo
                
                mensagem = ";".join(linha) # Concatena os campos com ";"
                
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