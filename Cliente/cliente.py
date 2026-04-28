# Módulo cliente.py: Responsável por enviar os arquivos para o servidor e
#                    receber os arquivos renomeados do servidor, salvando-os na pasta

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o servidor

SERVER_NAME = 'localhost' # nome do servidor
SERVER_PORT = 12000 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)
LIST_FILES = [
    'atumalaca.jpg',
    'boa_tarde_neymar.mp4',
    'poema.txt',
    'hold_the_line.mp3'
] # lista de arquivos a serem enviados e tratados pelo servidor

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # cria o socket UDP do cliente

## Laço para enviar e receber os arquivos
for fileName in LIST_FILES:
    
    ## ENVIO DE ARQUIVOS ##
    
    numPackage = 0 # reseta o contador de pacotes enviados

    ## Rotina que abre o arquivo para leitura em modo binário e envia-o em pacotes para o servidor
    with open('pasta/' + fileName, 'rb') as file:
        clientSocket.sendto(fileName.encode(), (SERVER_NAME, SERVER_PORT)) # envia o nome do arquivo codificado para o servidor
        numPackage += 1

        package = file.read(BUFFER_SIZE) # lê o conteúdo do arquivo em pacotes do tamanho do buffer
        
        ## Laço que envia os pacotes do arquivo para o servidor enquanto houver conteúdo para ler
        while package:
            clientSocket.sendto(package, (SERVER_NAME, SERVER_PORT)) # envia o pacote para o servidor
            numPackage += 1

            package = file.read(BUFFER_SIZE) # lê o próximo pacote do arquivo até o final do arquivo

        clientSocket.sendto(b'', (SERVER_NAME, SERVER_PORT)) # envia o caractere null para sinalizar o servidor do fim do arquivo

        numPackage += 1

        print(f"Número de pacotes enviados: {numPackage}")

    ## RECEBIMENTO DE ARQUIVOS ##

    msg, _ = clientSocket.recvfrom(BUFFER_SIZE) # recebe o nome do arquivo renomeado pelo servidor

    fileRenamed = msg.decode() # decodifica o nome do arquivo renomeado recebido do servidor

    numPackage = 0 # reseta o contador de pacotes recebidos

    ## Rotina que recebe os pacotes do arquivo renomeado enviado pelo servidor e escreve o conteúdo em um novo arquivo (com nome novo)
    with open('pasta/' + fileRenamed, 'wb') as file:
        msg, _ = clientSocket.recvfrom(BUFFER_SIZE) # recebe o primeiro pacote do arquivo renomeado enviado pelo servidor
        numPackage += 1

        ## Laço que recebe os pacotes do arquivo renomeado enviado pelo servidor enquanto houver conteúdo para ler, escrevendo o conteúdo dos pacotes recebidos no novo arquivo criado
        while True:
            file.write(msg) # escreve o conteúdo do pacote recebido no novo arquivo criado

            msg, _ = clientSocket.recvfrom(BUFFER_SIZE) # recebe o próximo pacote do arquivo renomeado enviado pelo servidor
            numPackage += 1

            if msg == b'':
                file.write(b'') # envia o caractere null para sinalizar o fim do arquivo no novo arquivo criado
                numPackage += 1

                break
        
        print(f"Arquivo {fileRenamed} retornado com sucesso!")
        print(f"Número de pacotes recebidos: {numPackage}")
        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

clientSocket.close() # fecha o socket após o envio e recebimento de todos os arquivos
