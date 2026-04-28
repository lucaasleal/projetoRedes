# Módulo servidor.py: Responsável por receber os arquivos enviados pelo cliente, salvando-os na pasta,
#                     renomear esses arquivos e retorná-los para o cliente


import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o cliente

SERVER_PORT = 12000 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # cria o socket UDP do servidor
serverSocket.bind(('', SERVER_PORT)) # vincula o socket à porta definida

print('O servidor está pronto para receber conexões!')

## Laço para receber os arquivos enviados pelo cliente e retornar os arquivos renomeados
while True: # (laço infinito para simular um servidor que fica sempre ativo para receber conexões dos clientes)

    ## RECEBIMENTO DOS ARQUIVOS ##

    msg, client = serverSocket.recvfrom(BUFFER_SIZE) # recebe o nome do arquivo enviado pelo cliente

    fileName = msg.decode() # decodifica o nome do arquivo recebido do cliente

    numPackage = 0 # reseta o contador de pacotes recebidos
    
    ## Rotina que recebe os pacotes do arquivo enviado pelo cliente e escreve o conteúdo em um novo arquivo
    with open('pasta/' + fileName, 'wb') as file:
        
        ## Laço que recebe os pacotes do arquivo enviado pelo cliente enquanto houver conteúdo para ler, escrevendo o conteúdo dos pacotes no novo arquivo criado
        while True:
            msg, client = serverSocket.recvfrom(BUFFER_SIZE) # recebe o pacote do arquivo enviado pelo cliente
            
            if msg == b'': # condição que sinaliza o fim do arquivo enviado pelo cliente
                file.write(b'') # envia o caractere null para sinalizar o fim do arquivo no novo arquivo criado
                numPackage += 1
                
                break

            file.write(msg) # escreve o conteúdo do pacote recebido no novo arquivo criado
            numPackage += 1

        print(f"Número de pacotes recebidos: {numPackage}")
        print(f"Arquivo {fileName} recebido com sucesso!")

    ## RETORNO DOS ARQUIVOS ##

    numPackage = 0 # reseta o contador de pacotes enviados

    ## Rotina que abre o arquivo para leitura em modo binário, renomea-o e envia-o em pacotes para o cliente
    with open('pasta/' + fileName, 'rb') as file:
        fileRenamed = 'leilao_' + fileName ## tratamento para enviar o arquivo renomeado para o cliente

        serverSocket.sendto(fileRenamed.encode(), client) # envia o nome do arquivo renomeado codificado para o cliente
        numPackage += 1

        package = file.read(BUFFER_SIZE) # lê o conteúdo do arquivo em pacotes do tamanho do buffer

        ## Laço que envia os pacotes do arquivo para o cliente enquanto houver conteúdo para ler
        while package:
            serverSocket.sendto(package, client) # envia o pacote para o cliente
            numPackage += 1

            package = file.read(BUFFER_SIZE) # lê o próximo pacote do arquivo até o final do arquivo

        serverSocket.sendto(b'', client) # envia o caractere null para sinalizar o cliente do fim do arquivo
        numPackage += 1

        print(f"Arquivo {fileRenamed} retornado com sucesso!")
        print(f"Número de pacotes enviados: {numPackage}")
        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

