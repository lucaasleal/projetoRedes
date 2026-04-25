from socket import *

serverPort = 12000
serverSocket = socket(AF_INET, SOCK_DGRAM)
serverSocket.bind(('', serverPort))
bufferSize = 1024

print('The server is ready to receive')

while True:

    # recebe o nome do arquivo
    msg, cliente = serverSocket.recvfrom(bufferSize)
    
    # cria o novo nome do arquivo (ex: arquivo_leilao.txt)
    fileName = msg.decode()
    num_pacotes = 0
    # recebe o conteúdo do arquivo e cria um novo arquivo com o mesmo nome
    # escreve o conteúdo dos pacotes recebidos no novo arquivo
    with open('pasta/' + fileName, 'wb') as f:
        msg, cliente = serverSocket.recvfrom(bufferSize)
        num_pacotes+=1

        while True:
            num_pacotes+=1
            f.write(msg)
            msg, cliente = serverSocket.recvfrom(bufferSize)
            if msg == b'':
                f.write(b'')
                num_pacotes+=1
                break
        print("Número de pacotes recebidos: ", num_pacotes)

        print(f"Arquivo {fileName} recebido com sucesso!")
    

    #REENVIO
    num_pacotes = 0
    
    with open('pasta/' + fileName, 'rb') as f:
        modified_fileName = 'leilao_' + fileName
        serverSocket.sendto(modified_fileName.encode(), cliente)
        num_pacotes+=1

        pct = f.read(bufferSize)
        while pct:
            num_pacotes+=1
            serverSocket.sendto(pct, cliente)
            pct = f.read(bufferSize)
        
        # envia o caracter null para sinalizar o servidor o fim do arquivo
        serverSocket.sendto(b'', cliente)
        num_pacotes+=1

        print("Número de pacotes enviados: ", num_pacotes)

        print(f"Arquivo {fileName} retornado com sucesso!")
        print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
    
    