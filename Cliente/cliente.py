from socket import *

serverName = 'localhost'
serverPort = 12000
clientSocket = socket(AF_INET, SOCK_DGRAM)

bufferSize = 1024

lista_arquivos = ['atumalaca.jpg', 'boa_tarde_neymar.mp4', 'poema.txt','hold_the_line.mp3']
for fileName in lista_arquivos:
    num_pacotes = 0

    with open('pasta/' + fileName, 'rb') as f:
        # envia o nome do arquivo para ser renomeado no servidor
        clientSocket.sendto(fileName.encode(), (serverName, serverPort))
        num_pacotes+=1

        pct = f.read(bufferSize)
        while pct:
            num_pacotes+=1
            clientSocket.sendto(pct, (serverName, serverPort))
            pct = f.read(bufferSize)
        
        # envia o caracter null para sinalizar o servidor o fim do arquivo
        clientSocket.sendto(b'', (serverName, serverPort))
        num_pacotes+=1
        print("Número de pacotes enviados: ", num_pacotes)

    #Recebimento
    msg, _ = clientSocket.recvfrom(bufferSize)
        
    # cria o novo nome do arquivo (ex: arquivo_leilao.txt)
    fileName = msg.decode()

    # recebe o conteúdo do arquivo e cria um novo arquivo com o mesmo nome
    # escreve o conteúdo dos pacotes recebidos no novo arquivo
    num_pacotes = 0
    with open('pasta/' + fileName, 'wb') as f:
        msg, _ = clientSocket.recvfrom(bufferSize)
        num_pacotes+=1

        while True:
            f.write(msg)
            msg, cliente = clientSocket.recvfrom(bufferSize)
            num_pacotes+=1
            if msg == b'':
                f.write(b'')
                num_pacotes+=1
                break
        print("Número de pacotes recebidos: ", num_pacotes)

    print(f"Arquivo {fileName} retornado com sucesso!")
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

clientSocket.close()
