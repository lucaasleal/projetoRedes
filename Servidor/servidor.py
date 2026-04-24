from socket import *

serverPort = 12000
serverSocket = socket(AF_INET, SOCK_DGRAM)
serverSocket.bind(('', serverPort))
bufferSize = 1024

print('The server is ready to receive')

while True:

    # recebe o nome do arquivo
    msg, servidor = serverSocket.recvfrom(bufferSize)
    
    # cria o novo nome do arquivo (ex: arquivo_leilao.txt)
    fileName = msg.decode().split('.')[0] + '_leilao.txt'
    
    # recebe o conteúdo do arquivo e cria um novo arquivo com o mesmo nome
    # escreve o conteúdo dos pacotes recebidos no novo arquivo
    with open(fileName, 'wb') as f:
        msg, servidor = serverSocket.recvfrom(bufferSize)
        while True:
            print("msg: ", msg)
            f.write(msg)
            msg, servidor = serverSocket.recvfrom(bufferSize)
            if msg == b'':
                f.write(b'')
                break
    
    