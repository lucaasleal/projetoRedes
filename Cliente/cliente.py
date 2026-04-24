from socket import *

serverName = 'localhost'
serverPort = 12000
clientSocket = socket(AF_INET, SOCK_DGRAM)

bufferSize = 1024

fileName = "arquivo.txt"

with open('pasta/' + fileName, 'rb') as f:

    # envia o nome do arquivo para ser renomeado no servidor
    clientSocket.sendto(fileName.encode(), (serverName, serverPort))

    pct = f.read(bufferSize)
    while pct:
        print("Palavra: ", pct)
        clientSocket.sendto(pct, (serverName, serverPort))
        pct = f.read(bufferSize)
    
    # envia o caracter null para sinalizar o servidor o fim do arquivo
    clientSocket.sendto(b'', (serverName, serverPort))

print("Arquivo retornado com sucesso!")
clientSocket.close()
