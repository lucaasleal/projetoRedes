from socket import *

serverName = 'localhost'
serverPort = 12000
clientSocket = socket(AF_INET, SOCK_DGRAM)

bufferSize = 1024

fileName = "pasta/arquivo.txt"
with open(fileName, 'rb') as f:
    pct = f.read(bufferSize)
    while pct:
        print("Palavra: ", pct)
        clientSocket.sendto(pct, (serverName, serverPort))
        pct = f.read(bufferSize)

print("Arquivo retornado com sucesso!")
clientSocket.close()
