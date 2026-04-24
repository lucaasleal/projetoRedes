from socket import *
import os

serverPort = 12000
serverSocket = socket(AF_INET, SOCK_DGRAM)
serverSocket.bind(('', serverPort))
fileName = "arquivo_serv.txt"
bufferSize = 1024

print('The server is ready to receive')

while True:
    with open(fileName, 'wb') as f:
        while True:
            msg, servidor = serverSocket.recvfrom(bufferSize)
            print("msg: ", msg)
            if msg == b'EOF':
                break
            f.write(msg)