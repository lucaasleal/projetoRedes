from socket import *

serverName = 'localhost'
serverPort = 12000
clientSocket = socket(AF_INET, SOCK_DGRAM)

message = input('digita ai lucas:')
clientSocket.sendto(message.encode(), (serverName, serverPort))

modifiedMessage, serverAddres = clientSocket.recvfrom(2048)
print(modifiedMessage.decode())

clientSocket.close()
