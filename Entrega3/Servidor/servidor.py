# Módulo servidor.py: Responsável por receber os arquivos enviados pelo cliente, salvando-os na pasta,
#                     renomear esses arquivos e retorná-los para o cliente

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o cliente
import os #importa a biblioteca do sistema para criação do diretório para salvamento de arquivos no servidor
from random import random # importa a função random para geração de perda de pacotes aleatória
from time import time # importa a função time para temporização de retransmição do pacote (rdt3.0)
import threading

SERVER_NAME = 'localhost'
SERVER_PORT = 12001 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)
HEADER_SIZE = 2 # tamanho do cabeçalho
PACKET_ERROR_RATE = 0.005 # taxa média de pacotes que serão perdidos ou serão corrompidos

COMMAND_LIST = """
\033[1;32;43m=-=-=-=-=-=-=-=LISTA DE COMANDOS=-=-=-=-=-=-=-=\033[m
\033[32mConectar ao sistema - \x1B[3mlogin <nome_do_usuario>\x1B[0m
\033[32mDar um Lance - \x1B[3mbid <id_item> <valor>\x1B[0m
\033[32mVer itens e preços - \x1B[3mlist\x1B[0m
\033[32mVer quem está ganhando - \x1B[3mstatus\x1B[0m
\033[32mSair do leilão - \x1B[3mlogout\x1B[0m \033[m
\033[32mSair do sistema - \x1B[3mexit\x1B[0m \033[m
\033[1;32;43m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[m
"""

items_name = ["caderno", "carro", "celular", "computador", "geladeira"]

class Item:
    def __init__(self, id, name, highest_bidder, top_val, counter, ip_port, timeout):
        self.id = id
        self.name = name
        self.highest_bidder = highest_bidder
        self.top_val = 0
        self.counter = 0
        self.destiny = ip_port
        self.timeout = timeout 

class Server:
    def __init__(self, server_name, server_port, buffer_size, header_size):
        self.server_name = server_name
        self.server_port = server_port
        self.buffer_size = buffer_size
        self.header_size = header_size
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # cria o socket UDP do cliente
        self.sequence_number = 0
        self.ack_number = 1
        self.data_size = buffer_size - header_size
        self.package_number = 0

        self.client_list = {}
        self.items_list = {}

        self.socket.bind(('', SERVER_PORT)) # vincula o socket à porta definida
        self.create_dir()
        self.create_items_list()

        self.send_buffer = []

        self.ack_correct = False


    def create_items_list(self):
        index = 0
        for i in items_name:
            item = Item(index, i, None, None, 0, None, time() + 60)
            self.items_list[index] = item
            index += 1


    # Cria o diretório para armazenar os arquivos
    def create_dir(self, dir_name = "pasta"):
        # Verifica se não existe antes de criar
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"Pasta criada.")
        else:
            print(f"Pasta já existe.")

        print('O servidor está pronto para receber conexões!')
    

    # Envia um ACK ao cliente confirmando o recebimento do último pacote.      
    def send_ack(self, client):
        self.socket.sendto(self.ack_number.to_bytes(1), client)


    # Recebe um segmento do cliente e separa o cabeçalho dos dados.
    def extract_segment(self):
        msg, client = self.socket.recvfrom(self.buffer_size)
        return msg, client


    # Gerenciamento do recebimento e o envio da confirmação
    def extract_rec_segment(self):
        while True:
            msg, client = self.extract_segment()

            if len(msg) > 1:
                sequence_number, data = msg[0], msg[1:]

                if sequence_number != self.ack_number:
                    self.ack_number = (self.ack_number + 1) % 2
                    self.package_number += 1
                
                    print(f"Pacote {self.package_number} recebido corretamente")
                    self.send_ack(client)
                    
                    return data.decode(), client
                
                # Reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
                # que o ultimo pacote recebido antes dele
                else:
                    print("Pacote esperado não foi recebido, enviando ack...")
                    self.send_ack(client)
            else:
                ack_number = msg[0]
                if ack_number == self.sequence_number:
                    self.ack_correct = True


    # Recebe um arquivo completo enviado pelo cliente, renomeia, reenvia e salva na pasta local.
    def receive(self):
        self.package_number = 0 # reseta o contador de pacotes recebidos
        self.socket.settimeout(None) # Trava o temporizador até receber o próximo pacote

        # primeiro pacote contém o nome do arqui
        command, client_ip_port = self.extract_rec_segment()

        self.socket.settimeout(10)
        
        return command, client_ip_port


    # Cria o segmento a partir do número de sequência (cabeçalho) e os dados
    # Foi implementada uma possível geração de erro no pacote, alternando o bit de sequência
    def create_segment(self, data, isFile):
        sequence_number_b = self.sequence_number.to_bytes(1)
        isfile = isFile.to_bytes(1)
        
        # Condicional para corromper o pacote na taxa média de erro estabelecida
        if (random() < PACKET_ERROR_RATE):
            false_number = (self.sequence_number + 1) % 2
            sequence_number_b = false_number.to_bytes(1)
        print("DATA QUE VAI ENCODE: ", data)

        if isinstance(data, str):
            data = data.encode()
        return sequence_number_b + isfile + data


    #Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data: str, client, isFile):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data, isFile)
            self.socket.sendto(segment, client)


    # Envia um segmento e aguarda o ACK correspondente (Stop-and-Wait).
    def send_rec_segment(self, data: str, client, timeout, isFile):
        send_time = time() # recebe o tempo atual
        self.send_segment(data, client, isFile)

        while True:
            if self.ack_correct:
                self.sequence_number = (self.sequence_number+1) % 2
                self.package_number += 1
                self.ack_correct = False
                break
            elif time() - send_time >= timeout:
                print(f"Timeout! Retransmitindo o pacote {self.package_number + 1}")
                send_time = time()
                self.send_segment(data, client, isFile)
                

    # Envia um arquivo renomeado de volta ao cliente em múltiplos pacotes.
    def send(self, data, client_ip_port, isFile):
        self.package_number = 0 # reseta o contador de pacotes enviados

        if isFile:
             with open('pasta/' + data + '.txt', 'rb') as file:
                file_name = data + '.txt'

                self.send_rec_segment(file_name, client_ip_port, .1, isFile)

                package = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer

                ## Laço que envia os pacotes do arquivo para o cliente enquanto houver conteúdo para ler
                while package:
                    self.send_rec_segment(package, client_ip_port, .1, isFile) # envia o pacote para o cliente
                    package = file.read(self.data_size) # lê o próximo pacote do arquivo até o final do arquivo

                self.send_rec_segment('EOF', client_ip_port, .1, isFile) # envia o caractere null para sinalizar o cliente do fim do arquivo

                print(f"Arquivo {file_name} retornado com sucesso!")
                print(f"Número de pacotes enviados: {self.package_number}")
                print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        else:
            for i in range(0, len(data), self.data_size):
                package = data[i:i+self.data_size]
                self.send_rec_segment(package, client_ip_port, .1, isFile) # envia o pacote para o cliente

    def sender(self):
        while True:
            if self.send_buffer:
                print(self.send_buffer[0])
                data, client_ip_port, isFile = self.send_buffer.pop(0)
                self.send(data, client_ip_port, isFile)
            else:
                continue

    def receiver(self):
        while True:
            # ------ THREAD 2 -------
            try:
                 command, client_ip_port = self.receive()
            except socket.timeout:
                continue
           
            print(command)
            
            match command.split()[0]:
                case "help":
                    self.send_buffer.append((COMMAND_LIST, client_ip_port, False))
                    #self.send(COMMAND_LIST, client_ip_port, False)
                case "login":
                    client_name = command.split()[1]

                    if client_name not in self.client_list.values():
                        self.client_list[client_ip_port] = client_name
                        
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m conectado!")
                        self.send_buffer.append(('voce esta online', client_ip_port, False))
                    else:
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m já existente!")
                        
                        
                case "bid":
                    print(f"{self.client_list[client_ip_port]} fez o lance no item {command.split()[1]} com valor R${command.split()[2]}")
                    _, item_id, value = command.split()
                    if (item_id == '' or value == ''):
                        print("Bid incompleto! (Formato inválido)")
                        continue

                    item_id = int(item_id)
                    value = int(value)

                    print(self.items_list)
                    print(self.client_list)

                    if item_id in self.items_list:
                        item = self.items_list[item_id]
                        value_now = item.top_val
                        if (value > value_now):
                            item.counter += 1
                            item.top_val = value
                            item.highest_bidder = self.client_list[client_ip_port]
                            item.destiny = client_ip_port
                            self.send_buffer.append(("Lance dado com sucesso!", client_ip_port, False))

                            for client in self.client_list:
                                print(client)
                                if client != client_ip_port:
                                    bid = 'Lance de' + (float)(item.value) +  'R$ dado para o' + item.name + ', por ' + item.higgest_bidder + '\n'
                                    self.send_buffer.append((bid, client, False))
                        else:
                            self.send_buffer.append(("Lance invalido, arranje mais dinheiro", client_ip_port, False))
                    else:
                        self.send_buffer.append(("O item especificado não está em leilão", client_ip_port, False))
                
                    
                case "list":
                    list = 'Id_item  |  Item_name  |  valor  \n'
                    for item_id in self.items_list:
                        item = self.items_list[item_id]
                        list += str(item.id) + '  |  ' + item.name + '  |  ' + str(item.top_val) + '\n'

                    self.send_buffer.append((list, client_ip_port, False))
                    
                case "status":
                    ranking = "Id_item  |  Item_name  |  maior_lance  |  valor_lance  \n"
                    for item_id in self.items_list:
                        item = self.items_list[item_id]
                        ranking += str(item.id) + '  |  ' + item.name + '  |  ' + str(item.highest_bidder) + '  |  ' + str(item.top_val) + '\n'
                    
                    self.send_buffer.append((ranking, client_ip_port, False))
                    
                case "logout":
                    print("Desfazendo a conexão...")
                    self.send_buffer.append(("voce esta offline", client_ip_port, False))
                    self.client_list.pop(client_ip_port)
                    
                case _:
                    print("Comando desconhecido (digite \x1B[3mhelp\x1B[0m para ver lista de comandos)")
                    
    
    def advertisement(self):
        while True:
            id_removed = -1
            for item_id in self.items_list:
                item = self.items_list[item_id]
                if ((item.counter == 5 or time() >= item.timeout) and item.destiny is not None): #OU TIMER
                    self.send_buffer.append((item.name, item.destiny, True))
                    id_removed = item_id

            if(id_removed != -1):
                self.items_list.pop(id_removed)   


    # Executa o loop principal do servidor, processando arquivos indefinidamente
    def main(self):
        sender = threading.Thread(target = self.sender)
        run = threading.Thread(target = self.receiver)
        advertisement = threading.Thread(target = self.advertisement)

        sender.start()
        run.start()
        advertisement.start()

        sender.join()
        run.join()
        advertisement.join()


server = Server(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)
server.main()