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

ACCEPT_MSG = "voce esta online"

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

class Item:
    def __init__(self, item_id, item_name, top_client, top_val, init_time, counter):
        self.item_id = item_id
        self.item_name = item_name
        self.top_client = top_client
        self.top_val = top_val
        self.init_time = init_time
        self.counter = counter


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
        
        return msg[0], msg[1:], client

    # Gerenciamento do recebimento e o envio da confirmação
    def extract_rec_segment(self):
        while True:
            sequence_number, data, client = self.extract_segment()

            # Caso o ack seja esperado, ou seja, diferente do pacote recebido anteriormente
            if sequence_number != self.ack_number:
                self.ack_number = (self.ack_number + 1) % 2
                self.package_number += 1
                
                print(f"Pacote {self.package_number} recebido corretamente")

                # manda o ack respectivo
                self.send_ack(client)
                
                return data.decode(), client
            
            # Reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
            # que o ultimo pacote recebido antes dele
            else:
                print("Pacote esperado não foi recebido, enviando ack...")

                self.send_ack(client)

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
        
        return sequence_number_b + isfile + data.encode()

    #Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data: str, client, isFile):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data, isFile)
            self.socket.sendto(segment, client)

    # Envia um segmento e aguarda o ACK correspondente (Stop-and-Wait).
    def send_rec_segment(self, data: str, client, timeout, isFile):
        initial_timeout = timeout
        send_time = time() # recebe o tempo atual
        self.send_segment(data, client, isFile)
        
        # Laço que realiza a transmissão e retransmissão conforme rdt3.0
        while True:
            try:
                self.socket.settimeout(timeout)                   # guarda o timeout inicial
                ack, _ = self.socket.recvfrom(self.buffer_size)   # registra o tempo da primeira tentativa de envio   
                ack_number, data_server = ack[0], ack[1:]
                
                # verifica se o ack recebido é o ack esperado
                if ack_number == self.sequence_number and data_server.decode() == '':                      
                    self.sequence_number = (self.sequence_number + 1) % 2   # troca para o próximo num de sequencia esperada
                    self.package_number += 1
                    break
                
                # caso o ack não for o esperado
                else:
                    elapsed_time = time() - send_time                                     # calcula o tempo desde a primeira tentativa
                    timeout = initial_timeout - elapsed_time                              # calcula o tempo restante para receber o ack esperado
                    print(f"ACK errado recebido, tempo restante de timeout: {timeout}")   # foi feita essa implementação porque o recvfrom recebe qualquer ack
                                                                                          # seja ele o correto ou não
                    # gera uma exceção se o timeout se esgotar
                    if timeout <= 0:
                        raise socket.timeout
            
            # envia o pacote novamente, reiniciando todo o processo
            except socket.timeout:                                                     
                print(f"Timeout! Retransmitindo o pacote {self.package_number + 1}") # o primeiro pacote começa com 1
                timeout = initial_timeout                                               
                send_time = time()                                         
                self.send_segment(data, client, isFile)


    # Envia um arquivo renomeado de volta ao cliente em múltiplos pacotes.
    def send(self, data, client_ip_port, isFile):
        self.package_number = 0 # reseta o contador de pacotes enviados

        if isFile:
            ## Rotina que abre o arquivo para leitura em modo binário, renomea-o e envia-o em pacotes para o cliente
            with open('pasta/' + data + 'txt', 'rb') as file:
                file_name = data + '.txt'

                self.send_rec_segment(file_name, client_ip_port, .1, isFile)

                package = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer

                ## Laço que envia os pacotes do arquivo para o cliente enquanto houver conteúdo para ler
                while package:
                    self.send_rec_segment(package, client_ip_port, .1, isFile) # envia o pacote para o cliente
                    package = file.read(self.data_size) # lê o próximo pacote do arquivo até o final do arquivo

                self.send_rec_segment(b'', client_ip_port, .1, isFile) # envia o caractere null para sinalizar o cliente do fim do arquivo

                print(f"Arquivo {file_name} retornado com sucesso!")
                print(f"Número de pacotes enviados: {self.package_number}")
                print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        else:
            for i in range(0, len(data), self.data_size):
                package = data[i:i+self.data_size]
                self.send_rec_segment(package, client_ip_port, .1, isFile) # envia o pacote para o cliente

    '''def run_sender(self):
        while True:
            # ------ THREAD 1 ------
            
            if command == "exit":
                self.exit = True
                break

            if not self.login_logout_request:
                if not self.online:
                    if command.split()[0] == "login":
                        self.login_logout_request = True
                        self.send(command)
                    else:
                        print("Tentativa de login inválida.")
                else:
                    if command == "logout":
                        self.login_logout_request = True
                    
                    self.send(command)
            else:
                print("Aguarde a resposta do comando anterior.")
            # -----------------------'''

    def run(self):
        while True:
            # ------ THREAD 2 -------
            command, client_ip_port = self.receive()
            print(command)
            
            match command.split()[0]:
                case "help":
                    self.send(COMMAND_LIST, client_ip_port, False)
                case "login":
                    client_name = command.split()[1]
                    if client_name not in self.client_list.values():
                        self.client_list[client_ip_port] = client_name
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m conectado!")
                        self.send(ACCEPT_MSG, client_ip_port, False)
                    else:
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m já existente!")
                case "bid":
                    print(f"Usuário fez o lance no item {command.split()[1]} com valor R${command.split()[2]}")
                    print("Usuário não conseguiu fazer o lance")
                #case "list":
                    
                #case "status":
                    
                case "logout":
                    print("Desfazendo a conexão...")
                    self.send("voce esta offline", client_ip_port, False)
                    self.client_list.pop(client_ip_port)
                case _:
                    print("Comando desconhecido (digite \x1B[3mhelp\x1B[0m para ver lista de comandos)")
            # -----------------------

    # Executa o loop principal do servidor, processando arquivos indefinidamente
    def main(self):
        #sender = threading.Thread(target = self.run_sender)
        run = threading.Thread(target = self.run)

        
        run.start()

        run.join()


server = Server(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)
server.main()
