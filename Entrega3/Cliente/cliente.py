# Módulo cliente.py: Responsável por enviar os arquivos para o servidor e
#                    receber os arquivos renomeados do servidor, salvando-os na pasta

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o servidor
import os #importa a biblioteca do sistema para criação do diretório para salvamento de arquivos no servidor
from random import random # importa a função random para geração de perda de pacotes aleatória
from time import time # importa a função time para temporização de retransmição do pacote (rdt3.0)
import threading

SERVER_NAME = 'localhost' # nome do servidor
SERVER_PORT = 12001 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)
HEADER_SIZE = 1 # tamanho do cabeçalho
PACKET_ERROR_RATE = 0.005 # taxa média de pacotes que serão perdidos ou serão corrompidos

COMMAND_LIST = """
\033[1;32;43m=-=-=-=-=-=-=-=LISTA DE COMANDOS=-=-=-=-=-=-=-=\033[m
\033[32mConectar ao sistema - \x1B[3mlogin <nome_do_usuario>\x1B[0m
\033[32mDar um Lance - \x1B[3mbid <id_item> <valor>\x1B[0m
\033[32mVer itens e preços - \x1B[3mlist\x1B[0m
\033[32mVer quem está ganhando - \x1B[3mstatus\x1B[0m
\033[32mSair do sistema - \x1B[3mlogout\x1B[0m \033[m
\033[1;32;43m=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\033[m
"""

class Client:
    def __init__(self, server_name, server_port, buffer_size, header_size):
        self.server_name = server_name
        self.server_port = server_port
        self.buffer_size = buffer_size
        self.header_size = header_size
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # cria o socket UDP do cliente
        self.seqnumber_rcv = 0
        self.acknumber_snd = 1
        self.data_size = buffer_size - header_size
        self.package_number = 0
        
        self.list_items = []
    
    # Cria o diretório para armazenar os arquivos
    def create_dir(self, dir_name: str = "pasta_clientes"):
        # Verifica se não existe antes de criar
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"'{dir_name}' criada.")
        else:
            print(f"'{dir_name}' já existe.")

        print('O servidor está pronto para receber conexões!')

    # Cria o segmento a partir do número de sequência (cabeçalho) e os dados
    # Foi implementada uma possível geração de erro no pacote, alternando o bit de sequência
    def create_segment(self, data):
        sequence_number_b = self.sequence_number.to_bytes(1)
        
        # Condicional para corromper o pacote na taxa média de erro estabelecida
        if (random() < PACKET_ERROR_RATE):
            false_number = (self.sequence_number + 1) % 2
            sequence_number_b = false_number.to_bytes(1)
        
        return sequence_number_b + data

    # Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data)
            self.socket.sendto(segment, (self.server_name, self.server_port))
    
    # Envia um segmento e aguarda o ACK correspondente (Stop-and-Wait).
    def send_rec_segment(self, data, timeout):
        initial_timeout = timeout   # i guarda o timeout inicial
        send_time = time() # recebe o tempo atual
        self.send_segment(data)
        
        # Laço que realiza a transmissão e retransmissão conforme rdt3.0
        while True:
            try:
                self.socket.settimeout(timeout)                  # guarda o timeout inicial
                ack, _ = self.socket.recvfrom(self.buffer_size)  # registra o tempo da primeira tentativa de envio   
                ack_number = ack[0]
                msg = ack[1:]
                
                # Condicional para verificar se o ack recebido é o ack esperado
                if ack_number == self.sequence_number and (msg == '' or msg == None):                      
                    self.sequence_number = (self.sequence_number + 1) % 2   # troca para o próximo num de sequencia esperada
                    self.package_number += 1
                    
                    break
                else:
                    elapsed_time = time() - send_time                                   # calcula o tempo desde a primeira tentativa
                    timeout = initial_timeout - elapsed_time                            # calcula o tempo restante para receber o ack esperado
                    print(f"ACK errado recebido, tempo restante de timeout: {timeout}") # foi feita essa implementação porque o recvfrom recebe qualquer ack
                                                                                        # seja ele o correto ou não
                    # gera uma exceção se o timeout se esgotar
                    if timeout <= 0:
                        raise socket.timeout

            # Em caso de exceção de tempo, retransmite o pacote e reinicia todo o processo
            except socket.timeout:
                print(f"Timeout! Retransmitindo o pacote {self.package_number + 1}") # o primeiro pacote começa com 1
                timeout = initial_timeout
                self.send_segment(data)

    # Método geral para transmissão e retransmissão de pacotes utilizando rdt3.0
    def send(self, command):
        self.sequence_number = 0
        self.package_number = 0
        self.send_rec_segment(command.encode(), .1)

    # Envia um ACK ao servidor confirmando o recebimento do último pacote
    def send_ack(self):
        self.socket.sendto(self.ack_number.to_bytes(1), (self.server_name, self.server_port))
    
    # Recebe um segmento do servidor e separa o cabeçalho dos dados
    def extract_segment(self):
        msg, _ = self.socket.recvfrom(self.buffer_size)
        return msg[0], msg[1:]
    
    # Recebe um segmento novo do servidor, descartando duplicatas (Stop-and-Wait receptor).
    def extract_rec_segment(self):
        while True:
            seq_server_number, data = self.extract_segment()
            if data is None: # vê se é ack
                continue
            
            # Caso o ack seja esperado, ou seja, diferente do pacote recebido anteriormente
            if seq_server_number != self.ack_number:
                self.ack_number = seq_server_number
                self.package_number += 1
                
                print(f"Pacote {self.package_number} recebido corretamente")
                
                # manda o ack respectivo
                self.send_ack()
                return data
            
            # reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
            # que o ultimo pacote recebido antes dele
            else:
                print("Pacote esperado não foi recebido, enviando ack...")
                
                self.send_ack()
    
    #
    def receive(self, save: bool = False):
        self.ack_number = 1
        self.sequence_number = 0
        self.package_number = 0 # reseta o contador de pacotes recebidos
        self.socket.settimeout(100)

        msg = self.extract_rec_segment()

        if not save:
            return msg
        
        file_renamed = msg
        
        ## Rotina que recebe os pacotes do arquivo renomeado enviado pelo servidor e escreve o conteúdo em um novo arquivo (com nome novo)
        with open('pasta/pasta_' + file_renamed.decode(), 'wb') as file:
            ## Laço que recebe os pacotes do arquivo renomeado enviado pelo servidor enquanto houver conteúdo para ler, escrevendo o conteúdo dos pacotes recebidos no novo arquivo criado
            while True:
                data = self.extract_rec_segment()
                
                if data == b'': # condição que sinaliza o fim do arquivo renomeado enviado pelo servidor
                    break
                else:
                    file.write(data) # escreve o conteúdo do pacote recebido no novo arquivo criado

            print(f"Arquivo {file_renamed.decode()} retornado com sucesso!")
            print(f"Número de pacotes recebidos e reconhecidos: {self.package_number}")
            print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        return
    
    # Recebe um arquivo completo enviado pelo servidor e salva na pasta local.
    def receive_str(self):
        self.ack_number = 1
        self.sequence_number = 0
        self.socket.settimeout(100)

        return self.extract_rec_segment().decode()
    
    # Fecha o socket após o envio e recebimento de todos os arquivos
    def close(self):
        self.socket.close()

    ## Laço principal para enviar e receber os arquivos
    def run_sender(self):
        self.create_dir()
        
        while True:
            # ------ THREAD 1 ------
            command = input("Insira o comando: ")
            self.send(command)

            if command == "exit":
                break
            # -----------------------

    def run_receiver(self):
         while True:
            # ------ THREAD 2 -------
            answer = self.receive()
            print(answer)
            
            if answer == "você está online":
                self.create_dir("pasta")
            
            if answer == "você arrematou um item":
                self.receive(save = True)
            # -----------------------
            

"""
            match command.split()[0]:
                case "help":
                    print(COMMAND_LIST)
                case "login":
                    print(f"Usuário \x1B[3m{self.client_name}\x1B[0m conectado!")
                    print(f"Usuário \x1B[3m{self.client_name}\x1B[0m não conectado!")
                case "bid":
                    print(f"Usuário fez o lance no item {id_item} com valor R${val}")
                    print("Usuário não conseguiu fazer o lance")
                case "list":
                    
                case "status":
                    
                case "logout":
                    print(f"Usuário \x1B[3m{self.client_name}\x1B[0m não conectado!")
                    print("Desfazendo a conexão...")
                case _:
                    print("Comando desconhecido (digite \x1B[3mhelp\x1B[0m para ver lista de comandos)")
"""    

    
# Criação do cliente e do seu socket
client = Client(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)
sender = threading.Thread(target=client.run_sender)
receiver = threading.Thread(target=client.run_receiver)

sender.start()
receiver.start()

sender.join()
receiver.join()

client.close()
