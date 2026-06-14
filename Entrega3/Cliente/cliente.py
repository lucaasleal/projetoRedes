# Módulo cliente.py: Responsável por enviar os arquivos para o servidor e
#                    receber os arquivos renomeados do servidor, salvando-os na pasta

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o servidor
from random import random # importa a função random para geração de perda de pacotes aleatória
from time import time # importa a função time para temporização de retransmição do pacote (rdt3.0)

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
        self.sequence_number = 0
        self.ack_number = 1
        self.data_size = buffer_size - header_size
        self.package_number = 0
        
        self.list_items = []
        self.client_name = None

            
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
                
                # Condicional para verificar se o ack recebido é o ack esperado
                if ack_number == self.sequence_number:                      
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
    def send_file(self, file_name: str):
        self.sequence_number = 0
        self.package_number = 0
        self.send_rec_segment(file_name.encode(), .1)
        
        ## Rotina que abre o arquivo para leitura em modo binário e envia-o em pacotes para o servidor
        with open('pasta/' + file_name, 'rb') as file:
            ## Laço que envia os pacotes do arquivo para o servidor enquanto houver conteúdo para ler
            while True:
                data = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer
                
                if data:
                    self.send_rec_segment(data, .1) # envia o pacote para o servidor
                else:
                    break
            
        self.send_rec_segment(b'', .1) # envia o caractere null para sinalizar o servidor do fim do arquivo

        print(f"Número de pacotes enviados e reconhecidos: {self.package_number}")

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
    def receive(self, save=False):
        self.ack_number = 1
        self.sequence_number = 0
        self.package_number = 0 # reseta o contador de pacotes recebidos
        self.socket.settimeout(100)

        msg = self.extract_rec_segment()

        if not save:
            return msg
        
        file_renamed = msg
        
        ## Rotina que recebe os pacotes do arquivo renomeado enviado pelo servidor e escreve o conteúdo em um novo arquivo (com nome novo)
        with open('pasta/' + file_renamed.decode(), 'wb') as file:
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
        return "salvo"
    
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
    def run(self):
        while True:
            command = input("Insira o comando: ")            
            self.send_file(command)
            
            match command.split()[0]:
                case "help":
                    print(COMMAND_LIST)
                case "login":
                    self.send_file(command)
                    
                    while True:
                        answer = self.receive()
                        
                        if answer == "você está online":
                            self.client_name = command.split()[1]
                            print(f"Usuário \x1B[3m{self.client_name}\x1B[0m conectado!")
                            break
                        elif answer == "você foi rejeitado":
                            print(f"Usuário \x1B[3m{self.client_name}\x1B[0m não conectado!")
                            break
                        else:
                            print(answer)
                case "bid":
                    self.send_file(command)
                    
                    while True:
                        answer = self.receive()
                    
                        if answer == "lance registrado":
                            id_item, val = command.split()[1], command.split[2]
                            print(f"Usuário fez o lance no item {id_item} com valor R${val}")
                            break
                        elif answer == "erro no lance":
                            print("Usuário não conseguiu fazer o lance")
                            break
                        else:
                            print(answer)
                    
                case "list":
                    self.send_file(command)
                    
                case "status":
                    self.send_file(command)
                case "logout":
                    if self.client_name is None:
                        print(f"Usuário \x1B[3m{self.client_name}\x1B[0m não conectado!")
                    else:
                        self.send_file(command)
                        print("Desfazendo a conexão...")
                case _:
                    print("Comando desconhecido (digite \x1B[3mhelp\x1B[0m para ver lista de comandos)")

        
        for fileName in self.list_files:
            ## ENVIO DOS ARQUIVOS ##
            self.send_file(fileName)

            ## RECEBIMENTO DOS ARQUIVOS ##
            self.receive()
    

    
# Criação do cliente e do seu socket
client = Client(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)
client.run()
