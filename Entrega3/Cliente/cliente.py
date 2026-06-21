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
HEADER_SIZE = 2 # tamanho do cabeçalho
PACKET_ERROR_RATE = 0.005 # taxa média de pacotes que serão perdidos ou serão corrompidos

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

        self.client_name = ""
        self.online = False
        self.login_logout_request = False
        self.exit = False

        self.ack_correct = False

    # Cria o diretório para armazenar os arquivos
    def create_dir(self, dir_name):
        # Verifica se não existe antes de criar
        if not os.path.exists(f"pasta_{dir_name}"):
            os.makedirs(f"pasta_{dir_name}")
            print("Pasta criada.")
        else:
            print("Pasta já existe.")

        print(f"Cliente {dir_name} está pronto para estabelecer conexões!")

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
        send_time = time() # recebe o tempo atual
        self.send_segment(data)
        
        # Laço que realiza a transmissão e retransmissão conforme rdt3.0
        while True:
            if self.ack_correct:
                self.sequence_number = (self.sequence_number+1) % 2
                self.package_number += 1
                self.ack_correct = False
                break
            elif time() - send_time >= timeout:
                print(f"Timeout! Retransmitindo o pacote {self.package_number + 1}")
                send_time = time()
                self.send_segment(data)

    # Método geral para transmissão e retransmissão de pacotes utilizando rdt3.0
    def send(self, command):
        self.package_number = 0
        self.send_rec_segment(command.encode(), .1)

    # Envia um ACK ao servidor confirmando o recebimento do último pacote
    def send_ack(self):
        self.socket.sendto(self.ack_number.to_bytes(1), (self.server_name, self.server_port))
    
    # Recebe um segmento do servidor e separa o cabeçalho dos dados
    def extract_segment(self):
        msg, _ = self.socket.recvfrom(self.buffer_size)
        return msg

    # Recebe um segmento novo do servidor, descartando duplicatas (Stop-and-Wait receptor).
    def extract_rec_segment(self):
        while True:
            msg = self.extract_segment()
            
            # Caso o ack seja esperado, ou seja, diferente do pacote recebido anteriormente
            if len(msg) > 2:
                seq_server_number, isFile, data = msg[0], msg[1], msg[2:]
                
                if seq_server_number != self.ack_number:
                    self.ack_number = (self.ack_number + 1) % 2
                    self.package_number += 1

                    print(f"Pacote {self.package_number} recebido corretamente")
                    self.send_ack()

                    return data.decode(), isFile
            
                # reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
                # que o ultimo pacote recebido antes dele
                else:
                    print("Pacote esperado não foi recebido, enviando ack...")
                    
                    self.send_ack()
            else:

                ack_number = msg[0]

                if ack_number == self.sequence_number:
                    self.ack_correct = True
    
    # Recebe um arquivo completo enviado pelo servidor e salva na pasta local.
    def receive(self):
        self.package_number = 0 # reseta o contador de pacotes recebidos
        self.socket.settimeout(1)

        msg, isFile = self.extract_rec_segment()

        if not isFile:
            return msg

        file_name = msg
        
        ## Rotina que recebe os pacotes do arquivo renomeado enviado pelo servidor e escreve o conteúdo em um novo arquivo (com nome novo)
        with open('pasta_' + self.client_name + '/' + file_name, 'wb') as file:
            ## Laço que recebe os pacotes do arquivo renomeado enviado pelo servidor enquanto houver conteúdo para ler, escrevendo o conteúdo dos pacotes recebidos no novo arquivo criado
            while True:
                data, _  = self.extract_rec_segment()
                
                if data == 'EOF': # condição que sinaliza o fim do arquivo renomeado enviado pelo servidor
                    break

            print(f"Arquivo {file_name} retornado com sucesso!")
            print(f"Número de pacotes recebidos e reconhecidos: {self.package_number}")
            print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
        return

    ## Laço principal para enviar e receber os arquivos
    def run_sender(self):
        while True:
            # ------ THREAD 1 ------
            command = input("Insira o comando: ")
            
            if command == "exit":
                self.exit = True
                print("Saindo do sistema...")
                break

            if not self.login_logout_request:
                if not self.online:
                    if command.split()[0] == "login":
                        self.login_logout_request = True
                        self.client_name = command.split()[1]
                        self.send(command) 
                    else:
                        print("Comando inválido.")
                else:
                    if "login" in command:
                        print("Usuário já conectado.")
                        continue

                    if command == "logout":
                        self.login_logout_request = True
                    
                    self.send(command)
            else:
                print("Aguarde a resposta do comando anterior.")


    def run_receiver(self):
         while True:
            # ------ THREAD 2 -------
            try:
                answer = self.receive()
            except socket.timeout:
                continue

            print(f"server mandou: {answer}\n")
            
            if self.exit and not self.login_logout_request:
                break

            match answer:
                case "voce esta online":
                    self.create_dir(self.client_name)
                    
                    self.online = True
                    self.login_logout_request = False
                case "voce esta offline":
                    self.client_name = ""

                    self.online = False
                    self.login_logout_request = False
                
    

    def run(self):
        sender = threading.Thread(target = self.run_sender)
        receiver = threading.Thread(target = self.run_receiver)

        sender.start()
        receiver.start()

        sender.join()
        receiver.join()


    # Fecha o socket após o envio e recebimento de todos os arquivos
    def close(self):
        self.socket.close()

    
# Criação do cliente e do seu socket
client = Client(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)

client.run()
client.close()
