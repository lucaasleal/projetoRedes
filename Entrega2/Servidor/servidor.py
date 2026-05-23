# Módulo servidor.py: Responsável por receber os arquivos enviados pelo cliente, salvando-os na pasta,
#                     renomear esses arquivos e retorná-los para o cliente


import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o cliente
import os #importa a biblioteca do sistema para criação do diretório para salvamento de arquivos no servidor

SERVER_NAME = 'localhost'
SERVER_PORT = 12000 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)
HEADER_SIZE = 1

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

        self.socket.bind(('', SERVER_PORT)) # vincula o socket à porta definida
        self.create_dir()
    
    def create_dir(self):
        dir_name = "pasta"

        # Verifica se não existe antes de criar
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"'{dir_name}' criada.")
        else:
            print(f"'{dir_name}' já existe.")

        print('O servidor está pronto para receber conexões!')

    def send_ack(self, client):        
        self.socket.sendto(self.ack_number.to_bytes(1), client)
    
    def extract_segment(self):
        msg, client = self.socket.recvfrom(self.buffer_size) # recebe o nome do arquivo renomeado pelo servidor

        return msg[0], msg[1:], client

    def extract_rec_segment(self):
        while True:
            sequence_number, data, client = self.extract_segment()

            if sequence_number != self.ack_number:
                self.ack_number = sequence_number
                self.package_number += 1

                self.send_ack(client)
                
                return data, client
            else:
                self.send_ack(client)
                

    def receive_file(self):
        self.sequence_number = 0
        self.package_number = 0 # reseta o contador de pacotes recebidos

        file_name, client = self.extract_rec_segment()
        print(file_name)
        file_name = file_name.decode()
        
        ## Rotina que recebe os pacotes do arquivo enviado pelo cliente e escreve o conteúdo em um novo arquivo
        with open('pasta/' + file_name, 'wb') as file:
            ## Laço que recebe os pacotes do arquivo enviado pelo cliente enquanto houver conteúdo para ler, escrevendo o conteúdo dos pacotes no novo arquivo criado
            while True:
                data, client = self.extract_rec_segment()
                
                if data == b'': # condição que sinaliza o fim do arquivo enviado pelo cliente                    
                    break
                else:
                    file.write(data) # escreve o conteúdo do pacote recebido no novo arquivo criado

            print(f"Número de pacotes recebidos: {self.package_number}")
            print(f"Arquivo {file_name} recebido com sucesso!")
        
        return client, file_name


    def create_segment(self, data: str):
        sequence_number_b = self.sequence_number.to_bytes(1)
        return sequence_number_b + data
    

    def send_segment(self, data: str, client):
        segment = self.create_segment(data)
        self.socket.sendto(segment, client)
    
    
    def send_rec_segment(self, data: str, client):
        self.send_segment(data, client)
        self.socket.settimeout(0.1);
        
        while True:
            try:
                ack, _ = self.socket.recvfrom(self.buffer_size)
                ack_number = ack[0]
                
                if ack_number == self.sequence_number:
                    self.sequence_number = (self.sequence_number + 1) % 2
                    self.package_number += 1
                    
                    break
                else:
                    self.send_segment(data, client)
            except:
                self.send_segment(data, client)

    def send_file(self, client, fileName):
        ## RETORNO DOS ARQUIVOS
        self.package_number = 0 # reseta o contador de pacotes enviados

        ## Rotina que abre o arquivo para leitura em modo binário, renomea-o e envia-o em pacotes para o cliente
        with open('pasta/' + fileName, 'rb') as file:
            file_renamed = 'leilao_' + fileName ## tratamento para enviar o arquivo renomeado para o cliente

            self.send_rec_segment(file_renamed.encode(), client)

            package = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer

            ## Laço que envia os pacotes do arquivo para o cliente enquanto houver conteúdo para ler
            while package:
                self.send_rec_segment(package, client) # envia o pacote para o cliente
                package = file.read(self.data_size) # lê o próximo pacote do arquivo até o final do arquivo

            self.send_rec_segment(b'', client) # envia o caractere null para sinalizar o cliente do fim do arquivo

            print(f"Arquivo {file_renamed} retornado com sucesso!")
            print(f"Número de pacotes enviados: {self.package_number}")
            print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
    

    def run(self):
        while True:
            client, file_name = self.receive_file()
            self.send_file(client, file_name)
        
        

server = Server(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)

server.run()
