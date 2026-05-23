# Módulo cliente.py: Responsável por enviar os arquivos para o servidor e
#                    receber os arquivos renomeados do servidor, salvando-os na pasta

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o servidor
import time

SERVER_NAME = 'localhost' # nome do servidor
SERVER_PORT = 12000 # porta do servidor
BUFFER_SIZE = 1024 # tamanho do buffer para leitura dos arquivos (1KB)
HEADER_SIZE = 1
LIST_FILES = [
    'atumalaca.jpg',
    'boa_tarde_neymar.mp4',
    'poema.txt',
    'hold_the_line.mp3'
] # lista de arquivos a serem enviados e tratados pelo servidor


class Client:
    def __init__(self, server_name, server_port, buffer_size, header_size, list_files):
        self.server_name = server_name
        self.server_port = server_port
        self.buffer_size = buffer_size
        self.header_size = header_size
        self.list_files = list_files
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # cria o socket UDP do cliente
        self.sequence_number = 0
        self.ack_number = 1
        self.data_size = buffer_size - header_size
        self.package_number = 0
            

    def create_segment(self, data):
        sequence_number_b = self.sequence_number.to_bytes(1)
        
        return sequence_number_b + data
    

    def send_segment(self, data):
        segment = self.create_segment(data)
        
        self.socket.sendto(segment, (self.server_name, self.server_port))
    
    
    def send_rec_segment(self, data):
        self.send_segment(data)
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
                    self.send_segment(data)
            except:
                self.send_segment(data)


    def send_file(self, file_name: str):
        self.package_number = 0

        self.send_rec_segment(file_name.encode())
        
        ## Rotina que abre o arquivo para leitura em modo binário e envia-o em pacotes para o servidor
        with open('pasta/' + file_name, 'rb') as file:
            ## Laço que envia os pacotes do arquivo para o servidor enquanto houver conteúdo para ler
            while True:
                data = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer
                
                if data:
                    self.send_rec_segment(data) # envia o pacote para o servidor
                else:
                    break
            
        self.send_rec_segment(b'') # envia o caractere null para sinalizar o servidor do fim do arquivo

        print(f"Número de pacotes enviados e reconhecidos: {self.package_number}")
    


    def send_ack(self):
        self.socket.sendto(self.ack_number.to_bytes(1), (self.server_name, self.server_port))
    
    def extract_segment(self):
        msg, _ = self.socket.recvfrom(self.buffer_size) # recebe o nome do arquivo renomeado pelo servidor
        
        return msg[0], msg[1:]


    def extract_rec_segment(self):
        while True:
            seq_server_number, data = self.extract_segment()
            
            if seq_server_number != self.ack_number:

                self.ack_number = seq_server_number
                self.package_number += 1

                self.send_ack()
                                
                return data
            else:
                self.send_ack()
    
       
    def receive_file(self):
        self.ack_number = 1
        self.package_number = 0 # reseta o contador de pacotes recebidos

        file_renamed = self.extract_rec_segment()
        
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
        
        
    def run(self):
        ## Laço para enviar e receber os arquivos
        for fileName in self.list_files:

            ## ENVIO DOS ARQUIVOS ##
            self.send_file(fileName)

            ## RECEBIMENTO DOS ARQUIVOS ##
            self.receive_file()
    
    
    def close(self):
        self.socket.close() # fecha o socket após o envio e recebimento de todos os arquivos
    
            
client1 = Client(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE, LIST_FILES)

client1.run() 
client1.close()
