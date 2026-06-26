# Módulo cliente.py: Responsável por enviar os arquivos para o servidor e
#                    receber os arquivos renomeados do servidor, salvando-os na pasta

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o servidor
import os #importa a biblioteca do sistema para criação do diretório para salvamento de arquivos no servidor
from random import random # importa a função random para geração de perda de pacotes aleatória
from time import time # importa a função time para temporização de retransmição do pacote (rdt3.0)
import threading # importa a biblioteca threading para criação e gerenciamento de threads

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

        self.client_name = "" # atributo que registra o nome do cliente que está rodando no momento
        self.online = False # flag para indicar se o cliente efetivamente está conectado no momento
        self.login_logout_request = False # flag para indicar se o cliente está tentando fazer o login ou logout no momento
        self.exit = False # flag para indicar que o programa está sendo desligado

        self.ack_correct = False # flag que identifica que a mensagem recebida foi o ack da última mensagem enviada

    # Cria o diretório do cliente conectado no momento para armazenar seus itens arrematados
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

    # Monta o segmento e envia para o servidor
    # Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data)
            self.socket.sendto(segment, (self.server_name, self.server_port))
    
    # Envia o segmento, registrando o tempo atual, e aguarda o ack correspondente (Stop-and-Wait).
    def send_rec_segment(self, data, timeout):
        send_time = time() # recebe o tempo atual
        self.send_segment(data)
        
        # Laço que realiza a transmissão e retransmissão conforme rdt3.0
        # Espera a flag sinalizar que o ack foi recebido da thread receiver
        # Caso a flag não sinalize, considera que houve um timeout
        while True:
            if self.ack_correct:
                self.sequence_number = (self.sequence_number + 1) % 2
                self.ack_correct = False
                break
            elif time() - send_time >= timeout:
                print("Falha no envio! Tentando novamente...")
                send_time = time()
                self.send_segment(data)

    # Realiza o envio da mensagem (dado) desejada
    # Utiliza o protocolo rdt3.0 para transmissão e retransmissão de pacotes
    def send(self, command):
        self.send_rec_segment(command.encode(), .1)

    # Envia um ACK ao servidor confirmando o recebimento do último pacote
    def send_ack(self):
        self.socket.sendto(self.ack_number.to_bytes(1), (self.server_name, self.server_port))
    
    # Recebe um segmento do servidor e separa o cabeçalho dos dados
    def extract_segment(self):
        msg, _ = self.socket.recvfrom(self.buffer_size)
        return msg

    # Recebe um segmento novo do servidor, fazendo a separação entre ack e atualização do leilão
    def extract_rec_segment(self):
        while True:
            msg = self.extract_segment()
            
            # Caso o segmento recebido seja maior que 2, conclui que é uma atualização do leilão
            # Caso contrário, conclui que um ack foi recebido
            if len(msg) > 2:
                # Recebe o número de sequência do arquivo recebido, uma flag de que é um arquivo
                # e a mensagem propriamente (podendo ser o item arrematado ou atualização do leilão)
                seq_server_number, is_file, data = msg[0], msg[1], msg[2:]
                
                # Caso seja o segmento novo, retorna da função para a thread receiver
                # e envia o ack do segmento recebido
                # Caso contrário (segmento recebido anteriormente),
                # manda o mesmo ack para receber o novo segmento
                if seq_server_number != self.ack_number:
                    self.ack_number = (self.ack_number + 1) % 2
                    self.send_ack()

                    return data.decode(), is_file
                else:
                    self.send_ack()
            else:
                ack_number = msg[0]

                # Caso esteja esperando receber um ack (por ter enviado um pacote) e ele seja o esperado,
                # sinaliza com a flag para a thread sender de que o pacote foi enviado com sucesso
                if ack_number == self.sequence_number:
                    self.ack_correct = True
    
    # Recebe um arquivo completo enviado pelo servidor e salva na pasta local caso seja um item arrematado
    def receive(self):
        self.socket.settimeout(1)

        msg, is_file = self.extract_rec_segment()

        if not is_file:
            return msg # não sendo um item, então é uma atualização do servidor

        file_name = msg # file_name no formato 'ganhador/nome_item'
                        
        # Rotina que insere um arquivo (.txt) representado pelo item que foi arrematado
        with open('pasta_' + file_name, 'wb'):
            # Laço que recebe os pacotes referentes ao item arrematado
            # até a mensagem de fim de linha (EOF)
            while True:
                data, _  = self.extract_rec_segment()
                
                if data == 'EOF': # condição que sinaliza o fim do item enviado pelo servidor
                    break

        ganhador, item = file_name.split('/')
        
        return f"Item {item} adquirido por {ganhador} com sucesso!"

    # Função utilizada pela thread sender
    # que trata do envio de comandos (recebidos no input) para os servidor
    # e limita o envio do comando a depender de um cliente estar conectado ou desconectado
    def run_sender(self):
        while True:
            print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
            
            # Caso um cliente não esteja tentando conectar ou desconectar, espera receber um comando
            # Caso contrário, não permite que novos comandos sejam inseridos
            if not self.login_logout_request:
                # Caso o cliente não esteja conectado, espera receber o comando de login
                # Caso contrário, espera receber um comando diferente de login
                if not self.online:
                    print("\033[3mlogin username\033[0m para entrar!")
                    command = input("Insira o comando: ")

                    # Caso receba o comando de login, tenta conectar o usuário
                    # Caso receba o comando de exit, fecha o programa
                    # Caso contrário, nega o comando
                    if command.split()[0] == "login":
                        self.login_logout_request = True
                        
                        # Nega o login caso venha sem nome de usuário
                        if command.split()[1] == '':
                            print("Usuário inválido!")
                            continue
                        
                        # Salva o nome do cliente que está rodando o programa no momento
                        self.client_name = command.split()[1]
                        
                        self.send(command)
                    elif command == "exit":
                        self.exit = True
                        print("Saindo do sistema...")
                        break
                    else:
                        print("Comando inválido!")
                else:
                    command = input("Insira o comando (\033[3mhelp\033[0m para ver comandos): ")

                    # Fecha o programa
                    if command == "exit":
                        self.exit = True
                        print("Saindo do sistema...")
                        break

                    # Nega a tentativa de login, pois o usuário já está conectado
                    if "login" in command:
                        print("Algum usuário já está conectado!")
                        continue

                    # Caso receba o comando de logout, começa a desconectar o usuário
                    if command == "logout":
                        self.login_logout_request = True
                    
                    self.send(command)
            else:
                print("Aguarde a resposta do comando anterior.")

    # Função utilizada pela thread receiver
    # que trata do recebimento de pacotes pelo socket único
    # e ajusta atributo e flags a depender da resposta do servidor,
    # que pode enviar uma atualização do servidor (a ser printada), um item arrematado ou um ack
    def run_receiver(self):
         while True:
            try:
                answer = self.receive()
            except socket.timeout:
                continue

            print(f"\n{answer}\n")
            
            # Caso o cliente não esteja conectando ou desconectando
            # e um comando exit tenha sido acionado, fecha esse programa
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
                case "usuario existente":
                    self.online = False
                    self.login_logout_request = False
    
    # Main para rodar as threads que vão enviar pacotes (sender) e vão receber pacotes (receiver)
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
    
# Criação do objeto cliente e do seu socket
client = Client(SERVER_NAME, SERVER_PORT, BUFFER_SIZE, HEADER_SIZE)

client.run()
client.close()
