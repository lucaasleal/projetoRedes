# Módulo servidor.py: Responsável por receber os arquivos enviados pelo cliente, salvando-os na pasta,
#                     renomear esses arquivos e retorná-los para o cliente

import socket # importa a biblioteca socket para criar o socket UDP e realizar a comunicação com o cliente
import os #importa a biblioteca do sistema para criação do diretório para salvamento de arquivos no servidor
from random import random # importa a função random para geração de perda de pacotes aleatória
from time import time # importa a função time para temporização de retransmição do pacote (rdt3.0)
import threading # importa a biblioteca threading para criação e gerenciamento de threads

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

items_name = ["caderno"] #, "carro", "celular", "computador", "geladeira"

class Item:
    def __init__(self, id, name, highest_bidder, ip_port):
        self.id = id
        self.name = name
        self.highest_bidder = highest_bidder    # nome do cliente que deu o maior lance
        self.top_val = 0                        # armazena o maior valor de lance dado para o item
        self.counter = 0                        # armazena o número de lances dados até o momento nesse item
        self.destiny = ip_port                  # armazena a porta do cliente o qual deve receber o item

class Hoststatus():
    def __init__(self):
        self.sequence_number = 0
        self.ack_number = 1
        
class Server:
    def __init__(self, server_name, server_port, buffer_size, header_size):
        self.server_name = server_name
        self.server_port = server_port
        self.buffer_size = buffer_size
        self.header_size = header_size

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # cria o socket UDP do cliente
        self.data_size = buffer_size - header_size                      # tamanho do quadro do segmento

        self.hosts = {}             # dicionário que mapeia o par (ip, porta) de cada host com o status da conexão de cada um
        self.client_list = {}       # dicionário que mapeia (ip, porta) com o nome do usuário

        self.items_list = {}        # dicionário que mapeia o id do item com suas informações
        self.items_ids = []         # 

        self.item_idx = 0           # indice do item que está sendo leiloado no momento

        self.socket.bind(('', SERVER_PORT)) # vincula o socket à porta definida
        self.create_dir()                   
        self.create_items_list()

        self.send_buffer = []       # lista que funciona como uma fila para enviar os pacotes de cada vez, em um único lugar

        self.ack_correct = False    # flag que indica que um ack correto chegou, fazendo com que as threads send e receive se comuniquem
        self.exit = False           # flag que indica que o usuário está desconectado

    # função que cria a lista de itens, criando uma instancia Item para cada um, no final setando o periodo que o primeiro item vai ser leiloado
    def create_items_list(self):
        id = 0
        for i in items_name:
            item = Item(id, i, None, None)
            self.items_list[id] = item
            self.items_ids.append(id)
            id += 1
        self.item_timeout = time() + 60

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
        self.socket.sendto(self.hosts[client].ack_number.to_bytes(1), client)


    # Recebe um segmento do cliente e separa o cabeçalho dos dados.
    def extract_segment(self):
        msg, client = self.socket.recvfrom(self.buffer_size)
        return msg, client


    # Gerenciamento do recebimento e o envio da confirmação
    def extract_rec_segment(self):
        while True:
            msg, client = self.extract_segment()   

            # verifica se o par (ip, porta) do host do cliente está no histório de hosts conectados e adiciona se não
            if client not in self.hosts:
                self.hosts[client] = Hoststatus()

            # se a msg tem tamanho maior que 1, ou seja, não for um ack
            if len(msg) > 1:
                sequence_number, data = msg[0], msg[1:]

                # se o sequence_number for o esperado (difente do numero de ack enviado por ultimo)
                # atualiza o ack, o envia e dá como retorno da função os dados da mensagem e o endereço do cliente
                if sequence_number != self.hosts[client].ack_number:
                    self.hosts[client].ack_number = (self.hosts[client].ack_number + 1) % 2 # atualiza o ack

                    self.send_ack(client)
                    
                    return data.decode(), client
                
                # Reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
                # que o ultimo pacote recebido antes dele
                else:
                    # print("Pacote esperado não foi recebido, enviando ack...")
                    self.send_ack(client)
            
            # caso seja um ack
            else:
                ack_number = msg[0]
                # o ack for o correto, ativa a flag para avisar à thread send que o ack correto chegou
                if ack_number == self.hosts[client].sequence_number:
                    self.ack_correct = True


    # Recebe o campo de dados da mensagem e o endereço do cliente e repassa
    def receive(self):
        self.socket.settimeout(None) # Trava o temporizador até receber o próximo pacote

        # primeiro pacote contém o nome do arqui
        command, client_ip_port = self.extract_rec_segment()

        self.socket.settimeout(10)
        
        return command, client_ip_port


    # Cria o segmento a partir do número de sequência (cabeçalho) e os dados
    # Foi implementada uma possível geração de erro no pacote, alternando o bit de sequência
    def create_segment(self, data, is_file, client):
        sequence_number_b = self.hosts[client].sequence_number.to_bytes(1)
        is_file = is_file.to_bytes(1)
        
        # Condicional para corromper o pacote na taxa média de erro estabelecida
        if (random() < PACKET_ERROR_RATE):
            false_number = (self.hosts[client].sequence_number + 1) % 2
            sequence_number_b = false_number.to_bytes(1)

        # garante que o dado não já está encodado, e o encoda
        if isinstance(data, str):
            data = data.encode()
        return sequence_number_b + is_file + data

    # Envia o segmento para o destino especificao
    # Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data: str, client, is_file):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data, is_file, client)
            self.socket.sendto(segment, client)


    # Envia um segmento e aguarda o ACK correspondente (Stop-and-Wait).
    def send_rec_segment(self, data: str, client, timeout, is_file):
        send_time = time() # recebe o tempo atual
        self.send_segment(data, client, is_file)

        # aguarda o ack
        while True:

            # se a flag foi ativa, significa que o ack esperado foi recebido, atualizando o ack e desativando a flag
            if self.ack_correct:
                self.hosts[client].sequence_number = (self.hosts[client].sequence_number+1) % 2
                self.ack_correct = False
                break

            # enquanto a flag não é ativada, simula o timer e reenvio o segmento caso acorra um timeout
            elif time() - send_time >= timeout:
                print(f"Timeout! Retransmitindo o pacote")
                send_time = time()
                self.send_segment(data, client, is_file)
                
    
    # Envia os segmentos, podendo o conteúdo deles sendo arquivos ou mensagens
    def send(self, data, client_ip_port, is_file):

        # se for arquivo
        if is_file:
             print(f"Item {data} começando envio!")
             item = data.split('/')[1]
             with open('pasta/' + item + '.txt', 'rb') as file:
                file_name = data + '.txt'

                self.send_rec_segment(file_name, client_ip_port, .1, is_file)

                package = file.read(self.data_size) # lê o conteúdo do arquivo em pacotes do tamanho do buffer

                ## Laço que envia os pacotes do arquivo para o cliente enquanto houver conteúdo para ler
                while package:
                    self.send_rec_segment(package, client_ip_port, .1, is_file) # envia o pacote para o cliente
                    package = file.read(self.data_size) # lê o próximo pacote do arquivo até o final do arquivo

                self.send_rec_segment('EOF', client_ip_port, .1, is_file) # envia o caractere null para sinalizar o cliente do fim do arquivo

                print(f"Item {file_name} enviado com sucesso!")

        # se for mensagem
        else:
            for i in range(0, len(data), self.data_size):
                package = data[i:i+self.data_size]
                self.send_rec_segment(package, client_ip_port, .1, is_file) # envia o pacote para o cliente

    # Thread de envio
    def sender(self):

        # enquanto o servidor estiver rodando
        while True:

            if self.exit and not self.send_buffer:
                return
            
            # se houver pacotes para mandar
            if self.send_buffer:
                # print(self.send_buffer[0])
                data, client_ip_port, is_file = self.send_buffer.pop(0)
                self.send(data, client_ip_port, is_file)
            else:
                continue


    # Thread de recebimento
    def receiver(self):

        # enquanto o servidor estiver rodando
        while True:
            if self.exit:
                return
            
            # recebe o comando vindo de algum cliente
            try:
                 command, client_ip_port = self.receive()
            except socket.timeout:
                continue
            
            # analise qual o comando
            match command.split()[0]:
                
                # retorna o guia de ajuda para o cliente
                case "help":
                    self.send_buffer.append((COMMAND_LIST, client_ip_port, False))

                # realiza o login do cliente
                case "login":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    client_name = command.split()[1]

                    # se o cliente que solicitou o login não está logado
                    if client_name not in self.client_list.values():
                        self.client_list[client_ip_port] = client_name
                        
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m conectado!")
                        self.send_buffer.append(('voce esta online', client_ip_port, False))
    
                    # se o cliente já está logado
                    else:
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m já existente!")
                        self.send_buffer.append(('usuario existente', client_ip_port, False))
                        
                # analisa o lance do cliente
                case "bid":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    if self.items_list:
                        _, item_id, value = command.split()
                        '''if (item_id is None or value is None):
                            print("Bid incompleto! (Formato inválido)")
                            continue'''

                        item_id = int(item_id)
                        value = float(value)
                        
                        # caso o item que se quer dar o lance está tá lista de items
                        if item_id == self.items_ids[self.item_idx]:
                            id = self.items_ids[self.item_idx]
                            item = self.items_list[id]
                            value_now = item.top_val

                            # analisa se o lance dado é o maior, se for, salva os dados do cliente que deu o maior valor naquele item
                            # e salvo o valor do maior lance, enviando para todos os clientes que tal cliente deu um lance válido
                            print(f"{self.client_list[client_ip_port]} fez o lance no item {command.split()[1]} com valor R${command.split()[2]}")

                            if (value > value_now):
                                item.counter += 1
                                item.top_val = value
                                item.highest_bidder = self.client_list[client_ip_port]
                                item.destiny = client_ip_port
                                self.send_buffer.append(("Lance dado com sucesso!", client_ip_port, False))

                                # envia a mensagem de que o cliente tal deu o melhor lance para tal item, no momento atual
                                for client in self.client_list:
                                    if client != client_ip_port:
                                        bid = 'Lance de R$' + str(value) +  ' dado para o ' + item.name + ' por ' + str(item.highest_bidder) + '\n'
                                        self.send_buffer.append((bid, client, False))
                            else:
                                self.send_buffer.append(("Lance invalido, arranje mais dinheiro!", client_ip_port, False))
                        else:
                            print(f"{self.client_list[client_ip_port]} fez o lance em um item indisponível.")
                            self.send_buffer.append(("O item especificado não está em leilão!", client_ip_port, False))
                    else:
                        self.send_buffer.append(("Nenhum item está em leilão!", client_ip_port, False))
                
                # retorna para o cliente a lista de items que estão sendo leiloados
                case "list":
                    if self.items_list:
                        list = f"{'Id':<8}{'Item':<15}{'Valor (R$)':>10}\n"
                        list += "-" * 33 + "\n"

                        id = self.items_ids[self.item_idx]
                        item = self.items_list[id]
                        value = f"R${item.top_val:.2f}" if item.top_val else "-"
                        list += f"{item.id:<8}{item.name:<15}{value:>10}\n"

                        self.send_buffer.append((list, client_ip_port, False))
                    else:
                        self.send_buffer.append(("Nenhum item está em leilão!", client_ip_port, False))

                # retorna para o cliente o status do item que está sendo leiloado
                case "status":
                    if self.items_list:
                        ranking = f"{'Id':<8}{'Item':<15}{'Maior lance':<15}{'Valor (R$)':>10}\n"
                        ranking += "-" * 48 + "\n"
                        
                        id = self.items_ids[self.item_idx]
                        item = self.items_list[id]
                        bidder = item.highest_bidder if item.highest_bidder else "-"
                        value  = f"R${item.top_val:.2f}" if item.top_val else "-"
                        ranking += f"{item.id:<8}{item.name:<15}{bidder:<15}{value:>10}\n"

                        self.send_buffer.append((ranking, client_ip_port, False))
                    else:
                        self.send_buffer.append(("Nenhum item está em leilão!", client_ip_port, False))

                # realiza o logout do cliente no servidor
                case "logout":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    print(f"Desfazendo a conexão de \x1B[3m{self.client_list[client_ip_port]}\x1B[0m...")
                    self.send_buffer.append(("voce esta offline", client_ip_port, False))
                    self.client_list.pop(client_ip_port)
                    
                # caso default
                case _:
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    print("Comando desconhecido (digite \x1B[3mhelp\x1B[0m para ver lista de comandos)")
                    
    
    # Implementa a lógica do controle dos itens e relata ao cliente o resultado do leilão.
    def advertisement(self):

        while True:
            
            # se não há mais nenhum item para ser leiloado, ativando a flag exit, fazendo que as outras threads finalizem
            if self.items_list:
                # se há items:

                idx = self.items_ids[self.item_idx]
                item = self.items_list[idx]

                # caso a condição de termino do leilão esteja satisfeita
                if ((item.counter == 5) or (time() >= self.item_timeout and item.destiny is not None)): #OU TIMER
                    self.send_buffer.append((f'{item.highest_bidder}/{item.name}', item.destiny, True))
                    
                    # envia para todos os clientes que o item tal foi recebido pelo cliente tal, ao final do leilão
                    for client in self.client_list:
                        if client != item.destiny:
                            bid = f"Item {item.name}.txt adquirido por {item.highest_bidder} com sucesso!\n"
                            self.send_buffer.append((bid, client, False))

                    # remove o item leiloado da lista
                    self.items_list.pop(self.items_ids[self.item_idx])
                    self.items_ids.pop(self.item_idx)

                    # p]poroximo item
                    self.item_timeout = time() + 60
                    if not self.items_ids:
                        continue
                    self.item_idx = self.item_idx % len(self.items_ids)
                
                # o tempo seja batido e nenhum lance tiver sido dado, leiloa o próximo item da lista, ou ele mesmo se for o únicou
                elif(time() >= self.item_timeout and item.destiny is None):
                    print(f"Item {item.name} não foi adquirido!\n")
                    for client in self.client_list:
                        bid = f"Item {item.name} não foi adquirido!\n"
                        self.send_buffer.append((bid, client, False))
                    self.item_timeout = time() + 60
                    if not self.items_ids:
                        continue
                    self.item_idx = (self.item_idx + 1) % len(self.items_ids)


    # Executa o loop principal do servidor, processando arquivos indefinidamente
    def main(self):
        # Inicialização das threads
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