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

items_name = ["caderno", "carro", "celular", "computador", "geladeira"]

## Represeta cada item com seus atributos relevantes ao leilão
class Item:
    def __init__(self, id, name, highest_bidder, ip_port):
        self.id = id                            # identificador único do item
        self.name = name                        # nome do item
        self.highest_bidder = highest_bidder    # nome do cliente que deu o maior lance
        self.top_val = 0                        # armazena o maior valor de lance dado para o item
        self.counter = 0                        # armazena o número de lances dados até o momento nesse item
        self.destiny = ip_port                  # armazena a porta do cliente o qual deve receber o item

## Representa as flags principais do protocolo rdt3.0 (sequence number e ack)
## para cada máquina (socket) que se conectar com o servidor
class Hoststatus():
    def __init__(self):
        self.sequence_number = 0
        self.ack_number = 1

## Representa o servidor, tratando de todas as funcionalidades do leilão,
## utilizando a lista de itens predefinidos a serem vendidos um por um
class Server:
    def __init__(self, server_name, server_port, buffer_size, header_size):
        self.server_name = server_name
        self.server_port = server_port
        self.buffer_size = buffer_size
        self.header_size = header_size

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # cria o socket UDP do cliente
        self.data_size = buffer_size - header_size                      # tamanho do segmento

        self.hosts = {}             # dicionário que mapeia o par (ip, porta) de cada host com o status da conexão de cada um
        self.client_list = {}       # dicionário que mapeia (ip, porta) com o nome do usuário

        self.items_list = {}        # dicionário que mapeia o id do item com suas informações
        
        self.items_ids = []         # lista dos id's de cada item restante do leilão (que não foram arrematados)
        self.item_idx = 0           # indice do item que está sendo leiloado no momento

        self.item_timeout = 0       # atributo para armazenar o tempo para que o item seja expirado

        self.socket.bind(('', SERVER_PORT)) # vincula o socket à porta definida
        self.create_dir()  # tenta criar a pasta para armazenar os itens do leilão         
        self.create_items_list() # armazena o nome dos items na lista de items e o id dos items na lista de id's

        self.send_buffer = []       # lista que funciona como uma fila para enviar os pacotes de cada vez, em um único lugar

        self.ack_correct = False    # flag que identifica que a mensagem recebida foi o ack da última mensagem enviada
        self.exit = False           # flag para indicar que o programa está sendo desligado

    # Cria a lista de itens, criando uma instancia de Item para cada um,
    # e armazena os id's de cada item na lista de id's
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
    
    # Envia um ACK ao cliente confirmando o recebimento do último pacote 
    def send_ack(self, client):
        self.socket.sendto(self.hosts[client].ack_number.to_bytes(1), client)

    # Recebe um segmento do cliente e separa o cabeçalho dos dados
    def extract_segment(self):
        msg, client = self.socket.recvfrom(self.buffer_size)
        return msg, client

    # Recebe um segmento novo do cliente, fazendo a separação entre ack e comando de um cliente
    def extract_rec_segment(self):
        while True:
            msg, client = self.extract_segment()   

            # Verifica se o par (ip, porta) do host do cliente está no histório de hosts conectados e adiciona se não
            if client not in self.hosts:
                self.hosts[client] = Hoststatus()

            # Se o tamanho do segmento for maior que 1, então é um comando de um cliente
            if len(msg) > 1:
                sequence_number, data = msg[0], msg[1:]

                # Se o sequence_number do cliente determinado for o esperado (diferente do numero de ack enviado por último),
                # atualiza o ack desse cliente e envia esse ack,
                # e dá como retorno da função os dados da mensagem e o endereço do cliente
                if sequence_number != self.hosts[client].ack_number:
                    self.hosts[client].ack_number = (self.hosts[client].ack_number + 1) % 2 # atualiza o ack

                    self.send_ack(client)
                    
                    return data.decode(), client
                
                # Reenvia o ack caso o pacote que chegou tenha o mesmo número de sequência
                # que o último pacote recebido antes dele
                else:
                    self.send_ack(client)
            # Caso contrário, é um ack de um cliente
            else:   
                ack_number = msg[0]
                
                # Se o ack recebido for o correto, ativa a flag para avisar à thread send que o ack correto chegou
                if ack_number == self.hosts[client].sequence_number:
                    self.ack_correct = True

    # Recebe um arquivo completo enviado por um cliente e seu socket
    def receive(self):
        self.socket.settimeout(None) # Trava o temporizador até receber o próximo pacote

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

        # Caso seja string, codifica o dado (bytes)
        if isinstance(data, str):
            data = data.encode()
        
        return sequence_number_b + is_file + data

    # Envia o segmento para o destino especificado
    # Foi implementada um possível não envio do pacote, simulando perda no transporte
    def send_segment(self, data: str, client, is_file):
        # Condicional para perder o pacote na taxa média de erro estabelecida
        if (random() >= PACKET_ERROR_RATE):
            segment = self.create_segment(data, is_file, client)
            self.socket.sendto(segment, client)

    # Envia o segmento, registrando o tempo atual, e aguarda o ack correspondente (Stop-and-Wait).
    def send_rec_segment(self, data: str, client, timeout, is_file):
        send_time = time() # recebe o tempo atual
        self.send_segment(data, client, is_file)

        # Laço que realiza a transmissão e retransmissão conforme rdt3.0
        # Espera a flag sinalizar que o ack foi recebido da thread receiver
        # Caso a flag não sinalize, considera que houve um timeout
        while True:
            # Se a flag foi ativa, significa que o ack esperado foi recebido, atualizando o ack e desativando a flag
            if self.ack_correct:
                self.hosts[client].sequence_number = (self.hosts[client].sequence_number+1) % 2
                self.ack_correct = False
                break
            # Enquanto a flag não é ativada, simula o timer e reenvio o segmento caso acorra um timeout
            elif time() - send_time >= timeout:
                send_time = time()
                self.send_segment(data, client, is_file)
                
    # Envia os segmentos, podendo o conteúdo deles sendo arquivos ou mensagens
    def send(self, data, client_ip_port, is_file):
        # Se for arquivo
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
            
            os.remove('pasta/' + item + '.txt')
        # Se for mensagem
        else:
            for i in range(0, len(data), self.data_size):
                package = data[i:i+self.data_size]
                self.send_rec_segment(package, client_ip_port, .1, is_file) # envia o pacote para o cliente

    # Função utilizada pela thread sender
    # que trata do envio de atualizações do leilão para o servidor
    def sender(self):
        # Enquanto o servidor estiver rodando
        while True:
            if self.exit and not self.send_buffer:
                return
            
            # Se houver pacotes para mandar
            if self.send_buffer:
                data, client_ip_port, is_file = self.send_buffer.pop(0)
                self.send(data, client_ip_port, is_file)
            else:
                continue

    # Thread de recebimento
    def receiver(self):
        # Enquanto o servidor estiver rodando
        while True:
            if self.exit:
                return
            
            # Recebe o comando vindo de algum cliente
            try:
                 command, client_ip_port = self.receive()
            except socket.timeout:
                continue
            
            # Analise qual o comando
            match command.split()[0]:
                
                # Retorna o guia de ajuda para o cliente
                case "help":
                    self.send_buffer.append((COMMAND_LIST, client_ip_port, False))

                # Realiza o login do cliente
                case "login":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    client_name = command.split()[1]

                    # Se o cliente que solicitou o login não está logado
                    if client_name not in self.client_list.values():
                        self.client_list[client_ip_port] = client_name
                        
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m conectado!")
                        self.send_buffer.append(('voce esta online', client_ip_port, False))
    
                    # Se o cliente já está logado
                    else:
                        print(f"Usuário \x1B[3m{client_name}\x1B[0m já existente!")
                        self.send_buffer.append(('usuario existente', client_ip_port, False))
                        
                # Analisa o lance do cliente
                case "bid":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    if self.items_list:
                        _, item_id, value = command.split()
                        '''if (item_id is None or value is None):
                            print("Bid incompleto! (Formato inválido)")
                            continue'''

                        item_id = int(item_id)
                        value = float(value)
                        
                        # Caso o item que se quer dar o lance está tá lista de items
                        if item_id == self.items_ids[self.item_idx]:
                            id = self.items_ids[self.item_idx]
                            item = self.items_list[id]
                            value_now = item.top_val

                            # Analisa se o lance dado é o maior, se for, salva os dados do cliente que deu o maior valor naquele item
                            # e salva o valor do maior lance, enviando para todos os clientes que tal cliente deu um lance válido
                            print(f"{self.client_list[client_ip_port]} fez o lance no item {command.split()[1]} com valor R${command.split()[2]}")

                            if (value > value_now):
                                item.counter += 1
                                item.top_val = value
                                item.highest_bidder = self.client_list[client_ip_port]
                                item.destiny = client_ip_port
                                self.send_buffer.append(("Lance dado com sucesso!", client_ip_port, False))

                                # Envia a mensagem de que o cliente tal deu o melhor lance para tal item, no momento atual
                                for client in self.client_list:
                                    if client != client_ip_port:
                                        bid = 'Lance de R$' + str(value) +  ' dado para ' + item.name + ' por ' + str(item.highest_bidder) + '\n'
                                        self.send_buffer.append((bid, client, False))
                            else:
                                self.send_buffer.append(("Lance invalido, arranje mais dinheiro!", client_ip_port, False))
                        else:
                            print(f"{self.client_list[client_ip_port]} fez o lance em um item indisponível.")
                            self.send_buffer.append(("O item especificado não está em leilão!", client_ip_port, False))
                    else:
                        self.send_buffer.append(("Nenhum item está em leilão!", client_ip_port, False))
                
                # Retorna para o cliente a lista de items que estão sendo leiloados
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

                # Retorna para o cliente o status do item que está sendo leiloado
                case "status":
                    if self.items_list:
                        ranking = f"{'Id':<8}{'Item':<15}{'Maior lance':<15}\n"
                        ranking += "-" * 38 + "\n"
                        
                        id = self.items_ids[self.item_idx]
                        item = self.items_list[id]
                        bidder = item.highest_bidder if item.highest_bidder else "-"
                        ranking += f"{item.id:<8}{item.name:<15}{bidder:<15}\n"

                        self.send_buffer.append((ranking, client_ip_port, False))
                    else:
                        self.send_buffer.append(("Nenhum item está em leilão!", client_ip_port, False))

                # Realiza o logout do cliente no servidor
                case "logout":
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    print(f"Desfazendo a conexão de \x1B[3m{self.client_list[client_ip_port]}\x1B[0m...")
                    self.send_buffer.append(("voce esta offline", client_ip_port, False))
                    self.client_list.pop(client_ip_port)
                    
                # Caso default
                case _:
                    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                    print("Usuário enviou comando desconhecido")
                    
    
    # Implementa a lógica do controle dos itens e relata ao cliente o resultado do leilão
    def advertisement(self):
        while True:
            # Se não há mais nenhum item para ser leiloado, ativando a flag exit, fazendo que as outras threads finalizem
            if self.items_list:
                # Se há items:
                idx = self.items_ids[self.item_idx]
                item = self.items_list[idx]

                # Caso a condição de termino do leilão esteja satisfeita
                if (item.counter == 5) or (time() >= self.item_timeout and item.destiny is not None): # OU TIMER
                    self.send_buffer.append((f'{item.highest_bidder}/{item.name}', item.destiny, True))
                    
                    # Envia para todos os clientes que o item tal foi recebido pelo cliente tal, ao final do leilão
                    for client in self.client_list:
                        if client != item.destiny:
                            bid = f"Item {item.name}.txt adquirido por {item.highest_bidder} com sucesso!\n"
                            self.send_buffer.append((bid, client, False))

                    # Remove o item leiloado da lista
                    self.items_list.pop(self.items_ids[self.item_idx])
                    self.items_ids.pop(self.item_idx)

                    # Próximo item
                    self.item_timeout = time() + 60
                    
                    if not self.items_ids:
                        continue
                    
                    self.item_idx = self.item_idx % len(self.items_ids)
                
                # O tempo seja batido e nenhum lance tiver sido dado, leiloa o próximo item da lista, ou ele mesmo se for o único
                elif time() >= self.item_timeout and item.destiny is None:
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
