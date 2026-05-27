# projetoRedes: Entrega 2 (Implementando uma transferência confiável com RDT 3.0)
## Equipe 6

João Henrique Moraes Guedes -	jhmg

Lucas Felipe Leal Andrade	- lfla2

Pedro Inácio Alves dos Santos -	pias

Rodrigo Florenço dos Santos	- rfs6
##


## Visão Geral
 
O UDP por natureza não é confiável, pois pacotes podem ser perdidos, duplicados ou chegar com a ordem incorreta. Essa entrega foca em implementar uma camada de confiabilidade sobre o UDP, simulando o comportamento do protocolo RDT 3.0 (Reliable Data Transfer), que inclui:
 
- Detecção e retransmissão de pacotes perdidos via **timeout**
- Descarte de pacotes duplicados via **número de sequência alternado (0/1)**
- Confirmação de recebimento via **ACK**
- Simulação de **perda de pacotes** e de **número de sequência corrompido**
O cliente envia arquivos para o servidor, que os renomeia (prefixo `leilao_`) e os devolve ao cliente.

## Como executar
Abra dois terminais, primeiro execute o servidor:
```bash
python3 servidor.py
```
Com o servidor aguardando e pronto para receber pacotes:
```bash
python3 cliente.py
```

OBS: Como o servidor não possui o diretório 'pasta', na primeira execução ele vai criar 'pasta', onde serão os arquivos recebidos. Em outras execuções, o diretório já existirá.


O cliente possui quatro arquivos e esses quatro serão enviados automaticamente pelo cliente, posteriormente recebidos e retornados pelo servidor, com os seus nomes alterados.

LIST_FILES = [
    'atumalaca.jpg',
    'boa_tarde_neymar.mp4',
    'poema.txt',
    'hold_the_line.mp3'
]


## Protocolo Stop-and-Wait (RDT 3.0)
- O remetente envia **um pacote por vez** e aguarda o ACK antes de enviar o próximo
- Se o ACK não chegar dentro do **timeout**, o pacote é retransmitido
- O número de sequência **alterna entre 0 e 1** (bit alternante), permitindo detectar duplicatas

## Formato dos Segmentos
 
Cada segmento tem **1 byte de cabeçalho** seguido pelos dados:
 
```
+----------------+-----------------------------+
|  seq/ack (1B)  |        dados (até 1023B)    |
+----------------+-----------------------------+
```
 
- `BUFFER_SIZE = 1024` bytes no total
- `HEADER_SIZE = 1` byte (número de sequência ou ACK)
- `DATA_SIZE = 1023` bytes de dados por pacote
Os ACKs são segmentos de 1 byte apenas, contendo o número de sequência confirmado (Precisa-se apenas de 0 e 1).


## Fluxo de Transferência
 
### Envio (cliente → servidor)
1. Cliente envia o **nome do arquivo** como primeiro pacote
2. Cliente envia o **conteúdo** do arquivo em pacotes de 1023 bytes
3. Cliente envia um **pacote vazio `b''`** sinalizando fim do arquivo
4. Servidor recebe, salva e renomeia o arquivo com prefixo `leilao_`


### Retorno (servidor → cliente)
1. Servidor envia o **novo nome** do arquivo renomeado
2. Servidor envia o **conteúdo** do arquivo renomeado em pacotes
3. Servidor envia um **pacote vazio `b''`** sinalizando fim
4. Cliente recebe e salva o arquivo renomeado na pasta


## Simulação de Perdas e Erros
O procedimento de perda de pacote foi feito utilizando da biblioteca *random* nativa do Python. De forma simplificada, quando o transmissor manda um arquivo, há uma chance definida pelo parâmetro *PACKET_LOSS* (valor padrão de 0.5%) da função *send_segment()* não realizar o envio do segmento criado para o receptor. Dessa forma, emulando a perda de pacotes que podem acontecer durante a transmissão TCP.

Para a emulação de entrega de pacotes com valores do cabeçalho incorretas (que, nesse caso, somente o número de sequência que pode ser entregue errado), foi-se realizado um procedimento parecido. Durante a criação do segmento (realizada na função *create_segment()*), há uma chance definida pelo parâmetro *PACKET_LOSS* de o valor do número de sequência ser trocado por um outro valor incorreto. Através dessa estratégia, o código consegue simular a situação em que o pacote entregue possui erros no ACK/SEQ.


## Tratamento de Timeout
Para a implementação do timeout, ou seja, quando o transmissor retransmite o pacote devido a falta de confirmação (ACK) do receptor, foi utilizada a biblioteca *time* nativa do Python para calcular o tempo percorrido durante as esperas do transmissor. Como a função *socket.recvfrom()* é bloqueante (o programa "trava" ao executar, uma vez que ele está esperando o pacote ACK do receptor), foi definido o tempo de espera da função utilizando *socket.settimeout()*. No código, caso o tempo de espera seja maior que 0.1 segundos, o transmissor irá entender que houve a perda do pacote ACK e realizará o reenvio do pacote.

Para o caso em que o pacote ACK recebido pelo transmissor está com erro do número de sequência, o modelo rdt3.0 dita que o transmissor deve desconsiderar esse pacote e esperar o próximo pacote, isto é, o temporizador não é . Implementar essa mecânica exigiu que se fosse criada uma variável *timeout* para armazenar o tempo percorrido desde a entrega do pacote de dados. Ao transmissor receber o pacote com erro, ele reconhece e atualiza o tempo de espera da função *socket.recvfrom()* através de *socket.settimeout()* com um novo valor (*initial_timeout - elapsed_time*) e continua a aguardar.


## Sincronização dos Números de Sequência
 
**Ponto crítico:** a cada novo arquivo, ambos os lados resetam seus estados para garantir sincronização:
 
| Evento | `sequence_number` | `ack_number` |
|---|---|---|
| Cliente inicia `send_file` | reset para `0` | — |
| Cliente inicia `receive_file` | — | reset para `1` |
| Servidor inicia `receive_file` | reset para `0` | reset para `1` |
| Servidor inicia `send_file` | reset para `0` | — |
 
Sem esses resets, o segundo arquivo pode começar com números trocados e o primeiro pacote (o nome do arquivo) é descartado como duplicata. causando um problema.
 

## Parâmetros e valores
| Parâmetro | Valor Padrão | Descrição |
|---|---|---|
| 'SERVER_PORT'  | 12000 | Porta utilizada pelo servidor |
| 'BUFFER_SIZE'  | 1024  | Quantidade de bytes de um pacote |
| 'HEADER_SIZE'  | 1  |  Quantidade de bytes do cabeçalho (ack/seq)
| 'PACKET_LOSS' | 0.005 | Taxa de perda para geração de perdas simuladas |
| Timeout Cliente | 0.1s | Timeout sobre o pacote enviado |
| Timeout Servidor | 10s | Timeout por pacote durante recebimento |
