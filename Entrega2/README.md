# projetoRedes: Entrega 1 (Envio e recebimento de arquivos)
## Equipe 6

João Henrique Moraes Guedes -	jhmg

Lucas Felipe Leal Andrade	- lfla2

Pedro Inácio Alves dos Santos -	pias

Rodrigo Florenço dos Santos	- rfs6
##


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

## Funcionamento do Sistema
### Cliente

O cliente realiza as seguintes etapas:

Define uma lista de arquivos a serem enviados:

LIST_FILES = [
    'atumalaca.jpg',
    'boa_tarde_neymar.mp4',
    'poema.txt',
    'hold_the_line.mp3'
]

Para cada arquivo:
- Envia o nome do arquivo ao servidor
- Divide o arquivo em pacotes de 1024 bytes
- Envia os pacotes sequencialmente
- Envia um pacote vazio (b'') para indicar fim do arquivo

Aguarda resposta do servidor:
- Recebe o novo nome do arquivo
- Recebe os pacotes do arquivo renomeado
- Reconstrói o arquivo localmente

### Servidor

O servidor executa continuamente e realiza:
- Recebe o nome do arquivo
- Recebe os pacotes até encontrar b'' (fim do arquivo)
- Salva o arquivo no diretório 'pasta/'

Renomeia o arquivo adicionando o prefixo:
> leilao_<nome_original>
- Envia de volta ao cliente:
- Nome do arquivo renomeado
- Conteúdo em pacotes
- Pacote vazio indicando fim
