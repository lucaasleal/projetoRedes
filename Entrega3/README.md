# projetoRedes — Entrega 3: Sistema AuctionCin (Arquitetura Cliente-Servidor)

## Equipe 6

| Integrante                    |  Login  |
| ----------------------------- | :-----: |
| João Henrique Moraes Guedes   |  `jhmg` |
| Lucas Felipe Leal Andrade     | `lfla2` |
| Pedro Inácio Alves dos Santos |  `pias` |
| Rodrigo Florenço dos Santos   |  `rfs6` |

---

## Visão Geral

Nesta terceira etapa do projeto, foi desenvolvido o **AuctionCin**, um sistema de leilão online multiusuário baseado na arquitetura **cliente-servidor**.

A implementação utiliza como base o protocolo **RDT 3.0**, desenvolvido na segunda entrega da disciplina, garantindo uma comunicação confiável entre clientes e servidor. Sobre essa infraestrutura, foram implementadas todas as funcionalidades obrigatórias especificadas no **Projeto da Disciplina**, incluindo autenticação de usuários, gerenciamento de itens e realização de leilões.

📄 **Especificação do projeto:**
https://drive.google.com/file/d/1Jj33e-FQy6W67vm6Q8Ga-trmgpiID_Jg/view?usp=sharing

---

## Estrutura do Projeto

```text
Entrega3/
├── Cliente/
│   ├── pasta_cliente1/
│   └── cliente.py
├── Servidor/
│   ├── pasta/
│   │   ├── caderno.txt
│   │   ├── carro.txt
│   │   ├── celular.txt
│   │   ├── computador.txt
│   │   └── geladeira.txt
│   └── servidor.py
└── run.bat
```

---

## Esquema Visual

<img width="6886" height="5556" alt="DiagramaV2" src="https://github.com/user-attachments/assets/58785ba5-80a9-49e7-b71c-ed1bd1c3310a" />


## Descrição dos Arquivos

### Cliente

* **cliente.py**

  Implementa todas as funcionalidades executadas pelo cliente, incluindo login, comunicação com o servidor, participação nos leilões e recebimento dos itens adquiridos.

* **pasta_clienteX/**

  Diretório criado automaticamente para cada usuário que realiza login pela primeira vez. Nele são armazenados os arquivos correspondentes aos itens adquiridos durante os leilões.

### Servidor

* **servidor.py**

  Responsável pelo gerenciamento do sistema, controle dos leilões, autenticação dos clientes, processamento dos lances e envio dos itens aos compradores.

* **pasta/**

  Contém os arquivos que representam os itens disponíveis para leilão.

### Script de execução

* **run.bat**

  Script para Windows que automatiza a inicialização do sistema, abrindo um terminal para o servidor e três terminais para clientes, facilitando os testes da aplicação.

---

## Como Executar

No Windows, execute o arquivo **run.bat**:

```bash
.\run.bat
```

O script abrirá automaticamente **quatro terminais**:

* 1 terminal para o servidor;
* 3 terminais para clientes.

### Observações

* Na primeira execução, caso o diretório **Servidor/pasta/** não exista, ele será criado automaticamente para armazenar os itens do leilão.

* Da mesma forma, quando um usuário realiza login pela primeira vez, é criado automaticamente um diretório no formato:

```text
pasta_<username>/
```

Esse diretório é utilizado para armazenar os itens adquiridos pelo respectivo cliente ao longo dos leilões.
