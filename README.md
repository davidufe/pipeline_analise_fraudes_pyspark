# Pipeline de Análise de Fraudes com PySpark

## Objetivo

Este projeto implementa um pipeline de dados utilizando PySpark para processar informações de pedidos e pagamentos, aplicando regras de negócio relacionadas à análise de fraude.

O objetivo é gerar um relatório contendo pedidos cujo pagamento foi recusado e que, ao mesmo tempo, foram classificados como legítimos pelo sistema antifraude.

O projeto foi desenvolvido como uma demonstração de boas práticas de Engenharia de Dados, incluindo:

* Leitura de dados estruturados em múltiplos formatos
* Definição explícita de schemas
* Transformações com PySpark
* Separação de responsabilidades por camadas
* Testes unitários automatizados
* Empacotamento da aplicação utilizando pyproject.toml

---

## Ambiente de Desenvolvimento

O projeto foi desenvolvido e validado utilizando o AWS Cloud9, executando PySpark em uma instância Linux hospedada na AWS.

Esse ambiente foi utilizado para:

* Desenvolvimento do pipeline
* Execução dos jobs Spark
* Execução dos testes automatizados
* Empacotamento da aplicação

---

## Regras de Negócio

A alta gestão da empresa deseja um relatório contendo pedidos de venda cujo:

* Pagamento foi recusado (`status = false`)
* Avaliação antifraude classificou o pedido como legítimo (`fraude = false`)

Além disso:

* O relatório deve considerar apenas pedidos do ano de 2025.
* O resultado deve estar ordenado por:

  1. UF
  2. Forma de pagamento
  3. Data de criação

---

## Relatório Gerado

O relatório final contém os seguintes atributos:

| Campo           | Descrição                          |
| --------------- | ---------------------------------- |
| id_pedido       | Identificador único do pedido      |
| uf              | Estado onde o pedido foi realizado |
| forma_pagamento | Forma de pagamento utilizada       |
| valor_total     | Quantidade × Valor Unitário        |
| data_criacao    | Data de criação do pedido          |

---

## Arquitetura da Solução

O projeto foi estruturado seguindo princípios de separação de responsabilidades.

### Camada de Configuração

Responsável pela criação e gerenciamento da SparkSession.

Arquivo:

```text
src/config/spark_session_manager.py
```

Responsabilidades:

* Criar SparkSession
* Configurar ambiente Spark
* Disponibilizar a sessão para toda a aplicação

---

### Camada de Entrada e Saída

Responsável pela leitura e escrita dos dados.

Arquivo:

```text
src/io_utils/data_handler.py
```

Responsabilidades:

* Leitura de pagamentos
* Leitura de pedidos
* Escrita do resultado final em Parquet

---

### Camada de Transformação

Responsável pelas regras de negócio.

Arquivo:

```text
src/processing/transformations.py
```

Responsabilidades:

* Padronização dos nomes das colunas
* Cálculo do valor total do pedido
* Join entre pedidos e pagamentos
* Aplicação dos filtros de negócio
* Seleção das colunas finais
* Ordenação do relatório

---

### Camada de Orquestração

Responsável por executar o pipeline completo.

Arquivo:

```text
src/main.py
```

---

## Estrutura do Projeto

```text
trabalho/
│
├── src/
│   ├── config/
│   │   └── spark_session_manager.py
│   │
│   ├── io_utils/
│   │   └── data_handler.py
│   │
│   ├── processing/
│   │   └── transformations.py
│   │
│   └── main.py
│
├── tests/
│   ├── conftest.py
│   │
│   └── unit/
│       ├── test_data_handler.py
│       └── test_transformations.py
│
├── pyproject.toml
│
└── README.md
```

---

## Tecnologias Utilizadas

* Python 3.10+
* PySpark 4.x
* Pytest
* Ruff
* Black
* Setuptools

---

## Formatos de Entrada

### Dataset de Pagamentos

Formato JSON compactado em GZIP.

Exemplo:

```json
{
  "id_pedido": "0fc8a4c7-d9e3-4b58-8ebd-41a833d4c9a7",
  "forma_pagamento": "CARTAO_CREDITO",
  "valor_pagamento": 1100.0,
  "status": true,
  "data_processamento": "2024-01-21T17:07:43.329582",
  "avaliacao_fraude": {
    "fraude": false,
    "score": 0.27
  }
}
```

Descrição dos campos:

| Campo                   | Descrição                                     |
| ----------------------- | --------------------------------------------- |
| id_pedido               | Identificador único do pedido                 |
| forma_pagamento         | Método utilizado para pagamento               |
| valor_pagamento         | Valor processado                              |
| status                  | Indica aprovação (`true`) ou recusa (`false`) |
| data_processamento      | Data e hora do processamento                  |
| avaliacao_fraude.fraude | Resultado da avaliação antifraude             |
| avaliacao_fraude.score  | Score calculado pelo motor antifraude         |

---

### Dataset de Pedidos

Formato CSV compactado em GZIP.

Exemplo:

```csv
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
123;TV;1500.0;2;2025-01-01T10:00:00;RN;1
```

Descrição dos campos:

| Campo          | Descrição                 |
| -------------- | ------------------------- |
| ID_PEDIDO      | Identificador do pedido   |
| PRODUTO        | Produto adquirido         |
| VALOR_UNITARIO | Valor unitário do produto |
| QUANTIDADE     | Quantidade adquirida      |
| DATA_CRIACAO   | Data de criação do pedido |
| UF             | Estado do cliente         |
| ID_CLIENTE     | Identificador do cliente  |

---

## Instalação

Clone o repositório:

```bash
git clone https://github.com/davidufe/pipeline_analise_fraudes_pyspark.git
```

Crie e ative um ambiente virtual:

```bash
python -m venv .venv

source .venv/bin/activate
```

Instale o projeto:

```bash
pip install .
```

---

## Build do Projeto

Gerar o pacote da aplicação:

```bash
python -m build
```

Os artefatos serão gerados no diretório:

```text
dist/
```

Instalação do pacote gerado:

```bash
pip install dist/*.whl
```

---

## Execução

Executar o pipeline:

```bash
python src/main.py
```

ou

```bash
run-data-pipeline
```

---

## Testes

Executar todos os testes:

```bash
pytest
```

Executar com saída detalhada:

```bash
pytest -v
```

---

## Testes Implementados

### Transformations

#### mudar_nome_colunas_pedidos

Valida:

* Renomeação correta das colunas
* Padronização para snake_case

#### add_valor_total_pedidos

Valida:

* Cálculo correto do valor total

#### join_and_filter

Valida:

* Estrutura final do relatório
* Colunas retornadas conforme regra de negócio

---

### DataHandler

#### load_pagamentos

Valida:

* Leitura de arquivos JSON compactados em GZIP
* Retorno de DataFrame Spark

#### load_pedidos

Valida:

* Leitura de arquivos CSV compactados em GZIP
* Retorno de DataFrame Spark

---

## Saída

O resultado final é persistido em formato Parquet.

Exemplo:

```text
output/
└── relatorio_fraudes/
```

Estrutura final:

| Campo           | Descrição               |
| --------------- | ----------------------- |
| id_pedido       | Identificador do pedido |
| uf              | Estado                  |
| forma_pagamento | Forma de pagamento      |
| valor_total     | Valor total do pedido   |
| data_criacao    | Data do pedido          |

---

## Autor

Davi Dutra

Projeto desenvolvido como exercício de Engenharia de Dados utilizando PySpark, AWS Cloud9 e boas práticas de desenvolvimento de software.
