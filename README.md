# Pipeline de Processamento de Dados com PySpark e Streamlit

Este projeto implementa um pipeline de processamento de dados em "tempo real" (streaming) para análise de transações financeiras. Ele simula o fluxo de dados através de sockets, processa as informações utilizando **Apache Spark (PySpark)** e exibe métricas dinâmicas em um dashboard interativo construído com **Streamlit**.

## 🏫 Contexto Acadêmico

Este código foi desenvolvido como parte das atividades avaliativas da disciplina de **Computação Paralela e Distribuída (CPD)** da **Universidade do Estado do Rio Grande do Norte (UERN)**, semestre 2025.2.

O objetivo principal é demonstrar as capacidades do Spark em um cenário de pipeline de dados simplificado, abordando conceitos de streaming, agregação e visualização de dados.

* **Professor:** Alysson Oliveira
* **Alunos:**
    * Eduardo Marinho
    * Paulo Sérgio
    * Vinicius Eduardo
    * Marcos Eduardo
    * Luiz Henrique

---

## ⚙️ Arquitetura do Projeto

O fluxo de dados funciona da seguinte maneira:

1.  **Fonte de Dados (Simulação):** Um servidor Python lê um arquivo CSV (`transações_2000.csv`) e envia cada linha sequencialmente via **Socket TCP** (localhost:9999).
2.  **Processamento (ETL):** O PySpark (Structured Streaming) conecta-se ao socket, recebe os dados brutos, realiza a limpeza, formatação e agregações (somas, contagens, máximos/mínimos).
3.  **Persistência Intermediária:** O Spark escreve os resultados processados periodicamente em arquivos CSV locais (`dash_*.csv`).
4.  **Visualização:** O Streamlit lê esses arquivos CSV em loop e atualiza os gráficos e KPIs na tela do usuário.

```mermaid
graph LR
    A[transações_2000.csv] -->|Lê| B(servidor_transacoes.py)
    B -->|Socket :9999| C(processador_spark.py)
    C -->|Processa & Salva| D[Arquivos CSV Temporários]
    D -->|Lê| E(dashboard.py)
    E -->|Exibe| F[Browser User]
```

-----

## 🛠️ Pré-requisitos

Para executar este projeto, você precisará ter instalado em sua máquina:

1.  **Python 3.8+**
2.  **Java 8 ou 11** (Obrigatório para rodar o Apache Spark).
      * *Nota: Certifique-se de que a variável de ambiente `JAVA_HOME` está configurada.*
3.  **Bibliotecas Python:**
    Instale as dependências executando:


```bash
pip install -r requirements.txt
```

-----

## 🚀 Como Executar

Como o sistema simula um ambiente distribuído, você precisará de **3 terminais** abertos simultaneamente. Siga a ordem abaixo:

### Passo 1: Iniciar o Servidor de Transações (Produtor)

Este script vai ler o CSV e aguardar uma conexão na porta 9999.

**No Terminal 1:**

```bash
python servidor_transacoes.py
```

*Aguarde a mensagem: "Aguardando conexão do Spark na porta 9999..."*

### Passo 2: Iniciar o Processador Spark (Consumidor)

Este script conecta no servidor, processa os dados e gera os arquivos de saída.

**No Terminal 2:**

```bash
python processador_spark.py
```

*Assim que iniciar, você verá no Terminal 1 que a conexão foi estabelecida e os dados começarão a ser enviados.*

### Passo 3: Iniciar o Dashboard (Frontend)

Este script sobe a interface visual.

**No Terminal 3:**

```bash
streamlit run dashboard.py
```

*O navegador abrirá automaticamente no endereço (geralmente) `http://localhost:8501`.*

-----

## 📂 Estrutura de Arquivos

  * `transações_2000.csv`: Dataset contendo 2000 registros de transações simuladas.
  * `servidor_transacoes.py`: Script socket que simula o envio de dados em streaming.
  * `processador_spark.py`: Script principal do PySpark que faz o ETL e cálculos estatísticos.
  * `dashboard.py`: Aplicação web para visualização dos dados.
  * `dash_*.csv`: Arquivos gerados automaticamente pelo Spark contendo os dados processados (KPIs, Análise Mensal, Instituição, etc).

-----

## ⚠️ Notas Importantes

  * **Erros de Conexão:** Se você encerrar o `servidor_transacoes.py`, o `processador_spark.py` irá parar. Para reiniciar, comece sempre pelo Passo 1.
  * **Porta em Uso:** Se der erro de "Address already in use", aguarde um minuto para o sistema operacional liberar a porta 9999 ou altere a porta nos arquivos `servidor_transacoes.py` e `processador_spark.py`.