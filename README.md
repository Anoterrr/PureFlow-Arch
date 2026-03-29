# 🌀 PureFlow-Arch: Medallion Lakehouse with Data Gatekeeper

**PureFlow-Arch** é uma plataforma de engenharia de dados de alta performance projetada para garantir a integridade e a qualidade dos dados em um ambiente de Lakehouse local. O projeto implementa o padrão de **Arquitetura de Medalhão** (Bronze, Silver, Gold) com um **Gatekeeper** (Circuit Breaker) ativo, utilizando o ecossistema Python moderno.

---

## 🏗️ Arquitetura e Fluxo de Dados

O projeto opera em um ambiente isolado via **Docker** no **Arch WSL**, simulando um pipeline de dados corporativo real. A imagem abaixo detalha como os componentes interagem:

![Arquitetura PureFlow-Arch](docs/pureflow_architecture.png)

### O Fluxo em Detalhes:

1.  **Landing Zone (MinIO/S3):** O ponto de entrada. Arquivos brutos (CSV/JSON) são recebidos via API S3 simulada pelo MinIO.
2.  **Ingestão (DuckDB):** O DuckDB lê os arquivos diretamente do S3 (via extensão `httpfs`), convertendo-os para **Parquet**.
3.  **Bronze Layer (Raw):** Armazenamento eficiente de arquivos Parquet, mantendo a fidelidade total à fonte (sem transformações).
4.  **Gatekeeper (Great Expectations):** A camada de confiança. Valida tipos, nulos e regras de negócio. Se o dado falhar nas "Expectativas", o pipeline é interrompido e o arquivo é movido para `/quarantine`.
5.  **Silver Layer (Cleansed):** Dados validados e normalizados, persistidos no formato **Delta Lake**, garantindo transações ACID e versionamento (Time Travel).
6.  **Gold Layer (Curated):** Transformações finais e agregações de negócio executadas pelo **dbt**. O produto final é materializado em tabelas analíticas dentro do arquivo `.db` do DuckDB, prontas para consumo.

---

## 🛠️ Stack Tecnológica

| Componente | Tecnologia | Papel Principal |
| :--- | :--- | :--- |
| **Orquestração** | Apache Airflow | Coordena o agendamento e a execução das tarefas (DAGs). |
| **Engine de Dados** | DuckDB | Processamento OLAP in-process de alta performance. |
| **Transformação** | dbt (duckdb-adapter) | Gerencia a linhagem (lineage) e modelos SQL. |
| **Qualidade** | Great Expectations | Validação de contratos de dados (Gatekeeper). |
| **Armazenamento** | MinIO + Delta Lake | Storage compatível com S3 e tabelas de alto desempenho. |
| **Ambiente** | Docker + Poetry | Isolamento de infraestrutura e gestão de dependências. |

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Docker & Docker Compose instalado.
* WSL2 (Ambiente testado: Arch Linux).
* Poetry (v2.0+) instalado no host (Arch).

### 1. Preparação
No terminal do seu Arch WSL, prepare as permissões e o ambiente:
```bash
# Cria pastas de volume e documentação
mkdir -p data/minio_data docs
touch README.md
poetry lock
```

### 2. Inicialização
Suba os serviços definidos no docker-compose.yml:

Bash
docker-compose up -d --build

### 3. Acessar as Interfaces
* Airflow UI: http://localhost:8080 (user: admin / pass: admin)
* MinIO Console: http://localhost:9001 (user: admin / pass: password123)
* Data Docs (Relatórios de Qualidade): Localizado em gx/uncommitted/data_docs/local_site/index.html

---

🛡️ O Diferencial: O Gatekeeper em Ação
Diferente de pipelines ETL comuns, o PureFlow-Arch foca na observabilidade. Durante a execução:
* Se um dado corrompido (ex: valor_venda negativo) tenta entrar na Silver, o Great Expectations detecta a anomalia.
* O Airflow recebe o sinal de falha e impede que o dbt processe a camada Gold com dados errados.
* O engenheiro de dados recebe um alerta e pode consultar o Data Doc HTML para ver exatamente qual linha e coluna causou o erro.

---

📄 Licença
Este projeto está sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.
