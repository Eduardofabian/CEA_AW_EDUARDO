#Adventure Works Data Warehouse

Este projeto é um pipeline de Engenharia de Dados moderno construído com dbt, transformando dados brutos da Adventure Works em um modelo dimensional pronto para Analytics.

## Arquitetura do Projeto
O projeto segue a arquitetura Modern Data Stack dividida em três camadas:
1.  **Staging (`dw_adventureworks`):**
    - Limpeza de nomes de colunas, casts e remoção de dados nulos.
    - Materialização: *View*.
2.  **Intermediate:**
    - Aplicação de regras de negócio, joins complexos e pré-cálculos.
    - Materialização: *View*.
3.  **Marts:**
    - Tabelas Fato e Dimensões finais, no modo Star Schema prontas para o BI.
    - Materialização: *Table* .

## Como Rodar

```bash
dbt deps
dbt build
dbt docs generate