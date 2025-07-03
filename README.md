# Coleta Spark Hive

Automação de coleta e atualização de dados no Hive com PySpark.

Este script realiza a extração, limpeza e mesclagem de coletas diárias de múltiplas visões Hive, consolidando-as em tabelas finais versionadas. Ideal para pipelines de Data Lake e projetos de Data Quality.

## 🚀 Tecnologias Utilizadas

- Python 3.x
- Apache Spark (PySpark)
- Hive (HiveWarehouseConnector)
- HDFS

## 📂 Estrutura do Processo

1. Inicializa ambientes Spark, Hive e HDFS.
2. Para cada coleta definida:
   - Remove tabelas auxiliares anteriores.
   - Cria nova tabela com dados do dia.
   - Mescla com dados históricos, evitando duplicações.
   - Atualiza a tabela final.
3. Gera logs detalhados com marcação de tempo e seções.

## 📦 Pré-requisitos

- Spark com HiveWarehouseConnector configurado
- Hive acessível via JDBC
- Permissões de escrita em HDFS e banco Hive
- Ambiente Hadoop configurado

## 🧾 Exemplo de Coleta

```python
coletas = [
    ('Clientes Ativos', 'vw_clientes_ativos', 'tb_clientes'),
    ('Produtos Vendidos', 'vw_produtos_vendidos', 'tb_produtos')
]
