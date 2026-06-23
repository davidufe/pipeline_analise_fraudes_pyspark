import gzip
import json
import os
import pytest
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                         ArrayType, DateType, FloatType, TimestampType,
                         BooleanType, DecimalType, DoubleType, IntegerType)
from io_utils.data_handler import DataHandler

@pytest.fixture
def arquivo_pagamentos_gz(tmp_path):
    """Arquivo JSON gzipado com dois clientes de exemplo."""
    pagamentos = [
        {"id_pedido": "0fc8a4c7-d9e3-4b58-8ebd-41a833d4c9a7", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 1100.0, "status": True, "data_processamento": "2024-01-21T17:07:43.329582", "avaliacao_fraude": {"fraude": False, "score": 0.27}},
        {"id_pedido": "6376d1a0-0bc8-4470-a45e-7305720de5a0", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 600.0, "status": True, "data_processamento": "2024-01-08T22:32:51.092299", "avaliacao_fraude": {"fraude": False, "score": 0.46}}
    ]
    
    gz_path = tmp_path/ "pagamentos.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for p in pagamentos:
            f.write(json.dumps(p) + "\n")
    return str(gz_path)
    
@pytest.fixture
def arquivo_pedidos_gz(tmp_path):
    """Arquivo CSV gzipado com três pedidos de exemplo."""
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
        "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
        "abc-003;MONITOR;800.0;3;2024-01-03T12:00:00;MG;1",
    ]
    gz_path = tmp_path / "pedidos.csv.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    return str(gz_path)

class TestLoadPagamentos:
    def test_le_gzon_gz_e_retorna_dataframe(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(arquivo_pagamentos_gz, compression = "gzip")
        assert (df.count() == 2) & (len(df.columns) == 6) #2 linhas e 6 colunas
        
class TestLoadPedidos:

    def test_le_csv_gz_com_separador_ponto_e_virgula(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz, compression="gzip", header=True, sep=";",
        )
        assert (df.count() == 3) & (len(df.columns) == 7)
        
class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
        """Verificar só a criação do diretório não basta: relemos para garantir integridade."""
        schema = StructType([
            StructField("id_cliente", LongType(), True),
            StructField("valor_total", FloatType(), True),
        ])
        df = spark.createDataFrame([(1, 3000.0), (2, 300.0)], schema)
        output_path = str(tmp_path / "saida_parquet")

        DataHandler(spark).write_parquet(df, output_path)

        assert os.path.exists(output_path)
        assert spark.read.parquet(output_path).count() == 2