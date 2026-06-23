import pytest
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                         ArrayType, DateType, FloatType, TimestampType,
                         BooleanType, DecimalType, DoubleType, IntegerType)
from processing.transformations import Transformations
from datetime import datetime

schema_pagamentos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DoubleType(), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", TimestampType(), True),
    StructField(
        "avaliacao_fraude",StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", FloatType(), True)
            ]),
        True
        )
    ])
    
schema_pedidos = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", DoubleType(), True),
    StructField("QUANTIDADE", IntegerType(), True),
    StructField("DATA_CRIACAO", TimestampType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", IntegerType(), True)
])

schema_df_final = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("data_criacao", TimestampType(), True)
])


class TestMudarNomeColunasPedidos:
    
    """O DataFrame resultante deve estar com o nome das colunas em caixa baixa"""
    def test_mudar_nome_colunas_pedidos(self, spark):
        transform = Transformations()
        
        dados = [("1", "TV", 2500.0, 1, datetime(2025,1,1,0,0,0), "RN", 123)]
        pedidos_df = spark.createDataFrame(dados, schema=schema_pedidos)
        
        
        colunas_esperadas = {
            "id_pedido",
            "produto",
            "valor_unitario",
            "quantidade",
            "data_criacao",
            "uf",
            "id_cliente"
        }
        
        resultado = transform.mudar_nome_colunas_pedidos(pedidos_df)
        assert set(resultado.columns) == colunas_esperadas
        
class TestAddValorTotalPedidos:
    def test_calcula_valor_total(self, spark):
        """valor_total deve ser diferente"""
        transform = Transformations()
        df = spark.createDataFrame(
                [("1", "TV", 2500.0, 2, datetime(2025,1,1,0,0,0), "RN", 123)], schema_pedidos,
            )
        resultado = transform.add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(5000.0)
        
class TestJoinAndFilter:
    @pytest.fixture
    def pedidos_df(self,spark):
        transform = Transformations()
        dados = [("1", "TV", 2500.0, 1, datetime(2025,1,1,0,0,0), "RN", 123)]
        pedidos_df = spark.createDataFrame(dados,schema=schema_pedidos)
        pedidos_df = transform.mudar_nome_colunas_pedidos(pedidos_df)
        pedidos_df = transform.add_valor_total_pedidos(pedidos_df)
        return pedidos_df
    
    
    @pytest.fixture
    def pagamentos_df(self, spark):
        dados = [
            ("1", "Débito", 5000.0, True, datetime(2025,1,1,0,0,0),  (False, 8.2))
        ]
        return spark.createDataFrame(dados, schema_pagamentos)
        
    def test_resultado_contem_apenas_as_colunas_esperadas(self, spark, pagamentos_df, pedidos_df):
        transform = Transformations()
        """O relatório deve expor somente id_pedido, uf, forma_pagamento, valor_total, data_criacao"""
        resultado = transform.join_and_filter(pagamentos_df, pedidos_df)
        assert resultado.columns == ['id_pedido', "uf", "forma_pagamento", "valor_total", "data_criacao"]
        
