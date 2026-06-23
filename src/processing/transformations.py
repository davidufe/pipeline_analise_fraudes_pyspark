from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Transformations:
    """Classe que contém as transformações e regras de negócio da aplicação."""

    def mudar_nome_colunas_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Muda os nomes das colunas de pedidos pra estar no mesmo padrão de pagamentos"""
        pedidos_df = (
            pedidos_df.withColumnRenamed("ID_PEDIDO", "id_pedido")
            .withColumnRenamed("PRODUTO", "produto")
            .withColumnRenamed("VALOR_UNITARIO", "valor_unitario")
            .withColumnRenamed("QUANTIDADE", "quantidade")
            .withColumnRenamed("DATA_CRIACAO", "data_criacao")
            .withColumnRenamed("UF", "uf")
            .withColumnRenamed("ID_CLIENTE", "id_cliente")
        )
        return pedidos_df

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' ao DataFrame de Pedidos."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def join_and_filter(
        self, pagamentos_df: DataFrame, pedidos_df: DataFrame
    ) -> DataFrame:
        """
        Faz a junção entre os DataFrames de pedidos e pagamentos, através do id_pedido
        Traz os registros em que o sistema recusou o pagamento (status=False) & o sistema acusou como legítimos (fraude = False)
        de 2025. Trazendo somente as colunas id_pedido, uf, forma_de_pagamento, valor_total, data_criacao
        """

        df = (
            pagamentos_df.join(
                pedidos_df, pedidos_df.id_pedido == pagamentos_df.id_pedido, "inner"
            )
            .filter(
                (F.col("status") == False)
                & (F.col("avaliacao_fraude.fraude") == False)
                & (F.year("data_criacao") == 2025)
            )
            .select(
                pagamentos_df.id_pedido,
                pedidos_df.uf,
                pagamentos_df.forma_pagamento,
                pedidos_df.valor_total,
                pedidos_df.data_criacao,
            )
            .orderBy("uf", "forma_pagamento", "data_criacao")
        )
        return df
