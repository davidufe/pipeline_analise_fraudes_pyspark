import logging
from py4j.protocol import Py4JJavaError #--> Tratamento para erros de JVM
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                         ArrayType, DateType, FloatType, TimestampType,
                         BooleanType, DecimalType, DoubleType, IntegerType)

class DataHandler:
    """Classe responsável pelo input e output dos daods (I|O)"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _get_schema_pagamentos(self) -> StructType:
        """"Define e retorna o schema para o DataFrame de Pagamentos."""
        return StructType([
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
            
    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema do DataFrame de Pedidos."""
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", DoubleType(), True),
            StructField("QUANTIDADE", IntegerType(), True),
            StructField("DATA_CRIACAO", TimestampType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", IntegerType(), True)
            ])
    
    def load_pagamentos(self, path: str, compression: str) ->DataFrame:
        try:
            """Carrega o DataFrame de Pagamentos."""
            schema = self._get_schema_pagamentos()
            df = self.spark.read.option("compression", compression) \
                .option("mode", "FAILFAST") \
                .json(path, schema=schema)
                
            #Verificação de DataFrame vazio
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}'' foi lido mas não contém registros")
            
            return df
        
        #Erro de I/O
        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e #Relança o erro para parar o pipeline
        
        #Erro de JVM
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}")
            raise e
        
    def load_pedidos(self, path: str, compression: str, header: bool, sep: str) ->DataFrame:
        try:
            """Carrega o DataFrame de Pedidos."""
            schema = self._get_schema_pedidos()
            #mode: FAILFAST falha a execução caso o dado não esteja de acordo com o schema
            df = self.spark.read.option("compression", compression) \
                .option("mode", "FAILFAST") \
                .csv(path,header=header,schema=schema, sep=sep)
            
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros")
            
            return df
        
        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e
        
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}")
            raise e
        
    def write_parquet(self, df:DataFrame, path:str):
        """Salva o DataFrame em formato parquet, sobrescrevendo o que já existe."""
        df.write.mode("overwrite").parquet(path)
        print(f"\nDados salvos com sucesso em: {path}\n")
        
    