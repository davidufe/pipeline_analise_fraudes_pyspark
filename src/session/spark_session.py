from pyspark.sql import SparkSession


class SparkSessionManager:
    """Classe que irá gerenciar a criaão e acesso à sessão Spark."""

    @staticmethod
    def get_spark_session(app_name: str = "analise-fraudes") -> SparkSession:
        """
        Cria e retorna uma sessão Spark.

        app_name: Nome da aplicação Spark.
        return: Instância da SparkSession.
        """
        return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()

        # .master("local"): Execute localmente nesta máquina
        # utilizando todos os núcleos de CPU disponíveis
