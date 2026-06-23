from config.settings import carregar_config  # lê o arquivo de configuração yaml
from session.spark_session import SparkSessionManager  # cria a sessão spark
from io_utils.data_handler import DataHandler  # io dos dataframes
from processing.transformations import Transformations  # tratamento e regras
from pipeline.pipeline import Pipeline

import logging


def configurar_logging():
    """Configura o logging para todo o projeto"""
    logging.basicConfig(
        # Nível mínimo de severidade para ser registrado
        # DEBUG < INFO < WARNING < ERROR < CRITICAL
        level=logging.INFO,
        # Formato da mensagem de log
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # Lista de handlers. Aqui, estamos logando para um arquivo e para o console
        handlers=[
            logging.FileHandler("trabalho.log"),  # Log para o arquivo
            logging.StreamHandler(),  # Log para o console
        ],
    )
    logging.info("\nLogging configurado")


def main():
    logger = logging.getLogger(__name__)  # carrega o logging

    config = carregar_config()  # carrega as configurações
    app_name = config["spark"]["app_name"]
    logger.info(f"\nObtido o app name: {app_name}")
    spark = None  # Inicia como None para segurança no finally

    try:
        spark = SparkSessionManager.get_spark_session(
            app_name=app_name
        )  # cria a sessão spark

        data_handler = DataHandler(
            spark
        )  # iniciando DataHandler para rodar na pipeline
        transformer = (
            Transformations()
        )  # iniciando Transformation para todar na pipeline
        pipeline = Pipeline(data_handler, transformer)
        pipeline.run(config=config)

    except Exception as e:
        logging.error(f"FALHA CRÍTICA NA PIPELINE: {e}")

    finally:
        if spark:
            spark.stop()
            logging.info("\nSessão Spark Finalizada.")


if __name__ == "__main__":
    configurar_logging()
    main()
