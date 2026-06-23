"""
A classe Pipeline não cria as duas dependências: ela recebe pronta no construtor.
Em vez de instancial DataHandler e Transformations internamente, o Pipeline apenas declara
que precisa desses colaboradores e confia que alguém os fornecerá. Essse alguém será o main.py
"""

from io_utils.data_handler import DataHandler
from processing.transformations import Transformations
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """Encapsula a lógica de execução do pipeline de dados"""

    def __init__(self, data_handler: DataHandler, transformer: Transformations):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """Executa o pipeline completo: carga, transformação e salvamento"""
        logger.info("\nPipeline iniciado\n")

        # --------------------INGESTÃO DOS DADOS----------------------

        ##PAGAMENTOS
        logger.info("Abrindo o DataFrame de Pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]
        compression_pagamentos = config["file_options"]["pagamentos_json"][
            "compression"
        ]
        pagamentos = self.data_handler.load_pagamentos(
            path=path_pagamentos, compression=compression_pagamentos
        )
        pagamentos.show(5, truncate=False)

        ##PEDIDOS

        logger.info("\nAbrindo o DataFrame de Pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separador_pedidos = config["file_options"]["pedidos_csv"]["sep"]
        pedidos = self.data_handler.load_pedidos(
            path=path_pedidos,
            compression=compression_pedidos,
            header=header_pedidos,
            sep=separador_pedidos,
        )
        pedidos.show(5, truncate=False)

        # --------------------PROCESSAMENTO DOS DADOS----------------------
        logger.info("\nRenaming das colunas do DataFrame de Pedidos")
        pedidos = self.transformer.mudar_nome_colunas_pedidos(pedidos_df=pedidos)

        logger.info("\nAdicionando a coluna 'valor_total' no DataFrame de Pedidos\n")
        pedidos = self.transformer.add_valor_total_pedidos(pedidos_df=pedidos)

        logger.info("\nGerando o DataFrame final")
        relatorio = self.transformer.join_and_filter(
            pedidos_df=pedidos, pagamentos_df=pagamentos
        )
        relatorio.show(5, truncate=False)

        logger.info("\nEscrevendo o relatorio em parquet")
        path_output = config["paths"]["output"]
        self.data_handler.write_parquet(df=relatorio, path=path_output)

        logger.info("\nPipeline concluída com sucesso!")
