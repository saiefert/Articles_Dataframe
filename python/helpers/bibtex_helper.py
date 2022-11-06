import pandas as pd
import bibtexparser
import os
import re


class BibtexHelper():

    @staticmethod
    def read_bibtex_files(path_files: str) -> pd.DataFrame:
        """Faz a leitura dos arquivos .bib da pasta solicitada.

        Args:
            path_files (str): recebe uma string com o
            path dos arquivos que serão lidos.

        Returns:
            pd.DataFrame: Retorna o DataFrame de todos os
            arquivos concatenados.
        """
        list_files = os.listdir(path_files)
        df_main = pd.DataFrame()

        for file in list_files:
            print(f'Arquivos listado no Dataframe: {file}')
            with open(
                    f"{path_files}/{file}",
                    encoding='utf8'
                    ) as bibtex_file:
                bib_database = bibtexparser.load(bibtex_file)

                df = pd.DataFrame(bib_database.entries)
                df_main = pd.concat([df_main, df])

        return df_main

    @staticmethod
    def cleaner_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Faz a limpeza das colunas do DataFrame de acordo
        com a lista que se deseja.

        Args:
            df (pd.DataFrame): recebe o DataFrame que
            será feito a limpeza de colunas.

        Returns:
            pd.DataFrame: Retorna o DataFrame apenas
            com as colunas solicitadas.
        """
        df.columns = df.columns.str.lower()

        for column in df.columns:
            column_regex = re.sub(r"([.])|([ ])", "_", column)
            df.rename(columns={column: column_regex}, inplace=True)
            if column == 'entrytype':
                df.rename(columns={column: 'type_publication'}, inplace=True)

        return df
