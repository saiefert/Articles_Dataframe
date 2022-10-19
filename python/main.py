import pandas as pd
import bibtexparser
import os
import yaml
from yaml.loader import SafeLoader


class ArticlesReader():

    def read_files(path_files: str) -> pd.DataFrame:
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
            with open(f"{path_files}/{file}",  encoding='utf8') as bibtex_file:
                bib_database = bibtexparser.load(bibtex_file)

                df = pd.DataFrame(bib_database.entries)
                df_main = pd.concat([df_main, df])

        return df_main

    def cleaner_columns(df: pd.DataFrame, columns_list: list) -> pd.DataFrame:
        """Faz a limpeza das colunas do DataFrame de acordo
        com a lista que se deseja.

        Args:
            df (pd.DataFrame): recebe o DataFrame que
            será feito a limpeza de colunas.
            columns_list (list): lista de colunas que
            será mantida no DataFrame.

        Returns:
            pd.DataFrame: Retorna o DataFrame apenas
            com as colunas solicitadas.
        """
        for column in df.columns:
            if column == "ENTRYTYPE":
                df.rename(
                    columns={'ENTRYTYPE': 'type_publication'},
                    inplace=True
                    )
                column = "type_publication"
                print('Coluna renomeada')
            if column not in columns_list:
                df = df.drop(column, axis="columns")
                print(f"Dataframe com coluna retirada: {column}")

        return df

    def concat_dfs(df_1: pd.DataFrame, df_2: pd.DataFrame) -> pd.DataFrame:
        """Concatena os dataframes.

        Args:
            df_1 (pd.DataFrame): que será concatenado
            df_2 (pd.DataFrame): que será concatenado

        Returns:
            pd.DataFrame: dataframe já concatenado
        """
        df_concat = pd.DataFrame()
        df_concat = pd.concat([df_1, df_2])

        return df_concat

    def save_df(df: pd.DataFrame, file_type: str) -> None:
        """Salva o DataFrame no formato solicitado.

        Args:
            df (pd.DataFrame): recebe o DataFrame que será salvo.
            file_type (str): recebe o tipo de formato de arquivo que a
            pessoa deseja escolher para salvar o DataFrame
        """
        file_type = file_type.lower()
        if (file_type == 'json'):
            df = df.to_json(orient="split")
            file = open(f"../resultados/results.{file_type}",
                        "w+", newline="",
                        encoding="utf-8"
                        )
            file.write(df)
            file.close()
            print(f'Arquivo salvo: resultados/results.{file_type}')

        elif (file_type == 'csv'):
            df = df.to_csv()
            file = open(f"../resultados/results.{file_type}",
                        "w+", newline="",
                        encoding="utf-8"
                        )
            file.write(df)
            file.close()
            print(f'Arquivo salvo: resultados/results.{file_type}')

        elif (file_type == 'yaml'):
            df = yaml.dump(
                df.reset_index().to_dict(orient='records'),
                sort_keys=False, width=72, indent=4,
                default_flow_style=None
            )
            file = open(f"../resultados/results.{file_type}",
                        "w+", newline="",
                        encoding="utf-8"
                        )
            file.write(df)
            file.close()
            print(f'Arquivo salvo: resultados/results.{file_type}')

        elif (file_type == 'xml'):
            df = df.to_xml()
            file = open(f"../resultados/results.{file_type}",
                        "w+", newline="",
                        encoding="utf-8"
                        )
            file.write(df)
            file.close()
            print(f'Arquivo salvo: resultados/results.{file_type}')

        else:
            print('Opção não suportada! As opções suportadas são: CSV, XML, Json, YAML') # noqa


if __name__ == '__main__':

    with open('../config.yaml') as f:
        config = yaml.load(f, Loader=SafeLoader)

    task = ArticlesReader

    df_IEEE = task.read_files(path_files='../dados/IEEE')
    df_ACM = task.read_files(path_files='../dados/ACM')
    df_SDC = task.read_files(path_files='../dados/SDC')

    df_main = task.concat_dfs(df_1=df_SDC, df_2=df_IEEE)
    df_main = task.concat_dfs(df_1=df_main, df_2=df_ACM)

    df_clean = task.cleaner_columns(
        df=df_main,
        columns_list=config['chosen_columns']
        )
    task.save_df(df=df_clean, file_type=config['file_saved_format'])
