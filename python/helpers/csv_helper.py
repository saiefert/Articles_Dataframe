import pandas as pd
import re


class CSVHelper():
    @staticmethod
    def read_csv_to_pandas(path_csv: str) -> pd.DataFrame:
        """Lê os arquivos CSV e transforma em DataFrame pandas

        Args:
            path_csv (str): Caminho do arquivo CSV que será transformado

        Returns:
            pd.DataFrame: Arquivo CSV após transformação
        """
        df = pd.read_csv(
            path_csv, sep=';',
            low_memory=False,
            on_bad_lines='skip',
            skip_blank_lines=True
            )
        return df

    @staticmethod
    def cleaner_columns_csv(df: pd.DataFrame, column_check: str, column_new_name: str) -> None: # noqa
        """Trata as colunas do DataFrame CSV

        Args:
            df (pd.DataFrame): DataFrame que será tratado
            column_check (str): Coluna chegar se existe algo
            column_new_name (str): Novo nome da coluna
        """
        df.columns = df.columns.str.lower()

        for column in df.columns:
            if str(column).__contains__(column_check):
                df.rename(columns={column: column_new_name}, inplace=True)

            column_regex = re.sub(r"([.])|([ ])", "_", column)
            column_regex = re.sub(r"([(])|([)])|([/])", "", column_regex)
            df.rename(columns={column: column_regex}, inplace=True)
