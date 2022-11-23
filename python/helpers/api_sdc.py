import pandas as pd
import requests
import math
import yaml
import urllib.parse
from yaml.loader import SafeLoader


class ApiSDCHelper():

    @staticmethod
    def api_key_sdc() -> str:
        """Busca APIKey no config.yaml

        Returns:
            str: APIKEY
        """
        with open('config_api_sdc.yaml') as f:
            config_api_sdc = yaml.load(f, Loader=SafeLoader)

            return config_api_sdc['apikey']

    @staticmethod
    def define_pagination(total_records: int, limit_articles_per_page: int) -> int:
        """Calcula a paginação da API

        Args:
            total_records (int): Recebe o total de respostas que a API trará
            limit_articles_per_page (int): Limite de dados por request

        Returns:
            int: Retorna a quantidade de páginas necessárias para finalizar a busca
        """
        pagination = math.ceil(total_records/limit_articles_per_page)

        return pagination

    @staticmethod
    def search_articles_with_pages_sdc(filter_list: list, operator: str) -> pd.DataFrame:
        """Faz uma consulta via API no site da sdc e devolve os artigos que se enquadram na busca,
        considerando a paginação.

        Args:
            filter_list (list): lista com as palavras que serão usadas na busca
            operator (str): tipo de operador na hora da busca. Ex: OR ou AND

        Returns:
            pd.DataFrame: Retorna o DataFrame pandas com os dados dos artigos
        """
        try:
            operator_query = f'+{operator}+'
            query_text = ''
            df_main = pd.DataFrame()
            api_key = ApiSDCHelper.api_key_sdc()
            MAX_ARTICLES_PER_PAGES = 25
            SUCCESS_REQUEST = 200
            for index, value in enumerate(filter_list):
                value = urllib.parse.quote(value)
                if index == 0:
                    query_text = query_text + value
                else:
                    query_text =query_text + operator_query + value 

            URL = f"https://api.elsevier.com/content/search/sciencedirect?query={query_text}&httpAccept=application%2Fjson&apikey={api_key}"
            response = requests.get(
                URL
            )
            dict_df = response.json()
            pages = ApiSDCHelper.define_pagination(
                total_records=int(dict_df['search-results']['opensearch:totalResults']),
                limit_articles_per_page=MAX_ARTICLES_PER_PAGES
                )
            for page in range(pages):
                start_record = 0 + (page*MAX_ARTICLES_PER_PAGES)
                URL = f"https://api.elsevier.com/content/search/sciencedirect?query={query_text}&httpAccept=application%2Fjson&apikey={api_key}&start={start_record}"
                response = requests.get(URL)
                if response.status_code == SUCCESS_REQUEST:
                    dict_df = response.json()
                    dict_df = dict_df['search-results']['entry']
                    df = pd.json_normalize(data=dict_df)
                    df_main = pd.concat([df_main, df])

            return df_main
        
        except Exception as e:
            raise e

    @staticmethod
    def search_articles_without_pages_sdc(filter_list: list, operator: str) -> pd.DataFrame:
        """Faz uma consulta via API no site da sdc e devolve os artigos que se enquadram na busca,
        não considerando a paginação.

        Args:
            filter_list (list): lista com as palavras que serão usadas na busca
            operator (str): tipo de operador na hora da busca. Ex: OR ou AND

        Returns:
            pd.DataFrame: Retorna o DataFrame pandas com os dados dos artigos
        """
        try:
            operator_query = f'+{operator}+'
            query_text = ''
            df_main = pd.DataFrame()
            api_key = ApiSDCHelper.api_key_sdc()
            SUCCESS_REQUEST = 200
            for index, value in enumerate(filter_list):
                value = urllib.parse.quote(value)
                if index == 0:
                    query_text = query_text + value
                else:
                    query_text =query_text + operator_query + value 

            URL = f"https://api.elsevier.com/content/search/sciencedirect?query={query_text}&httpAccept=application%2Fjson&apikey={api_key}"
            response = requests.get(
                URL
            )
            if response.status_code == SUCCESS_REQUEST:
                dict_df = response.json()
                dict_df = dict_df['search-results']['entry']
                df = pd.json_normalize(data=dict_df)
                df_main = pd.concat([df_main, df])

            return df_main
            
        except Exception as e:
            raise e

    @staticmethod
    def clean_columns_sdc(df: pd.DataFrame) -> None:
        """Retira as sujeiras do nome do DataFrame

        Args:
            df (pd.DataFrame): DataFrame que possui ":" no nome da coluna
        """
        for column in df.columns:
            if column.__contains__(':'):
                rename = column.split(':')
                df.rename(columns={column: rename[1]}, inplace=True)