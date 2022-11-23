import pandas as pd
import requests
import math
import yaml
import urllib.parse
from yaml.loader import SafeLoader


class ApiIEEEHelper():
    @staticmethod
    def api_key_ieee() -> str:
        """Busca APIKey no config.yaml

        Returns:
            str: APIKEY
        """
        with open('config_api_ieee.yaml') as f:
            config_api_ieee = yaml.load(f, Loader=SafeLoader)

            return config_api_ieee['apikey']

    @staticmethod
    def filter_list_query(filter_list: list, operator: str) -> str:
        """Transforma as configs no formato da query
        para o request

        Args:
            filter_list (list): lista com o que deseja filtrar
            operator (str): tipo do operador OR ou AND

        Returns:
            str: retorna a query já no formato
        """
        operator_query = f'+{operator}+'
        query_text = ''
        for index, value in enumerate(filter_list):
            value = urllib.parse.quote(value)
            if index == 0:
                query_text = query_text + value
            else:
                query_text = query_text + operator_query + value

    @staticmethod
    def define_pagination(total_records: int, limit_articles_per_page: int) -> int: # noqa
        """Calcula a paginação da API

        Args:
            total_records (int): Recebe o total de respostas que a API trará
            limit_articles_per_page (int): Limite de dados por request

        Returns:
            int: Retorna a quantidade de páginas necessárias para
            finalizar a busca
        """
        pagination = math.ceil(total_records/limit_articles_per_page)

        return pagination

    @staticmethod
    def search_articles_with_pages_ieee(filter_list_config: list, operator_config: str) -> pd.DataFrame: # noqa
        """Faz uma consulta via API no site da IEEE e devolve os
        artigos que se enquadram na busca, considerando a paginação.

        Args:
            filter_list_config (list): lista com as palavras que
            serão usadas na busca
            operator_config (str): tipo de operador na hora da busca.
            Ex: OR ou AND

        Returns:
            pd.DataFrame: Retorna o DataFrame pandas com os dados dos artigos
        """
        try:
            api_key = ApiIEEEHelper.api_key_ieee()
            df_main = pd.DataFrame()
            MAX_ARTICLES_PER_PAGES = 200
            SUCCESS_REQUEST = 200
            query_text = ApiIEEEHelper.filter_list_query(
                filter_list=filter_list_config,
                operator=operator_config
                )

            URL = f"https://ieeexploreapi.ieee.org/api/v1/search/articles?querytext={query_text}&format=json&apikey={api_key}&max_records=1" # noqa

            response = requests.get(
                URL
            )
            dict_df = response.json()
            pages = ApiIEEEHelper.define_pagination(
                total_records=dict_df['total_records'],
                limit_articles_per_page=MAX_ARTICLES_PER_PAGES
                )
            for page in range(pages):
                start_record = 1 + (page*200)
                URL = f"https://ieeexploreapi.ieee.org/api/v1/search/articles?querytext={query_text}&format=json&apikey={api_key}&max_records={MAX_ARTICLES_PER_PAGES}&start_records={start_record}&sort_order=asc" # noqa
                response = requests.get(URL)
                if response.status_code == SUCCESS_REQUEST:
                    dict_df = response.json()
                    dict_df = dict_df['articles']
                    df = pd.json_normalize(data=dict_df)
                    df_main = pd.concat([df_main, df])

            return df_main

        except Exception as e:
            raise e

    @staticmethod
    def search_articles_without_pages_sdc(filter_list_config: list, operator_config: str) -> pd.DataFrame: # noqa
        """Faz uma consulta via API no site da IEEE e devolve
        os artigos que se enquadram na busca, não considerando a paginação.

        Args:
            filter_list_config (list): lista com as palavras que serão usadas
            na busca
            operator_config (str): tipo de operador na hora da busca.
            Ex: OR ou AND

        Returns:
            pd.DataFrame: Retorna o DataFrame pandas com os dados dos artigos
        """
        try:
            api_key = ApiIEEEHelper.api_key_ieee()
            df_main = pd.DataFrame()
            SUCCESS_REQUEST = 200
            query_text = ApiIEEEHelper.filter_list_query(
                filter_list=filter_list_config,
                operator=operator_config
                )

            URL = f"https://ieeexploreapi.ieee.org/api/v1/search/articles?querytext={query_text}&format=json&apikey={api_key}&max_records=1" # noqa

            response = requests.get(
                URL
            )
            if response.status_code == SUCCESS_REQUEST:
                dict_df = response.json()
                dict_df = dict_df['articles']
                df = pd.json_normalize(data=dict_df)
                df_main = pd.concat([df_main, df])

            return df_main

        except Exception as e:
            raise e
