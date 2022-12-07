import sqlite3
import pandas as pd
from constants import Constants


class SQLite3Helper:

    @staticmethod
    def create_connection(db_file: str):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        con = None

        con = sqlite3.connect(db_file)
        print(f'Conexão criada, novo database: {db_file}')
        con.close()

    @staticmethod
    def insert_table_control(db_file: str, search_query: str, id_search: int, table_name: str) -> None:
        """Insere valores de contrele na tabela, caso não exista tabela, cria a tabela e insere

        Args:            
            db_file (str): Database que que receberá a tabela
            id_search (int): Chave para a busca nas tabelas de pesquisa
            search_query (str): Query de busca que será gravada para retornar busca local caso localizada
            table_name (str): Nome da tabela que será buscada, no caso tabela IEEE, SDC ou de BIBTEX
        """
        print(f"Criando tabela controle no database {db_file}")
        con = sqlite3.connect(db_file)
        create = f"CREATE TABLE IF NOT EXISTS {Constants.db_table_control()} (id INTEGER PRIMARY KEY AUTOINCREMENT, search_query varchar, id_search int, table_name varchar);"
        con.execute(create)

        search_q = ','.join(search_query)
        insert = f"insert into {Constants.db_table_control()} (search_query, id_search, table_name) values ({search_q}, {id_search}, {table_name})"
        con.execute(insert)

        con.close()

    @staticmethod
    def check_table_control(db_file: str, search_query: list, table_name: str):
        """Insere valores de contrele na tabela, caso não exista tabela, cria a tabela e insere

        Args:            
            db_file (str): Database que que receberá a tabela
            search_query (str): Verifica se existe na tabela cache a query
            table_name (str): Nome da tabela que será buscada, no caso tabela IEEE, SDC ou de BIBTEX
        """
        print(f"Criando tabela controle no database {db_file}")
        con = sqlite3.connect(db_file)
        create = f"CREATE TABLE IF NOT EXISTS {Constants.db_table_control()} (id INTEGER PRIMARY KEY AUTOINCREMENT, search_query varchar, id_search int, table_name varchar);"
        con.execute(create)

        search_q = ','.join(search_query)
        busca = f"select * from {Constants.db_table_control()} where search_query = {search_q} and table_name = {table_name})"
        query = con.execute(busca)

        con.close()
        return query.fetchall()

    @staticmethod
    def create_table(df: pd.DataFrame, db_file: str, table_name: str) -> None:
        """Cria a tabela em database utilizando um DataFrame Pandas

        Args:
            df (pd.DataFrame): DataFrame Pandas que será fixado na tabela
            db_file (str): Database que que receberá a tabela
            table_name (str): Nome da tabela que será criada ou substituida
        """
        print(f"Criando tabela {table_name} no database {db_file}")
        df = df.applymap(str)
        con = sqlite3.connect(db_file)
        df.to_sql(table_name, con, if_exists="replace")
        print("Tabela criada")

        con.close()

    @staticmethod
    def run_query(db_file: str, query: str) -> pd.DataFrame:
        """
        Query all rows in the tasks table
        :param conn: the Connection object
        :return:
        """
        print("Rodando Query")
        con = sqlite3.connect(db_file)
        df = pd.read_sql_query(query, con)
        con.close()
        print("DataFrame disponível com resultado da query")

        return df
