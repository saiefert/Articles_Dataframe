from typing import Union
from fastapi import FastAPI, File, UploadFile
import sqlite3
import json
from python.helpers.create_dictionary import create_dictionary
from python.helpers.bibtex_helper import BibtexHelper
from python.helpers.sqllite_helper import SQLite3Helper
import yaml
from yaml.loader import SafeLoader
from python.helpers.api_ieee import ApiIEEEHelper
from python.helpers.api_sdc import ApiSDCHelper
from constants import Constants

app = FastAPI()
app.title = "MBA Impacta"
app.description = "API de estudo da aula do MBA de Engenharia de Dados, com o foco em estudar Python para Engenheiro de Dados"


@app.get("/")
def read_root():
    return {"alunos": ["Alex Rossi", "Daniel", "Josmar Silva", "Lucas Sintra", "João Marcos"]}


@app.post("/upload/bibtex")
def upload_bibtex(file: UploadFile):

    file_location = f"temp/{file.filename}"
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())

    SQLite3Helper.create_table(BibtexHelper.read_bibtex_file(
        file_location), Constants.db_conn(), "tb_bibtex_temp")

    return json.dumps("Sucesso")


@app.post("/search/bibitex")
def pesquisa_com_bibtex(string_query: str):
    dicionario = create_dictionary()
    connection = sqlite3.connect(Constants.db_conn())
    df_query_response = connection.execute(string_query)

    for row in df_query_response:
        dicionario.add_item(
            row[0], ({"title": row[2], "publisher": row[3], "isbn": row[4]}))

    return json.dumps(dicionario, indent=2, sort_keys=True)


@app.post("/search/")
def pesquisa_cientifica(search_query: list, operator: str, api_type: str):
    dicionario = create_dictionary()
    connection = sqlite3.connect(Constants.db_conn())   
    
    if api_type == 'ieee':
        with open('config_api_ieee.yaml') as f:
            config = yaml.load(f, Loader=SafeLoader)
            df_api = ApiIEEEHelper.search_articles_with_pages_ieee(
                filter_list_config=config['search_query'],
                operator_config=config['operator']
            )
        connection.close()
        return df_api.to_json()

    elif api_type == 'sdc':
        df_api = ApiIEEEHelper.search_articles_without_pages_sdc(
            search_query,
            operator
            )
        SQLite3Helper.check_table_control(Constants.db_conn(), search_query,Constants.db_table_sdc())
        SQLite3Helper.create_table(df_api, Constants.db_conn(), 'tb_iee')
        connection.close()
        return df_api.to_json()
    else:
        connection.close()
        return "a api_type deve ser ieee ou sdc"


@app.get("/search/title")
def pesquisa_cientifica_title(title: str):
    connection = sqlite3.connect(Constants.db_conn())
    connection.row_factory = sqlite3.Row
    query = "SELECT * FROM concat where title like " + "'%"+title+"%'"
    df_query_response = connection.execute(query).fetchall()

    return json.dumps([dict(x) for x in df_query_response])


@app.get("/search/id")
def pesquisa_bibtex_id(id: str):
    connection = sqlite3.connect(Constants.db_conn())
    connection.row_factory = sqlite3.Row
    query = f"SELECT * FROM {Constants.db_table_bibtex()} where \"index\" = " + id
    df_query_response = connection.execute(query).fetchall()

    return json.dumps([dict(x) for x in df_query_response])


@app.get("/search/listar")
def lista_ultimas_10_pesquisas():
    tupla = tuple
    list = []
    connection = sqlite3.connect("df_concat_api.db")
    connection.row_factory = sqlite3.Row
    query = "SELECT * FROM concat order by \"index\" desc"
    df_query_response = connection.execute(query).fetchall()

    for row in df_query_response:
        tupla = ({"index": row[0], "title": row[2]})
        list.append(tupla)

    return json.dumps(list)


@app.post("/search/{search_query}")
def read_item(search_query: str, file: UploadFile):
    bib_tx = file.file.read()
    return {"query": bib_tx}
