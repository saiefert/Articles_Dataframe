import pandas as pd
import bibtexparser
import os

def read_files(path_files: str) -> pd.DataFrame:
    """Faz a leitura dos arquivos .bib da pasta solicitada.

    Args:
        path_files (str): recebe uma string com o path dos arquivos que serão lidos.

    Returns:
        pd.DataFrame: Retorna o DataFrame de todos os arquivos concatenados.
    """
    list_files = os.listdir(path_files)
    main_dataframe = pd.DataFrame()

    for file in list_files:
        print(f'Arquivos listado no Dataframe: {file}')
        with open(f"{path_files}/{file}") as bibtex_file:
            bib_database = bibtexparser.load(bibtex_file)
            
            df = pd.DataFrame(bib_database.entries)
            main_dataframe = pd.concat([main_dataframe, df])

    return main_dataframe
            