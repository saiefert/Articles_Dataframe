import pandas as pd

def cleaner_columns(df: pd.DataFrame, columns_list: list ) -> pd.DataFrame:
    """Faz a limpeza das colunas do DataFrame de acordo com a lista que se deseja.

    Args:
        df (pd.DataFrame): recebe o DataFrame que será feito a limpeza de colunas.
        columns_list (list): lista de colunas que será mantida no DataFrame.

    Returns:
        pd.DataFrame: Retorna o DataFrame apenas com as colunas solicitadas.
    """
    for column in df.columns:
        if column not in columns_list: 
            df = df.drop(column, axis="columns")
            print(f"Dataframe com coluna retirada: {column}")

    return df

    