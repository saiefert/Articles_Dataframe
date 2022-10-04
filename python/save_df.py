import pandas as pd
import yaml


def save_df(df: pd.DataFrame, file_type: str) -> None:
    """Salva o DataFrame no formato solicitado.

    Args:
        df (pd.DataFrame): recebe o DataFrame que será salvo.
        file_type (str): recebe o tipo de formato de arquivo
        que a pessoa deseja escolher para salvar o DataFrame
    """
    file_type = file_type.lower()
    if (file_type == 'json'):
        df = df.to_json(orient="split")
        file = open(f"resultados/results.{file_type}",
                    "w+", newline="",
                    encoding="utf-8"
                    )
        file.write(df)
        file.close()
        print(f'Arquivo salvo: resultados/results.{file_type}')

    elif (file_type == 'csv'):
        df = df.to_csv()
        file = open(f"resultados/results.{file_type}",
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
        file = open(
            f"resultados/results.{file_type}",
            "w+", newline="",
            encoding="utf-8"
            )
        file.write(df)
        file.close()
        print(f'Arquivo salvo: resultados/results.{file_type}')

    elif (file_type == 'xml'):
        df = df.to_xml()
        file = open(
            f"resultados/results.{file_type}",
            "w+", newline="",
            encoding="utf-8"
            )
        file.write(df)
        file.close()
        print(f'Arquivo salvo: resultados/results.{file_type}')

    else:
        print('Opção não suportada! As opções suportadas são: CSV, XML, Json, YAML') # noqa
