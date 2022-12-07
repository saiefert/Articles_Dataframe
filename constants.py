class Constants():

    @staticmethod
    def db_conn() -> str:
        return "df_concat_api.db"

    @staticmethod
    def db_table_bibtex() -> str:
        return "tb_bibtex_temp"

    @staticmethod
    def db_table_ieee() -> str:
        return "tb_iee"

    @staticmethod
    def db_table_sdc() -> str:
        return "tb_sdc"

    @staticmethod
    def db_table_control() -> str:
        return "db_table_control"

    @staticmethod
    def find_db_table(txt: str) -> str:
        if (txt == "ieee"):
            return Constants.db_table_ieee()
        elif (txt == "sdc"):
            return Constants.db_table_sdc()
        else:
            return ""
