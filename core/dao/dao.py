import pandas as pd

from core.scrap import download
from core.utils.requests import UrlBuildeR
from core.exceptions import TabelaIndisponivel

DOMAIN='repositorio.dados.gov.br'
NAMESPACE='/seges/detru/'

MAPPER_TABELAS = {
    'programas' : 'siconv_programa.csv.zip',
    'programa_proponente' : 'siconv_programa_proponentes.csv.zip'
}

class DAO:

    def __init__(self):

        self.download = download
        self.build_url = UrlBuildeR(DOMAIN)
        self.tables = self.__init_tables()

    def __init_tables(self)->dict:

        return {table : None for table in MAPPER_TABELAS.keys()}

    def __url_csvs(self, nome_tabela:str)->str:

        if nome_tabela not in MAPPER_TABELAS:
            raise TabelaIndisponivel(nome_tabela)
        
        file_tabela = MAPPER_TABELAS[nome_tabela]
        url = self.build_url(NAMESPACE, file_tabela)
        return url

    def __download_table(self, table_name:str)->pd.DataFrame:

        url = self.__url_csvs(table_name)

        return self.download(url, parse=True)

    def __cached_download(self, table_name:str)->pd.DataFrame:
        
        if table_name not in self.tables:
            raise TabelaIndisponivel(table_name)

        if self.tables[table_name] is not None:
            return self.tables[table_name]
        else:
            df = self.__download_table(table_name)
            self.tables[table_name]=df

            return df

    @property
    def programas(self)->pd.DataFrame:

        return self.__cached_download('programas')
    
    @property
    def programa_proponente(self)->pd.DataFrame:

        return self.__cached_download('programa_proponente')
        
        

    