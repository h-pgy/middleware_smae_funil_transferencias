import pandas as pd

from typing import Union

from core.scrap import download
from core.utils.requests import UrlBuildeR
from core.utils.datetime import agora_unix_timestamp
from core.exceptions import TabelaIndisponivel

from config import DOWNLOAD_TTL_SECS

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

        try:
            self.download_ttl = int(DOWNLOAD_TTL_SECS)
        except ValueError:
            raise RuntimeError(f'Env var DOWNLOAD_TTL_SECONDS must be integer')

    def __init_tables(self)->dict:

        return {table : {'data' : None, 'last_download': None} for table in MAPPER_TABELAS.keys()}

    def __url_csvs(self, nome_tabela:str)->str:

        if nome_tabela not in MAPPER_TABELAS:
            raise TabelaIndisponivel(nome_tabela)
        
        file_tabela = MAPPER_TABELAS[nome_tabela]
        url = self.build_url(NAMESPACE, file_tabela)
        return url

    def __download_table(self, table_name:str)->pd.DataFrame:

        url = self.__url_csvs(table_name)

        return self.download(url, parse=True)
    
    def __check_download_alive(self, last_download:Union[int, None])->bool:

        #pode ser None caso tenha acabado de instanciar a classe
        if last_download is None:
            return False
        agora = agora_unix_timestamp()
        segundos_passados = agora - last_download
        return segundos_passados < self.download_ttl


    def __cached_download(self, table_name:str)->pd.DataFrame:
        
        if table_name not in self.tables:
            raise TabelaIndisponivel(table_name)

        dados = self.tables[table_name]['data']
        last_download = self.tables[table_name]['last_download']
        if dados is not None and self.__check_download_alive(last_download):
            return self.tables[table_name]['data']
        else:
            df = self.__download_table(table_name)
            self.tables[table_name]['data']=df
            self.tables[table_name]['last_download'] = agora_unix_timestamp()

            return df

    @property
    def programas(self)->pd.DataFrame:

        return self.__cached_download('programas')
    
    @property
    def programa_proponente(self)->pd.DataFrame:

        return self.__cached_download('programa_proponente')
        
        

    