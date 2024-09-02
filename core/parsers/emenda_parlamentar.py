import pandas as pd

from core.dao import DAO
from core.utils.datetime import dmy_series_to_datetime
from config import COLUNAS_DADOS, COD_PROPONENTE_CIDADE

class EmendasParlamentares:

    COLUNAS_DT_PRAZO = {'inicio' : 'DT_PROG_INI_EMENDA_PAR', 
                    'fim' : 'DT_PROG_FIM_EMENDA_PAR'}

    def __init__(self, dao_obj:DAO)->None:

        self.dao = dao_obj
        self.__set_tables()

    def __set_tables(self):

        self.programas = self.dao.programas.copy(deep=True)
        self.programa_proponente = self.dao.programa_proponente.copy(deep=True)
        self.__colunas_dt()
        self.__solve_ids()
        self.__filtrar_proponente_sp()


    def __solve_ids(self):

        self.programas['ID_PROGRAMA'] = self.programas['ID_PROGRAMA'].astype(int)
        self.programa_proponente['ID_PROGRAMA'] = self.programa_proponente['ID_PROGRAMA'].astype(int)
        self.programa_proponente['ID_PROPONENTE'] = self.programa_proponente['ID_PROPONENTE'].astype(int)

    def __filtrar_proponente_sp(self):

        proponente_is_sp = self.programa_proponente['ID_PROPONENTE']==int(COD_PROPONENTE_CIDADE)
        
        if proponente_is_sp.sum()<1:
            print('Não há nenhuma proposta com São Paulo como proponente.')

        self.programa_proponente = self.programa_proponente[proponente_is_sp]


    def __colunas_dt(self):

        for col in self.COLUNAS_DT_PRAZO.values():
            self.programas[col] = dmy_series_to_datetime(self.programas[col])
            
    def __recebimento_iniciado(self):

        self.programas['recebimento_iniciado'] = self.programas[
                                                    self.COLUNAS_DT_PRAZO['inicio']
                                                    ]<=pd.Timestamp.today()

    def __aux_fim_recebimento_vazio(self, row)->bool:

        if row['recebimento_nao_finalizado'] == True:
            return True

        if pd.isnull(row[self.COLUNAS_DT_PRAZO['fim']]):
            return True
        
        return False
    
    def __rebecimento_nao_finalizado(self):

        self.programas['recebimento_nao_finalizado'] = self.programas[
                                                        self.COLUNAS_DT_PRAZO['fim']
                                                        ]>=pd.Timestamp.today()
        self.programas['recebimento_nao_finalizado'] = self.programas.apply(self.__aux_fim_recebimento_vazio, axis=1)

    def __programa_disponivel(self):

        self.programas['programa_disponivel'] = self.programas['SIT_PROGRAMA']!='INATIVO'

    def __nao_e_proponente_especifico(self):

        self.programas['not_prop_especifico'] = self.programas['DT_PROG_INI_BENEF_ESP'].isnull()

    def __nao_e_voluntaria(self):

        self.programas['not_voluntaria'] = self.programas['DT_PROG_INI_RECEB_PROP'].isnull()

    def __proponente_is_sp(self):

        programas_sp = self.programa_proponente['ID_PROGRAMA']
        self.programas['proponente_is_sp'] = self.programas['ID_PROGRAMA'].isin(programas_sp)

    def __create_cols_filtros(self):

        self.__recebimento_iniciado()
        self.__rebecimento_nao_finalizado()
        self.__programa_disponivel()
        self.__proponente_is_sp()
        self.__nao_e_proponente_especifico()
        self.__nao_e_voluntaria()

    def __filtro_final(self):

        colunas_filtros = [
            'recebimento_iniciado',
            'recebimento_nao_finalizado',
            'programa_disponivel',
            'proponente_is_sp',
            'not_prop_especifico',
            'not_voluntaria'
        ]

        self.programas['is_interesse'] = self.programas[colunas_filtros].all(axis=1)

    def __filtrar(self):

        self.__create_cols_filtros()
        self.__filtro_final()
        self.programas = self.programas[self.programas['is_interesse']].reset_index(drop=True)

    def __selecionar_colunas(self):

        self.programas = self.programas[COLUNAS_DADOS].copy()

    def __rename_colunas(self):

        self.programas.rename({col : col.lower() for col in COLUNAS_DADOS}, inplace=True, axis=1)

    def __pipeline(self):

        if not self.dao.check_download_alive('programas'):
            #reset tables if download is stale
            self.__set_tables()

        self.__filtrar()
        self.__selecionar_colunas()
        self.__rename_colunas()
    
    def __call__(self, json=True):

        self.__pipeline()
        if not json:
            return self.programas
        return self.programas.to_dict(orient='records')