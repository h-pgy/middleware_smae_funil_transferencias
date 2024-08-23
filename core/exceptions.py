from fastapi import HTTPException

class UpstreamError(HTTPException):
    '''Raises quando o sistema consultado não está retornando as requisicoes'''
    def __init__(self, erro: str = None, headers: dict = None):

        detail = f'Erro sistema: {erro}'

        super().__init__(status_code=503, detail=detail, headers=headers)

