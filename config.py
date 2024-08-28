from dotenv import load_dotenv
from typing import Union
import os

def load_env_var(varname:str)->Union[str, int, float, bool]:


    if not os.path.exists('.env'):
        print('Creating environment from .env.example')
        with open('.env', 'w') as envfile:
            with open('.env.example', 'r') as envexample:
                content = envexample.read()
                envfile.write(content)

    load_dotenv()

    variable = os.getenv(varname)
    if variable is None:
        raise RuntimeError(f'Variavel de ambiente {varname} não definida.')
    
    return variable

DOWNLOAD_TTL_SECS = load_env_var('DOWNLOAD_TTL_SECS')
MAX_RETRIES = load_env_var('MAX_RETRIES')