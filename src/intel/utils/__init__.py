import sys 
import yaml 
from src.intel.exception.exception import IntelException
from src.intel.constants import *

def read_yaml_file(file_path: str) -> dict:
    try: 
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
            
    except Exception as e:
        raise IntelException(e,sys) from e