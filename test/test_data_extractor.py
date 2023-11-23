import pytest
import re
import requests
from bs4 import BeautifulSoup
import os
import sys
from datetime import date, datetime

# add path to src
test_dir = os.path.dirname(os.path.abspath(__file__))
src_path = f"{test_dir}/../src"
if src_path not in sys.path:
    sys.path.append(src_path)

from  data_extractor import *



def test_convert_str_date_to_date_valid_date():
    # Fecha válida en formato 'day-month-year'
    date_str = '21-11-2023'
    
    # Convertir la cadena a objeto datetime.date usando la función
    converted_date = convert_str_date_to_date(date_str)
    
    # Verificar si el tipo de resultado es datetime.date
    assert isinstance(converted_date, date)


    # Verificar si la fecha obtenida coincide con la fecha esperada
    expected_date = datetime.strptime(date_str, "%d-%m-%Y").date()
    assert converted_date == expected_date

def test_convert_str_date_to_date_invalid_date():
    # Fecha inválida en un formato diferente
    invalid_date_str = '2023-11-21'

    # Verificar si se lanza una excepción ValueError para una fecha inválida
    with pytest.raises(ValueError):
        convert_str_date_to_date(invalid_date_str)
