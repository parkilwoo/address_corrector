import sys
sys.path.append('/workspace/addr_correct')
from addr_corrector import AddressCorrector
import pytest


@pytest.fixture
def pre_set():
    return AddressCorrector(url="http://localhost:9200")

def test_신주소_주소보정_테스트(pre_set):
    connector:AddressCorrector = pre_set
    
    wrong_address = "서울시 강담구 두산대로8길 20-7"
    answer_address = "서울특별시 강남구 도산대로8길 20-7"
    result_list = connector.correct(wrong_address)

    result = result_list[0]
    correct_address = result[1]
    assert answer_address == correct_address

def test_구주소_주소보정_테스트(pre_set):
    connector:AddressCorrector = pre_set
    
    wrong_address = "광주시 곤지읍 건엄리 236-1"
    answer_address = "경기도 광주시 곤지암읍 건업리 236-1"
    result_list = connector.correct(wrong_address)

    result = result_list[0]
    correct_address = result[2]
    assert answer_address == correct_address    

def test_건물명_주소보정_테스트(pre_set):
    connector:AddressCorrector = pre_set
    
    wrong_address = "강남세브란스병원"    
    answer_address = "서울특별시 강남구 언주로 211 강남세브란스병원"

    result_list = connector.correct(wrong_address)

    result = result_list[0]
    correct_address = result[1]
    assert answer_address == correct_address        