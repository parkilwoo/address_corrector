from elasticsearch import Elasticsearch
from typing import List
import re
from soynlp.hangle import jamo_levenshtein
from enum import Enum
import itertools

class ADDRESS_CLASSIFICATION(Enum):
    DORO = [("DEFAULT_ADDR", 1), ("DORO", 3), ("BUILD_NO1", 4), ("BUILD_NO2", 5)]
    DONG = [("DEFAULT_ADDR", 1), ("DONG_NM", 3), ("ZIBUN1", 4), ("ZIBUN2", 5)]
    RI = [("DEFAULT_ADDR", 1), ("RI", 3), ("ZIBUN1", 4), ("ZIBUN2", 4)]
    NONE = [("FULL_ADDR", 1), ]

class AddressCorrector:
    _SIMILER_BUNZI_UNIT = "호|로|길|김|가|동|리|히"
    _APT_DONG_UNIT = "가나다라마바사아자차파카타하"
    _FIRST_ADDR_REG = re.compile(r"(\d*?[가-힣0-9a-zA-Z\s]{1,}("+_SIMILER_BUNZI_UNIT+r")(\s+)?(산)?\d+(-+\d+|\s+\d+)?\b)")
    _SECOND_ADDR_REG = re.compile(r"(\d*?[가-힣0-9a-zA-Z\s]{1,}("+_SIMILER_BUNZI_UNIT+r"))")
    _THIRD_ADDR_REG = re.compile(r"([가-힣0-9a-zA-Z]{1,}\s{1,}[가-힣0-9a-zA-Z]("+_SIMILER_BUNZI_UNIT+r"))")
    _TWO_ZIBUN_REG = re.compile(r"\d+-\d+")
    _ONLY_NUMBER_REG = re.compile(r"\b\d+(?![가-힣a-zA-Z])\b")
    _DETAIL_ADDR_REG = re.compile(r"\b((\d+|["+_APT_DONG_UNIT+r"]|[a-zA-Z])동|\d+호|\d+층)(?![가-힣a-zA-Z])(.*|\b)")
    _SIGUNGU_ADDR_REG = re.compile(r"([가-힣0-9a-zA-Z]+)(시|군|구)")
    _SAN_PATTERN = r'(산)\d+'
    _DORO_ADDR = ('SIDO', 'SIGUNGU', 'EUPMYUN', 'DORO', 'BUILD_NO1', 'BUILD_NO2')
    _ZIBUN_ADDR = ('SIDO', 'SIGUNGU', 'EUPMYUN', 'DONG_NM', 'RI', 'ZIBUN1', 'ZIBUN2')
    _LAST_UNIT_VALUE = ('로', '길', '가', '동', '리')    

    def __init__(self, url="http://elasticsearch:9200", timeout=30, max_retries=10, retry_on_timeout=True, min_score = 20) -> None:
        self.client = Elasticsearch(url, timeout=timeout, max_retries=max_retries, retry_on_timeout=retry_on_timeout)
        self.filter_query_list = []
        self.filter_dict = {}
        self.should_query_list = []
        self.min_score = min_score

    def _correct_si_do(self, full_addr: str) -> str:
        addr_array = full_addr.split(" ")
        si_do_addr = addr_array[0]
        if '경기' in si_do_addr: return '경기도'
        elif '서울' in si_do_addr: return '서울특별시'
        elif '부산' in si_do_addr: return '부산광역시'
        elif '울산' in si_do_addr: return '울산광역시'
        elif '대전' in si_do_addr: return '대전광역시'
        elif '대구' in si_do_addr: return '대구광역시'
        elif '인천' in si_do_addr: return '인천광역시'
        elif '세종' in si_do_addr: return '세종특별자치시'
        elif '경북' in si_do_addr or '경상북도' in si_do_addr: return '경상북도'
        elif '강원' in si_do_addr: return '강원도'
        elif '경남' in si_do_addr or '경상남도' in si_do_addr: return '경상남도'
        elif '전남' in si_do_addr or '전라남도' in si_do_addr: return '전라남도'
        elif '전북' in si_do_addr or '전라북도' in si_do_addr: return '전라북도'
        elif '충남' in si_do_addr or '충청남도' in si_do_addr: return '충청남도'
        elif '충북' in si_do_addr or '충청북도' in si_do_addr: return '충청북도'
        elif '제주' in si_do_addr: return '제주특별자치도'
        elif '광주' in si_do_addr:
            if len(addr_array) >= 2 \
                and addr_array[1] in ['광산구','북구','서구','동구','북구']: return '광주광역시'
        return None
    
    def _update_filter_dict(self, key: str, value: any):
        self.filter_dict.update({
            key: value
        })

    def _separate_bunzi(self, reg_search_addr: str) -> str:
        second_search = self._SECOND_ADDR_REG.search(reg_search_addr)
        if not second_search:
            return reg_search_addr
        second_addr = second_search.group(0).strip()
        check_value = reg_search_addr.split(second_addr)[1].strip()
        if check_value:
            return " ".join([second_addr,check_value])
        return second_addr
    
    def _attach_hangjung_addr(self, second_reg_search_addr: str) -> str:
        third_search = self._THIRD_ADDR_REG.search(second_reg_search_addr)
        if not third_search:
            return second_reg_search_addr
        third_addr = third_search.group(0).strip()

        third_addr_arr = third_addr.split(" ")
        if not third_addr_arr[-1] in ('로', '길', '가', '동', '리', '대로'):
            return second_reg_search_addr
        attach_addr = re.sub(r"\s", "", third_addr)
        return second_reg_search_addr.replace(third_addr, attach_addr)    
    
    def _gil_address_correct(self, search_addr_array: List[str]) -> List[str]:
        """도로명 주소가 '길'로 끝났을때 보정작업(EX. 방배천로 2길 -> 방배천로2길)

        Args:
            search_addr_array (List[str]): 입력받은 주소를 띄어쓰기로 분리한 배열

        Returns:
            List[str]: 보정된 주소 배열
        """
        if len(search_addr_array) < 3: 
            return search_addr_array
        check_value = search_addr_array[-3]
        if jamo_levenshtein(check_value[-1], '로') >= 0.6:
            return search_addr_array
        
        correct_addr = search_addr_array.pop(-3) + search_addr_array.pop(-2)
        search_addr_array.insert(-1, correct_addr)

        return search_addr_array

    def _separate_address(self, input_addr: str, detail_address:str):
        """_summary_
            입력받은 주소값을 행정부주소+상세주소로 구분하는 함수
        Args:
            input_addr (str): OCR 검출 결과에서 가져온 풀 주소 값

        Returns:
            tuple[str, str]: (행정부주소, 상세주소)
        """
        search_result = self._FIRST_ADDR_REG.search(input_addr)
        # detail_address = ''
        is_san = False
        if not search_result:
            return [], detail_address, is_san
        
        search_addr = search_result.group(0).strip()
        # 지번주소 붙어있는 경우 처리
        zibun_separate_addr = self._separate_bunzi(search_addr)
        # 행정주소 떨어져 있는경우 붙이는 작업
        hangung_attach_addr = self._attach_hangjung_addr(zibun_separate_addr)
        search_addr_array = hangung_attach_addr.split(" ") 

        if search_addr_array[-2].isdigit() and search_addr_array[-1].isdigit():
            zibun_1 = search_addr_array.pop(-2)
            zibun_2 = search_addr_array.pop(-1)
            search_addr_array.append("-".join([zibun_1, zibun_2]))
        
        # 1. zibun, doro명 주소 분리
        if(search_addr_array[-2][-1] == '길'):
            # 길로 끝났을경우 앞의 주소가 '로'로 끝나는 경우는 합쳐야함(방배천로 2길-> 방배천로2길로 보정하려는 작업)
            search_addr_array = self._gil_address_correct(search_addr_array)

        zibun_doro_addr = search_addr_array[-2:]
        sigungu_separate_addr_search = self._SIGUNGU_ADDR_REG.search(zibun_doro_addr[0])
        if sigungu_separate_addr_search:
            sigungu = sigungu_separate_addr_search.group(0).strip()
            search_addr_array.insert(-2, sigungu)
            separte_value = zibun_doro_addr[0].replace(sigungu, "")
            zibun_doro_addr[0] = separte_value
            search_addr_array[-2] = separte_value        


        bungi_value:str = zibun_doro_addr.pop(-1)
        bungi_value_array = bungi_value.split("-")
        
        for v in bungi_value_array:
            if re.search(self._SAN_PATTERN, v):
                is_san = True
                v = v.replace('산','')
            zibun_doro_addr.append(v.replace(" ",""))
        
        default_address = " ".join(search_addr_array[:-2])
        if not detail_address:
            detail_address = input_addr.split(search_addr)[1]
        
        zibun_doro_addr.insert(0, default_address.strip())
        return zibun_doro_addr, detail_address, is_san
    
    def _genreate_elastic_filter_query(self, field: str, value: str) -> dict:
        filter_dict = {
            "term": {
                field: value
            }
        }

        return filter_dict    

    def _generate_filter_query_list(self) -> List:
        for k, v in self.filter_dict.items():
            self.filter_query_list.append(self._genreate_elastic_filter_query(k, v))    

    def _check_minimum_edit_distance(self, check_value: str) -> str:
        min_value = 999
        result_val = ''    
        for unit in self._LAST_UNIT_VALUE:
            distance = jamo_levenshtein(unit, check_value)
            if distance == 0:
                return unit
            if distance != 1 and min_value > distance:
                min_value = distance
                result_val = unit

        return result_val

    def _check_addr_format(self, unit_address: str) -> ADDRESS_CLASSIFICATION:
        """_summary_
            도로명주소인지 지번주소인지 확인하는 함수
        Args:
            check_val (str): 행정부주소를 공백으로 split한 배열의 마지막 값

        Returns:
            bool: True(도로명주소) / False(지번주소)
        """
        if not unit_address:
            return ADDRESS_CLASSIFICATION.NONE
        
        check_value = self._check_minimum_edit_distance(unit_address[-1])
        if check_value in ('로', '길'):
            return ADDRESS_CLASSIFICATION.DORO
        if check_value in ('동', '가'):
            return ADDRESS_CLASSIFICATION.DONG
        if check_value == '리':
            return ADDRESS_CLASSIFICATION.RI
        
        return ADDRESS_CLASSIFICATION.NONE           

    def _generate_elastic_should_query(self, target: str, query: str, boost: int = 1) -> dict:
        """엘라스틱서치에서 사용할 should query 만드는 함수

        Args:
            target (str): 검색에 사용할 field
            query (str): 검색에 사용할 query
            boost (int, optional): boost값 Defaults to 1.

        Returns:
            dict: should query dict
        """
        should_dict = {
            "match": {
                target: {
                    "query": query,
                    "fuzziness": "AUTO",
                    "boost": boost
                }
            }
        }

        return should_dict
    
    def _generate_full_addr_should_query_list(self, full_addr: str):
        """_summary_
            번지부 주소를 나눌 수 없을때 풀주소 + 건물명으로 검색 쿼리를 만드는 함수
        Args:
            full_addr (str): _description_
            correct_sido_value (str): _description_

        Returns:
            List: _description_
        """


        if not len(full_addr):
            return None
        
        two_zibun_search = self._TWO_ZIBUN_REG.search(full_addr)
        if two_zibun_search:
            two_zibun = two_zibun_search.group(0)

            full_addr = full_addr.replace(two_zibun, "")
            self.should_query_list.append(self._generate_elastic_should_query('BUILD_NO', two_zibun, 2))
            self.should_query_list.append(self._generate_elastic_should_query('ZIBUN', two_zibun, 2))
        else:
            number_serach = self._ONLY_NUMBER_REG.findall(full_addr)
            number_search_len = len(number_serach)
            if number_search_len >= 2:
                two_zibun = "-".join([number_serach[0], number_serach[1]])

                self.should_query_list.append(self._generate_elastic_should_query('BUILD_NO', two_zibun, 2))
                self.should_query_list.append(self._generate_elastic_should_query('ZIBUN', two_zibun, 2))
                full_addr = full_addr.replace(number_serach[0], "")
                full_addr = full_addr.replace(number_serach[1], "")            
            elif number_search_len > 0 and  number_search_len < 2:
                two_zibun = number_serach[0]
                self.should_query_list.append(self._generate_elastic_should_query('BUILD_NO', two_zibun, 2))
                self.should_query_list.append(self._generate_elastic_should_query('ZIBUN', two_zibun, 2))
                full_addr = full_addr.replace(number_serach[0], "")
        self.should_query_list.append(self._generate_elastic_should_query('FULL_ADDR', " ".join(full_addr)))


    def _generate_should_query_list(self, enum: ADDRESS_CLASSIFICATION, address_array: List[str]) -> List:
        for e_value, addr in itertools.zip_longest(enum.value, address_array, fillvalue='0'):
            if not addr: continue
            self.should_query_list.append(self._generate_elastic_should_query(e_value[0], addr, e_value[1]))

    def _search_elastic_search(self) -> List:
        query = {"bool": {}}
        if self.filter_query_list:
            query.get('bool').update({"filter": self.filter_query_list})
        
        if self.should_query_list:
            query.get('bool').update({"should": self.should_query_list})  

        result = self.client.search(index="addr", query=query, size=3, min_score=self.min_score)
        
        hit_value = result["hits"]["hits"]
        return hit_value
    
    def _generate_search_address(self, target_tuple: tuple, elastic_result_source: dict):
        string_array = []
        int_array = []

        for target in target_tuple:
            value: str = elastic_result_source.get(target)
            if not value or value == '0': continue
            if value.isdigit():
                int_array.append(value)
                continue
            string_array.append(value)

        int_to_string = "-".join(int_array)
        if 'DONG_NM' in target_tuple and elastic_result_source.get('SAN_YN') != '0':
            int_to_string = '산' + int_to_string
        string_array.append(int_to_string)

        return " ".join(string_array)    

    def _generate_result_list(self, hit_value: List, detail_addr: str):
        result_list = []          

        for value in hit_value[:3]:
            zip_num = value.get('_source').get('ZIP_NO')
            doro = self._generate_search_address(self._DORO_ADDR, value.get('_source'))
            zibun = self._generate_search_address(self._ZIBUN_ADDR, value.get('_source'))
            build_nm = value.get('_source').get('BUILD_NM')
            result_list.append((zip_num, " ".join([doro, build_nm]).strip(), " ".join([zibun, build_nm]).strip(), detail_addr))        

        return result_list    

    def correct(self, address: str):
        address_strip: str = address.strip()
        if not address_strip:
            raise ValueError("Empty input address")
        
        correct_si_do_value = self._correct_si_do(address_strip)

        if correct_si_do_value:
            self._update_filter_dict('SIDO', correct_si_do_value)
            addr_array = address_strip.split(" ")
            addr_array[0] = correct_si_do_value
            address_strip = " ".join(addr_array)
        
        detail_search = self._DETAIL_ADDR_REG.search(address_strip)
        detail_address = ""
        if detail_search:
            detail_address = detail_search.group(0)
            address_strip = address_strip.replace(detail_address, "").strip()     

        separation_address, detail_address, is_san = self._separate_address(address_strip, detail_address)
        if is_san:
            self._update_filter_dict("SAN_YN", "1")
        self._generate_filter_query_list()

        enum_check_value = separation_address[1] if len(separation_address) else ""
        enum: ADDRESS_CLASSIFICATION = self._check_addr_format(enum_check_value)

        if enum == ADDRESS_CLASSIFICATION.NONE:
            # 건물번호 or 번지수가 없을경우 풀주소 + 건물명으로 검색
            self._generate_full_addr_should_query_list(address_strip)
        else:
            self._generate_should_query_list(enum, separation_address)

        hit_value = self._search_elastic_search()
        return self._generate_result_list(hit_value, detail_address.strip())