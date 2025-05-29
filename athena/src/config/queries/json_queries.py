"""JSON 관련 Athena 쿼리 템플릿"""

from typing import Dict, Any

class JSONQueries:
    """JSON 데이터 처리를 위한 쿼리 템플릿 모음"""
    
    @staticmethod
    def extract_valid_content(
        table: str, 
        column: str, 
        json_loc: str, 
        partition_conditions: Dict[str, str] = None
    ) -> str:
        """ 유효한 라벨 검출 쿼리 템플릿
        
        Args:
            table (str): 테이블 이름
            column (str): JSON 컬럼 이름
            json_loc (str): JSON 경로
            partition_conditions (Dict[str, str], optional): {파티션컬럼: 값} 형태의 파티션 조건
            
        Returns:
            str: JSON 데이터 추출 쿼리
        """
        conditions = [
            f"{column} IS NOT NULL",
            f"CARDINALITY(CAST(JSON_EXTRACT({column}, '{json_loc}') AS ARRAY(JSON))) > 0"
        ]
        if partition_conditions:
            partition_checks = [f"{k}='{v}'" for k, v in partition_conditions.items()]
            conditions.extend(partition_checks)

        return f"""
        SELECT DISTINCT image_path, {column}
        FROM {table}
        WHERE {' AND '.join(conditions)}
        """
 
    
    @staticmethod
    def extract_text_in_content(
        table: str,
        column: str,
        json_loc: str,
        search_text: str,
        partition_conditions: Dict[str, str] = None
    ) -> str:
        """ 특정 Text 데이터가 있는 라벨 추출 쿼리 템플릿
        
        Args:
            table (str): 테이블 이름
            column (str): JSON 컬럼 이름
            json_loc (str): JSON 경로
            partition_conditions (Dict[str, str], optional): {파티션컬럼: 값} 형태의 파티션 조건
            
        Returns:
            str: JSON 데이터 추출 쿼리
        """
        conditions = [
            f"{column} IS NOT NULL",
            f"CARDINALITY(CAST(JSON_EXTRACT({column}, '{json_loc}') AS ARRAY(JSON))) > 0"
        ]
        if partition_conditions:
            partition_checks = [f"{k}='{v}'" for k, v in partition_conditions.items()]
            conditions.extend(partition_checks)

        return f"""
        WITH extracted_content AS (
            SELECT 
                image_path,
                {column},
                element as content
            FROM {table}
            CROSS JOIN UNNEST(
                CAST(json_extract({column}, '{json_loc}') AS ARRAY<VARCHAR>)
            ) AS t(element)
            WHERE {' AND '.join(conditions)}
        )
        SELECT DISTINCT image_path, {column}
        FROM extracted_content
        WHERE content LIKE '%{search_text}%'
        """
        