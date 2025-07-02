"""JSON 관련 Athena 쿼리 템플릿"""

from typing import Dict, Any

class SQLQueries:

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
            json_loc (str): JSON 경로 (예: '$.word.text.content')
            partition_conditions (Dict[str, str], optional): {파티션컬럼: 값} 형태의 파티션 조건
                예: {'provider': 'huggingface', 'dataset': 'ocr_data', 'task':ocr', 'variant': 'word'}
            
        Returns:
            str: JSON 데이터 추출 쿼리
        """
        conditions = [
            f"{column} IS NOT NULL",
            f"json_extract({column}, '{json_loc}') IS NOT NULL",
            f"cardinality(cast(json_extract({column}, '{json_loc}') as json[])) > 0"
        ]
        
        if partition_conditions:
            partition_checks = [f"{k}='{v}'" for k, v in partition_conditions.items()]
            conditions.extend(partition_checks)

        return f"""
        SELECT DISTINCT hash, path, {column}
        FROM {table}
        WHERE {' AND '.join(conditions)}
        """
    
    @staticmethod
    def search_text_in_column(
        table: str,
        column: str,
        search_text: str,
        search_type: str = "simple",  # 🆕 "simple" 또는 "json"
        json_loc: str = None,  # JSON 검색시에만 필요
        partition_conditions: Dict[str, str] = None,
        engine: str = "athena"
    ) -> str:
        """ 컬럼에서 텍스트 검색 쿼리 템플릿
        
        Args:
            table (str): 테이블 이름
            column (str): 검색할 컬럼 이름
            search_text (str): 검색할 텍스트
            search_type (str): 검색 방법 ("simple" 또는 "json")
            json_loc (str): JSON 경로 (search_type="json"일 때만 사용)
            partition_conditions (Dict[str, str], optional): 파티션 조건
            engine (str): 쿼리 엔진
            
        Returns:
            str: 텍스트 검색 쿼리
        """
        
        base_conditions = [f"{column} IS NOT NULL"]
        
        if partition_conditions:
            partition_checks = [f"{k}='{v}'" for k, v in partition_conditions.items()]
            base_conditions.extend(partition_checks)
        
        if search_type == "simple":
            # 단순 LIKE 검색
            conditions = base_conditions + [f"{column} LIKE '%{search_text}%'"]

            return f"""
            SELECT DISTINCT hash, path, {column}
            FROM {table}
            WHERE {' AND '.join(conditions)}
            """

        elif search_type == "json":
            # JSON 파싱 검색
            if not json_loc:
                raise ValueError("JSON 검색시 json_loc 파라미터가 필요합니다")
                
            if engine.lower() == "duckdb":
                # DuckDB 문법
                json_conditions = [
                    f"json_extract({column}, '{json_loc}') IS NOT NULL",
                    f"json_array_length(json_extract({column}, '{json_loc}')) > 0"
                ]
                unnest_part = f"unnest(cast(json_extract({column}, '{json_loc}') as varchar[])) as content"
                
            else:
                # Athena 문법
                json_conditions = [
                    f"json_extract({column}, '{json_loc}') IS NOT NULL",
                    f"cardinality(cast(json_extract({column}, '{json_loc}') as json[])) > 0"
                ]
                unnest_part = f"unnest(cast(json_extract({column}, '{json_loc}') as varchar[])) AS t(content)"
            
            all_conditions = base_conditions + json_conditions
            
            return f"""
            WITH extracted_content AS (
                SELECT 
                    hash,
                    path,
                    {column},
                    {unnest_part}
                FROM {table}
                WHERE {' AND '.join(all_conditions)}
            )
            SELECT DISTINCT hash, path, {column}
            FROM extracted_content
            WHERE content LIKE '%{search_text}%'
            """

    @staticmethod
    def get_distinct_partitions(table: str) -> str:
        """모든 파티션 조합 조회"""
        return f"""
        SELECT DISTINCT provider, dataset, task, variant, COUNT(*) AS num_samples
        FROM {table}
        GROUP BY provider, dataset, task, variant
        ORDER BY provider, dataset, task, variant
        """

    @staticmethod
    def create_table_from_parquet_duckdb(
        table_name: str,
        parquet_path: str
    ) -> str:
        """DuckDB용 Parquet 테이블 생성 (Hive 파티션 지원)"""
        return f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_parquet('{parquet_path}/**/*.parquet', hive_partitioning=true)
        """
        
    @staticmethod
    def get_providers_query(table: str) -> str:
        """모든 Provider 목록 조회 쿼리"""
        return f"SELECT DISTINCT provider FROM {table} ORDER BY provider"