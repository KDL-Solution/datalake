import os
import sys
from typing import Optional, Dict, Union, List
import pandas as pd
import duckdb
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.config.queries.json_queries import JSONQueries


class DuckDBClient:
    """DuckDB 쿼리 실행을 위한 클라이언트

    DuckDB를 사용하여 로컬 파일이나 S3 데이터를 조회하고 처리하는 기능을 제공
    쿼리 템플릿을 사용하여 일관된 쿼리 실행을 보장

    Attributes:
        database_path (str): DuckDB 데이터베이스 파일 경로 (None이면 인메모리)
        connection (duckdb.DuckDBPyConnection): DuckDB 연결 객체

    Partition Info Example
        catalog --- provider=A --- dataset=B --- task=D --- variant=X --- abc.parquet  
                |__ ...        |__ ...        
                |__ ...        |__ ...
                |__ ...        |__ dataset=C --- ...
                |__ ...
                |__ provider=K --- ... 
        Partitions = [provider, dataset, task, variant]
    """

    def __init__(
        self,
        database_path: Optional[str] = None,
        read_only: bool = True,
    ):
        """DuckDBClient 초기화

        Args:
            database_path (Optional[str]): DuckDB 데이터베이스 파일 경로. None이면 인메모리 DB 사용
        """
        self.database_path = database_path
        self.connection = None
        # 쿼리 템플릿 초기화
        self.json_queries = JSONQueries()
        
        # 연결 초기화
        self.connect(read_only=read_only)

    def connect(self, read_only: bool = False) -> None:
        """DuckDB 연결 생성 및 초기 설정"""
        try:
            if self.database_path:
                # 디렉토리 생성
                Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
                self.connection = duckdb.connect(self.database_path, read_only=read_only)
            else:
                # 인메모리 데이터베이스
                self.connection = duckdb.connect(':memory:', read_only=read_only)
            # 기본 확장 설치
            self._install_extensions()
            
        except Exception as e:
            raise Exception(f"DuckDB 연결 실패: {str(e)}")

    def _install_extensions(self) -> None:
        """필요한 DuckDB 확장 설치"""
        try:
            # JSON 확장
            self.connection.execute("INSTALL json")
            self.connection.execute("LOAD json")
            
            # HTTPfs 확장 (S3 지원)
            self.connection.execute("INSTALL httpfs")
            self.connection.execute("LOAD httpfs")
            
        except Exception as e:
            print(f"확장 설치 중 오류: {str(e)}")

    def execute_query(self, sql: str) -> pd.DataFrame:
        """SQL 쿼리 실행
        
        Args:
            sql (str): 실행할 SQL 쿼리문
        Returns:
            pd.DataFrame: 쿼리 결과
        """
        try:
            return self.connection.execute(sql).df()
        except Exception as e:
            raise Exception(f"쿼리 실행 실패: {str(e)}\nSQL: {sql}")

    def create_table_from_parquet(
        self, 
        table_name: str, 
        parquet_path: str,
        hive_partitioning: bool = True,
        union_by_name: bool = True
    ) -> None:
        """Parquet 파일에서 테이블 생성
        
        Args:
            table_name (str): 생성할 테이블 이름
            parquet_path (str): Parquet 파일 경로 (와일드카드 지원)
            hive_partitioning (bool): Hive 파티션 사용 여부
        """
        try:
            if hive_partitioning:
                sql = f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true, union_by_name={str(union_by_name).lower()})
                """
            else:
                sql = self.json_queries.create_table_from_parquet_duckdb(table_name, parquet_path)
            self.connection.execute(sql)
            print(f"✅ 테이블 '{table_name}' 생성 완료")
            
        except Exception as e:
            raise Exception(f"테이블 생성 실패: {str(e)}")

    def _process_variants(
        self,
        variants: Union[str, List[str]],
        query_func: callable,
        **kwargs
    ) -> pd.DataFrame:
        """variants 처리 및 결과 병합"""
        if isinstance(variants, str):
            variants = [variants]
        
        dfs = []
        for variant in variants:
            df = query_func(variant=variant, **kwargs)
            if not df.empty:
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def search_valid_content(
        self,
        table: str,
        column: str,
        variants: Union[str, List[str]],
        partition_conditions: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """유효한 라벨 데이터 검색
        
        Args:
            table (str): 검색할 테이블 이름
            column (str): JSON 데이터가 저장된 컬럼 이름
            variants (Union[str, List[str]]): 검색할 variant 타입
            partition_conditions (Optional[Dict[str, str]]): 파티션 조건
                
        Returns:
            pd.DataFrame: 검색된 데이터
        """
        def query_func(variant: str, **kwargs) -> pd.DataFrame:
            json_loc = f'$.{variant}.text.content'
            sql = self.json_queries.extract_valid_content(
                table=table,
                column=column,
                json_loc=json_loc,
                partition_conditions=partition_conditions
            )
            return self.execute_query(sql)

        return self._process_variants(
            variants=variants,
            query_func=query_func
        )

    def search_text_in_content(
        self,
        table: str,
        column: str,
        search_text: str,
        variants: Union[str, List[str]],
        partition_conditions: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """특정 텍스트가 포함된 라벨 데이터 검색
        
        Args:
            table (str): 검색할 테이블 이름
            column (str): JSON 데이터가 저장된 컬럼 이름
            search_text (str): 검색할 텍스트
            variants (Union[str, List[str]]): 검색할 variant 타입
            partition_conditions (Optional[Dict[str, str]]): 파티션 조건
                
        Returns:
            pd.DataFrame: 검색된 데이터
        """
        def query_func(variant: str, **kwargs) -> pd.DataFrame:
            json_loc = f'$.{variant}.text.content'
            sql = self.json_queries.extract_text_in_content(
                table=table,
                column=column,
                json_loc=json_loc,
                search_text=search_text,
                partition_conditions=partition_conditions,
                engine="duckdb"  # DuckDB 엔진 사용
            )
            return self.execute_query(sql)

        return self._process_variants(
            variants=variants,
            query_func=query_func
        )

    def retrieve_num_samples(self, table: str = "catalog") -> pd.DataFrame:
        """데이터셋별로 샘플 수를 조회"""
        sql = self.json_queries.count_samples_by_partition(table)
        return self.execute_query(sql)

    def retrieve_partitions(self, table: str = "catalog") -> pd.DataFrame:
        """모든 파티션 조합 조회"""
        sql = self.json_queries.get_distinct_partitions(table)
        return self.execute_query(sql)

    def retrieve_with_existing_cols(
        self,
        providers: List = [],
        datasets: List = [],
        tasks: List = [],
        variants: List = [],
        table: str = "catalog"
    ) -> pd.DataFrame:
        """존재하는 컬럼만 포함해서 조회"""
        # str 일경우 리스트로 변경
        if isinstance(providers, str):
            providers = [providers]
        if isinstance(datasets, str):
            datasets = [datasets]
        if isinstance(tasks, str):
            tasks = [tasks]
        if isinstance(variants, str):
            variants = [variants]
        # WHERE 조건 구성
        conditions = []
        
        if providers:
            provider_condition = " OR ".join([f"provider = '{i}'" for i in providers])
            conditions.append(f"({provider_condition})")

        if datasets:
            dataset_condition = " OR ".join([f"dataset = '{i}'" for i in datasets])
            conditions.append(f"({dataset_condition})")

        if tasks:
            task_condition = " OR ".join([f"task = '{i}'" for i in tasks])
            conditions.append(f"({task_condition})")
            
        if variants:
            variant_condition = " OR ".join([f"variant = '{i}'" for i in variants])
            conditions.append(f"({variant_condition})")

        # 첫 번째 행으로 존재하는 컬럼 확인
        sql_for_cols = f"SELECT * FROM {table}"
        if conditions:
            sql_for_cols += f" WHERE {' AND '.join(conditions)}"
        sql_for_cols += " LIMIT 1"
        
        try:
            df_cols = self.execute_query(sql_for_cols)
            if df_cols.empty:
                return pd.DataFrame()
                
            # NULL이 아닌 컬럼만 선택
            cols = [k for k, v in df_cols.iloc[0].to_dict().items() if pd.notna(v)]
            
            # 실제 데이터 조회
            sql = f"SELECT {', '.join(cols)} FROM {table}"
            if conditions:
                sql += f" WHERE {' AND '.join(conditions)}"
                
            return self.execute_query(sql)
            
        except Exception as e:
            print(f"컬럼 조회 실패: {str(e)}")
            return pd.DataFrame()

    def close(self) -> None:
        """연결 종료"""
        if self.connection:
            self.connection.close()
            print("✅ DuckDB 연결 종료")

    def __enter__(self):
        """컨텍스트 매니저 진입"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()

    def get_table_info(self, table: str) -> pd.DataFrame:
        """테이블 정보 조회"""
        try:
            return self.execute_query(f"DESCRIBE {table}")
        except Exception as e:
            print(f"테이블 정보 조회 실패: {str(e)}")
            return pd.DataFrame()

    def list_tables(self) -> pd.DataFrame:
        """데이터베이스의 모든 테이블 목록 조회"""
        try:
            return self.execute_query("SHOW TABLES")
        except Exception as e:
            print(f"테이블 목록 조회 실패: {str(e)}")
            return pd.DataFrame()