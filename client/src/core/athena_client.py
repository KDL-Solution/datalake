import os
import sys
import time
from typing import Optional, Dict, Union, List
import re

import awswrangler as wr
import pandas as pd
import boto3

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.config.queries.json_queries import JSONQueries
from src.core import DEFAULT_DATABASE, DEFAULT_S3_OUTPUT


class AthenaClient:
    """Athena 쿼리 실행을 위한 클라이언트

    AWS Athena를 사용하여 데이터를 조회하고 처리하는 기능을 제공
    쿼리 템플릿을 사용하여 일관된 쿼리 실행을 보장

    Attributes:
        database (str): Athena 데이터베이스 이름
        s3_output (str): Athena 쿼리 결과가 저장될 S3 경로
        session (boto3.Session): AWS 세션 객체

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
        database: str = None,
        s3_output: str = None,
        **kwargs
    ):
        """AthenaClient 초기화

        Args:
            database (str): Athena 데이터베이스 이름
            s3_output (str): Athena 쿼리 결과가 저장될 S3 경로
            **kwargs: boto3.Session 생성에 사용될 파라미터
                AWS 자격 증명 정보를 파라미터로 기입할 때 사용,
                입력 안하면 기본값으로 자동 적용(환경변수 or aws configure에서 정의한 값)
                - region_name (str): AWS 리전
                - aws_access_key_id (str): AWS 액세스 키
                - aws_secret_access_key (str): AWS 시크릿 키
        """
        self.database = database if database else DEFAULT_DATABASE
        self.s3_output = s3_output if s3_output else DEFAULT_S3_OUTPUT
        self.session = boto3.Session(**kwargs) if kwargs else None
        
        # 쿼리 템플릿 초기화
        self.json_queries = JSONQueries()

    def execute_query(
        self,
        sql: str,
        chunksize: Optional[int] = None
    ) -> pd.DataFrame:
        """SQL 쿼리 실행
        Args:
            sql (str): 실행할 SQL 쿼리문
            chunksize (Optional[int]): 청크 크기 (None이면 전체 데이터)
            
        Returns:
            pd.DataFrame: 쿼리 결과
        """
        self.cleanup_previous_output()
        if chunksize:
            return self._execute_query_in_chunks(sql, chunksize)

        return wr.athena.read_sql_query(
            sql=sql,
            database=self.database,
            s3_output=self.s3_output,
            boto3_session=self.session
        )

    def _execute_query_in_chunks(
        self,
        sql: str,
        chunksize: int,
    ) -> pd.DataFrame:
        """청크 단위로 쿼리 실행
        Args:
            sql (str): 실행할 SQL 쿼리문
            chunksize (int): 청크 크기
            
        Returns:
            pd.DataFrame: 모든 청크를 합친 결과
        """
        dfs = []
        for chunk in wr.athena.read_sql_query(
            sql=sql,
            database=self.database,
            s3_output=self.s3_output,
            boto3_session=self.session,
            chunksize=chunksize
        ):
            dfs.append(chunk)
        return pd.concat(dfs, ignore_index=True)

    def _process_variants(
        self,
        variants: Union[str, List[str]],
        query_func: callable,
        **kwargs
    ) -> pd.DataFrame:
        """variants 처리 및 결과 병합
        Args:
            variants (Union[str, List[str]]): 단일 variant 또는 variant 리스트
            query_func (callable): 실행할 쿼리 함수
            **kwargs: query_func에 전달할 파라미터
        Returns:
            pd.DataFrame: 병합된 결과
        """
        if isinstance(variants, str):
            variants = [variants]
        
        dfs = []
        for variant in variants:
            df = query_func(variant=variant, **kwargs)
            dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def search_valid_content(
        self,
        table: str,
        column: str,
        variants: Union[str, List[str]],
        chunksize: Optional[int] = None,
        partition_conditions: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """유효한 라벨 데이터 검색
        JSON 컬럼에서 특정 variant의 text.content가 유효한 데이터를 검색합니다.
        유효한 데이터란 NULL이 아니고, 빈 배열이 아니며, 배열의 길이가 0보다 큰 경우를 의미합니다.
        Args:
            table (str): 검색할 테이블 이름
            column (str): JSON 데이터가 저장된 컬럼 이름
            variants (Union[str, List[str]]): 검색할 variant 타입 ('word', 'line', 'char' 등)
                단일 문자열 또는 문자열 리스트로 지정 가능.
            chunksize (Optional[int], optional): 청크 크기. None이면 전체 데이터. Defaults to None.
            partition_conditions (Optional[Dict[str, str]], optional): 파티션 조건.
                {파티션컬럼: 값} 형태로 지정. Defaults to None.
        Returns:
            pd.DataFrame: 검색된 데이터의 hash, path를 포함한 DataFrame
        Example:
            >>> client = AthenaClient(database="my_db", s3_output="s3://my-bucket/output")
            >>> df = client.search_valid_content(
            ...     table="catalog",
            ...     column="spotter_label",
            ...     variants=["word", "line"],
            ...     partition_conditions={"provider": "aihub", "dataset": "ocr_data"}
            ... )
        """
        def query_func(variant: str, **kwargs) -> pd.DataFrame:
            json_loc = f'$.{variant}.text.content'
            sql = self.json_queries.extract_valid_content(
                table=table,
                column=column,
                json_loc=json_loc,
                partition_conditions=partition_conditions
            )
            return self.execute_query(sql, chunksize=chunksize)

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
        chunksize: Optional[int] = None,
        partition_conditions: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """특정 텍스트가 포함된 라벨 데이터 검색
        JSON 컬럼에서 특정 variant의 text.content에 검색어가 포함된 데이터를 검색합니다.
        검색은 대소문자를 구분하며, 부분 문자열 매칭을 수행합니다.
        Args:
            table (str): 검색할 테이블 이름
            column (str): JSON 데이터가 저장된 컬럼 이름
            search_text (str): 검색할 텍스트
            variants (Union[str, List[str]]): 검색할 variant 타입 ('word', 'line', 'char' 등)
                단일 문자열 또는 문자열 리스트로 지정 가능
            chunksize (Optional[int], optional): 청크 크기. None이면 전체 데이터. Defaults to None.
            partition_conditions (Optional[Dict[str, str]], optional): 파티션 조건.
                {파티션컬럼: 값} 형태로 지정. Defaults to None.
        Returns:
            pd.DataFrame: 검색된 데이터의 hash,path를 포함한 DataFrame
        Example:
            >>> client = AthenaClient(database="my_db", s3_output="s3://my-bucket/output")
            >>> df = client.search_text_in_content(
            ...     table="catalog",
            ...     column="spotter_label",
            ...     search_text="tissue",
            ...     variants="word",
            ...     partition_conditions={"provider": "aihub", "dataset": "ocr_data"}
            ... )
        """
        def query_func(
            variant: str,
            **kwargs,
        ) -> pd.DataFrame:
            json_loc = f'$.{variant}.text.content'
            sql = self.json_queries.extract_text_in_content(
                table=table,
                column=column,
                json_loc=json_loc,
                search_text=search_text,
                partition_conditions=partition_conditions
            )
            return self.execute_query(sql, chunksize=chunksize)

        return self._process_variants(
            variants=variants,
            query_func=query_func
        )

    def retrieve_with_existing_cols(
        self,
        providers: List = [],
        tasks: List = [],
        variants: List = [],
        datasets: List = [],
        table: str = "catalog"
    ) -> pd.DataFrame:
        """존재하는 컬럼만 포함해서 조회.
        """
        
        conditions = []
        
        if providers:
            provider_condition = " OR ".join([f"provider = '{i}'" for i in providers])
            conditions.append(f"({provider_condition})")
        
        if datasets:
            dataset_condition = " OR ".join([f"dataset = '{i}'" for i in datasets])
            conditions.append(f"({dataset_condition})")
            
        if tasks:
            tasks_condition = " OR ".join([f"task = '{i}'" for i in tasks])
            conditions.append(f"({tasks_condition})")

        if variants:
            variant_condition = " OR ".join([f"variant = '{i}'" for i in variants])
            conditions.append(f"({variant_condition})")

        sql_for_cols = f"SELECT * FROM {table}"
        if conditions:
            sql_for_cols += f" WHERE {' AND '.join(conditions)}"
        sql_for_cols += " LIMIT 1"
        
        try:
            df_cols = self.execute_query(sql_for_cols)
            if df_cols.empty:
                return pd.DataFrame()
            
            cols = [k for k, v in df_cols.iloc[0].to_dict().items() if pd.notna(v)]

            sql = f"SELECT {', '.join(cols)} FROM catalog"
            if conditions:
                sql += f" WHERE {' AND '.join(conditions)}"
            return self.execute_query(sql)
        except Exception as e:
            print(f"컬럼 조회 실패: {str(e)}")
            return pd.DataFrame()

    def run_crawler(
        self,
        crawler_name: str,
        wait: bool = True,
        timeout: int = 3600
    ) -> Dict:
        """Glue Crawler 실행
        Args:
            crawler_name (str): 실행할 Crawler 이름
            wait (bool): Crawler 실행 완료를 기다릴지 여부
            timeout (int): 최대 대기 시간(초)
        Returns:
            Dict: Crawler 실행 결과
        Raises:
            Exception: Crawler 실행 실패 시
        """
        try:
            # Glue 클라이언트 생성
            glue_client = self.session.client('glue') if self.session else boto3.client('glue')

            # Crawler 실행
            response = glue_client.start_crawler(Name=crawler_name)

            if wait:
                # Crawler 상태 확인
                start_time = time.time()
                while True:
                    crawler = glue_client.get_crawler(Name=crawler_name)
                    state = crawler['Crawler']['State']

                    if state == 'READY':
                        break

                    if time.time() - start_time > timeout:
                        raise TimeoutError(f"Crawler {crawler_name} 실행 시간 초과")

                    time.sleep(10)

                last_run = crawler['Crawler'].get('LastCrawl')
                if last_run and last_run.get('Status') == 'FAILED':
                    raise Exception(f"Crawler {crawler_name} 실행 실패: {last_run.get('ErrorMessage')}")

            return response

        except Exception as e:
            raise Exception(f"Crawler {crawler_name} 실행 중 오류 발생: {str(e)}")

    def cleanup_previous_output(
        self,
    ):
        """이전 쿼리 실행으로 생성된 임시 데이터 정리"""
        try:
            s3_client = self.session.client('s3') if self.session else boto3.client('s3')
            bucket = self.s3_output.split('/')[2]
            prefix = '/'.join(self.s3_output.split('/')[3:])
            
            # 해당 경로의 모든 객체 삭제
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': [
                        {'Key': obj['Key']} 
                        for obj in s3_client.list_objects_v2(
                            Bucket=bucket,
                            Prefix=prefix
                        ).get('Contents', [])
                    ]
                }
            )
        except Exception as e:
            print(f"데이터 정리 중 오류 발생: {str(e)}")
