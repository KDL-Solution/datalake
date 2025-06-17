# from src.core.athena_client import AthenaClient

# # Athena 클라이언트 초기화
# athena_client = AthenaClient(
#     database="my_catalog_db",
#     s3_output="s3://my-bucket/athena-results/",
#     # AWS 자격증명은 환경변수나 IAM 역할 사용
# )

# # 1) 모든 파티션 조합 조회
# partitions_df = athena_client.retrieve_partitions(table="catalog")
# print("사용 가능한 파티션:")
# print(partitions_df.head())

# # 2) 특정 조건으로 샘플 수 조회
# samples_df = athena_client.retrieve_num_samples(table="catalog")
# print("\n파티션별 샘플 수:")
# print(samples_df.head())

# # 3) 유효한 라벨 데이터 검색
# valid_labels = athena_client.search_valid_content(
#     table="catalog",
#     column="spotter_label",
#     variants=["word", "line"],
#     partition_conditions={
#         "provider": "huggingface", 
#         "dataset": "ocr_documents"
#     }
# )
# print(f"\n유효한 라벨이 있는 이미지: {len(valid_labels)}개")

# # 4) 특정 텍스트 검색
# text_results = athena_client.search_text_in_content(
#     table="catalog",
#     column="spotter_label", 
#     search_text="invoice",
#     variants="word",
#     partition_conditions={
#         "provider": "custom",
#         "dataset": "business_docs"
#     }
# )
# print(f"\n'invoice' 텍스트가 포함된 이미지: {len(text_results)}개")

# # 5) 존재하는 컬럼만으로 데이터 조회
# existing_data = athena_client.retrieve_with_existing_cols(
#     providers=["huggingface", "custom"],
#     datasets=["ocr_documents"],
#     variants=["word"]
# )
# print(f"\n조회된 데이터: {len(existing_data)}행")


from src.core.duckdb_client import DuckDBClient

# # DuckDB 클라이언트 초기화 (로컬 파일 기반)
# with DuckDBClient(database_path="./catalog.duckdb") as duckdb_client:
    
#     # S3 설정 (S3 데이터 사용 시)
#     duckdb_client.setup_s3()
    
#     # 로컬 Parquet 파일에서 테이블 생성
#     duckdb_client.create_table_from_parquet(
#         table_name="catalog",
#         parquet_path="/mnt/AI_NAS/datalake/catalog/**/*.parquet",
#         hive_partitioning=True
#     )
    
#     # 또는 S3 Parquet 파일에서 테이블 생성 (S3 설정 후)
#     # duckdb_client.create_table_from_parquet(
#     #     table_name="catalog",
#     #     parquet_path="s3://my-bucket/catalog/**/*.parquet",
#     #     hive_partitioning=True
#     # )
    
#     # 1) 테이블 정보 확인
#     table_info = duckdb_client.get_table_info("catalog")
#     print("테이블 구조:")
#     print(table_info)
    
#     # 2) 모든 파티션 조합 조회
#     partitions_df = duckdb_client.retrieve_partitions("catalog")
#     print("\n사용 가능한 파티션:")
#     print(partitions_df.head())
    
#     # 3) 파티션별 샘플 수 조회
#     samples_df = duckdb_client.retrieve_num_samples("catalog")
#     print("\n파티션별 샘플 수:")
#     print(samples_df.head())
    
#     # 4) 유효한 라벨 데이터 검색
#     valid_labels = duckdb_client.search_valid_content(
#         table="catalog",
#         column="spotter_label",
#         variants=["word", "line"],
#         partition_conditions={
#             "provider": "huggingface", 
#             "dataset": "ocr_documents"
#         }
#     )
#     print(f"\n유효한 라벨이 있는 이미지: {len(valid_labels)}개")
    
#     # 5) 특정 텍스트 검색
#     text_results = duckdb_client.search_text_in_content(
#         table="catalog",
#         column="spotter_label",
#         search_text="invoice", 
#         variants="word",
#         partition_conditions={
#             "provider": "custom",
#             "dataset": "business_docs"
#         }
#     )
#     print(f"\n'invoice' 텍스트가 포함된 이미지: {len(text_results)}개")
    
#     # 6) 커스텀 쿼리 실행
#     custom_result = duckdb_client.execute_query("""
#         SELECT provider, dataset, variant, COUNT(*) as count
#         FROM catalog 
#         WHERE spotter_label IS NOT NULL
#         GROUP BY provider, dataset, variant
#         HAVING count > 1000
#         ORDER BY count DESC
#     """)
#     print(f"\n1000개 이상 라벨이 있는 파티션: {len(custom_result)}개")


# =============================================================================
# 3. 인메모리 DuckDB 사용 (임시 분석용)
# =============================================================================

# 인메모리 데이터베이스로 빠른 분석
with DuckDBClient() as memory_db:  # database_path=None이면 인메모리
    
    # 특정 파일만 로드해서 분석
    memory_db.execute_query("""
        CREATE TABLE temp_analysis AS
        SELECT * FROM read_parquet('/mnt/AI_NAS/datalake/migrate_test/catalog/provider=*/dataset=*/task=*/variant=*/*.parquet', hive_partitioning=true, union_by_name=true)
        WHERE provider = 'huggingface'
    """)
    
    # 빠른 분석
    analysis_result = memory_db.execute_query("""
        SELECT variant, 
               COUNT(*) as total_samples,
               COUNT(DISTINCT dataset) as unique_datasets
        FROM temp_analysis
        GROUP BY variant
    """)
    print("임시 분석 결과:")
    print(analysis_result)
