import concurrent.futures
import asyncio
import logging
import uvicorn
import os
from typing import Dict, List, Optional
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from datalake.server.app import DatalakeProcessor
from datalake.utils import setup_logging


class ValidateAssetsRequest(BaseModel):
    """DataFrame 기반 Assets 유효성 검사 요청"""
    user_id: str
    search_data: List[Dict] 
    sample_percent: Optional[float] = None


class StatusResponse(BaseModel):
    """상태 응답 모델"""
    pending: int
    processing: int
    failed: int
    server_status: str
    last_updated: str


class ProcessingJob(BaseModel):
    """처리 작업 상태"""
    job_id: str
    status: str  # "running", "completed", "failed"
    started_at: str
    completed_at: str = None
    result: Dict = None
    error: str = None


processor = None
logger = None
current_jobs: Dict[str, ProcessingJob] = {}
job_lock = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 실행"""
    global processor, logger
    
    BASE_PATH = os.environ["BASE_PATH"]
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))
    NUM_PROC = int(os.environ.get("NUM_PROC", 4))
    CREATE_DIRS = os.environ.get("CREATE_DIRS", "false").lower() == "true"
    
    try:
        processor = DatalakeProcessor(
            base_path=BASE_PATH,
            log_level=LOG_LEVEL,
            num_proc=NUM_PROC,
            batch_size=BATCH_SIZE,
            create_dirs=CREATE_DIRS
        )
        setup_logging(
            user_id="server",
            log_level=LOG_LEVEL, 
            base_path=BASE_PATH
        )
        logger = logging.getLogger(__name__)
        logger.info("✅ DatalakeProcessor 초기화 완료")
    except Exception as e:
        logger.error(f"❌ Processor 초기화 실패: {e}")
        raise
    
    yield
    
    # 종료 시 정리
    logger.info("🔄 서버 종료 중...")


# FastAPI 앱 생성
app = FastAPI(
    title="Datalake Processing API",
    description="Datalake 데이터 처리 및 카탈로그 생성을 담당하는 API 서버",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/info")
async def get_server_info():
    """서버 설정 정보 조회 - 모든 경로 정보 포함"""
    try:
        if not processor:
            raise HTTPException(status_code=503, detail="Processor not initialized")
        
        return {
            "server_status": "running",
            "version": "1.0.0",
            "num_proc": processor.num_proc,
            "batch_size": processor.batch_size,
            "timestamp": datetime.now().isoformat(),
            
            # 모든 경로 정보
            "paths": {
                "base_path": str(processor.base_path),
                "staging_path": str(processor.staging_path),
                "staging_pending_path": str(processor.staging_pending_path),
                "staging_processing_path": str(processor.staging_processing_path),
                "staging_failed_path": str(processor.staging_failed_path),
                "catalog_path": str(processor.catalog_path),
                "assets_path": str(processor.assets_path),
                "collections_path": str(processor.collections_path),
            }
        }
    except Exception as e:
        logger.error(f"서버 정보 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """현재 상태 조회"""
    try:
        if not processor:
            raise HTTPException(status_code=503, detail="Processor not initialized")
        
        status = processor.get_status()
        
        return StatusResponse(
            pending=status["pending"],
            processing=status["processing"],
            failed=status["failed"],
            server_status="running",
            last_updated=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process", response_model=Dict)
async def process_pending_data(
    background_tasks: BackgroundTasks,
):
    """Pending 데이터 처리 (비동기)"""
    try:
        job_id = f"process_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:19]}"

        # 현재 실행 중인 작업이 있는지 확인
        async with job_lock:
            running_jobs = [job for job in current_jobs.values() if job.status == "running"]
            if running_jobs:
                return {
                    "job_id": running_jobs[0].job_id,
                    "status": "already_running",
                    "message": "이미 처리 중인 작업이 있습니다"
                }

            job = ProcessingJob(
                job_id=job_id,
                status="running",
                started_at=datetime.now().isoformat()
            )
            current_jobs[job_id] = job

        asyncio.create_task(run_processing_job(job_id))

        return {
            "job_id": job_id,
            "status": "started",
            "message": "처리 작업이 시작되었습니다"
        }

    except Exception as e:
        logger.error(f"처리 요청 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate-assets")
async def validate_assets(request: ValidateAssetsRequest, background_tasks: BackgroundTasks):
    """DataFrame 기반 NAS Assets 파일 유효성 검사 (비동기)"""
    try:
        job_id = f"validate_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:19]}"
        
        async with job_lock:
            # 기존 validation job 확인
            running_validation_jobs = [
                job for job in current_jobs.values() 
                if job.status == "running" and job.job_id.startswith("validate_")
            ]
            
            if running_validation_jobs:
                return {
                    "job_id": running_validation_jobs[0].job_id,
                    "status": "already_running",
                    "message": "이미 유효성 검사가 진행 중입니다"
                }
        
            job = ProcessingJob(
                job_id=job_id,
                status="running",
                started_at=datetime.now().isoformat()
            )
            current_jobs[job_id] = job
        
        # 백그라운드 작업 시작
        asyncio.create_task(run_validation_job(job_id, request))
        
        return {
            "job_id": job_id,
            "status": "started",
            "message": f"파일 유효성 검사가 시작되었습니다. 데이터: {len(request.search_data)}개"
        }
        
    except Exception as e:
        logger.error(f"유효성 검사 요청 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    async with job_lock:
        job = current_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "job_id": job.job_id,
        "status": job.status,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "result": job.result,
        "error": job.error
    }


@app.get("/jobs")
async def list_jobs():
    """모든 작업 목록"""
    async with job_lock:
        return {
            "jobs": [
                {
                    "job_id": job.job_id,
                    "status": job.status,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at
                }
                for job in current_jobs.values()
            ]
        }


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """작업 삭제 (완료된 작업만)"""
    async with job_lock:
        if job_id not in current_jobs:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = current_jobs[job_id]
        if job.status == "running":
            raise HTTPException(status_code=400, detail="Cannot delete running job")
        
        del current_jobs[job_id]
        logger.info(f"✅ 작업 {job_id} 삭제됨")
        return {"message": f"Job {job_id} deleted"}


async def run_processing_job(job_id: str):
    """백그라운드에서 실행할 처리 작업"""
    await _run_background_job(
        job_id=job_id,
        job_name="처리 작업",
        job_func=processor.process_all_pending,
        job_args=(),
    )


async def run_validation_job(job_id: str, request: ValidateAssetsRequest):
    """백그라운드에서 실행할 유효성 검사 작업"""
    await _run_background_job(
        job_id=job_id,
        job_name="유효성 검사",
        job_func=processor.validate_assets,
        job_args=(request.user_id, request.search_data, request.sample_percent),
        extra_log_info=f"유저: {request.user_id}, 데이터: {len(request.search_data)}개"
    )


async def _run_background_job(
    job_id: str, 
    job_name: str, 
    job_func, 
    job_args: tuple = (),
    extra_log_info: str = ""
):
    try:
        log_msg = f"🔄 {job_name} 시작: {job_id}"
        if extra_log_info:
            log_msg += f" ({extra_log_info})"
        logger.info(log_msg)
        
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(executor, job_func, *job_args)
        
        # 성공 시 상태 업데이트
        await _update_job_status(job_id, "completed", result=result)
        logger.info(f"✅ {job_name} 완료: {job_id}")
        
    except Exception as e:
        await _handle_job_error(job_id, e, job_name)


async def _update_job_status(
    job_id: str, 
    status: str, 
    result: dict = None, 
    error: str = None
):
    """작업 상태 업데이트"""
    async with job_lock:
        if job_id in current_jobs:
            current_jobs[job_id].status = status
            current_jobs[job_id].completed_at = datetime.now().isoformat()
            if result:
                current_jobs[job_id].result = result
            if error:
                current_jobs[job_id].error = error
                
                
async def _handle_job_error(job_id: str, error: Exception, job_type: str):
    error_msg = str(error)
    logger.error(f"❌ {job_type} 실패: {job_id} - {error_msg}")
    
    async with job_lock:
        if job_id in current_jobs:
            current_jobs[job_id].status = "failed"
            current_jobs[job_id].completed_at = datetime.now().isoformat()
            current_jobs[job_id].error = error_msg


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Datalake Processing API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("--base-path", default="/mnt/AI_NAS/datalake/", help="Base path for datalake")
    parser.add_argument("--num-proc", type=int, default=16, help="Number of processing threads")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing")
    parser.add_argument("--create-dirs", action="store_true", help="Create necessary directories if they do not exist")

    args = parser.parse_args()

    os.environ["BASE_PATH"] = args.base_path
    os.environ["LOG_LEVEL"] = args.log_level
    os.environ["NUM_PROC"] = str(args.num_proc)
    os.environ["BATCH_SIZE"] = str(args.batch_size)
    os.environ["CREATE_DIRS"] = str(args.create_dirs).lower()
    print(f"🚀 Starting Datalake Processing API Server on {args.host}:{args.port}")

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        workers=args.workers,
    )
if __name__ == "__main__":
    main()
