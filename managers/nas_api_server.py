import concurrent.futures
import asyncio
import logging
import uvicorn
import sys
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

sys.path.append(str(Path(__file__).resolve().parent.parent))  # 상위 디렉토리 추가
from managers.data_processor import NASDataProcessor
from managers.logger import setup_logging



# Request/Response 모델들
class ProcessRequest(BaseModel):
    """처리 요청 모델"""
    pass  # body 없이 모든 pending 처리

class ProcessResponse(BaseModel):
    """처리 응답 모델"""
    success: int
    failed: int
    message: str = ""

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


# 전역 변수들
processor = None

current_jobs: Dict[str, ProcessingJob] = {}
job_lock = asyncio.Lock()
setup_logging(log_level="INFO")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 실행"""
    global processor
    
    # 시작 시 프로세서 초기화
    logging.info("🚀 NASDataProcessor 초기화 중...")
    try:
        processor = NASDataProcessor(
            batch_size=1000,
            num_proc=4
        )
        
        logging.info("✅ NASDataProcessor 초기화 완료")
    except Exception as e:
        logging.error(f"❌ Processor 초기화 실패: {e}")
        raise
    
    yield
    
    # 종료 시 정리
    logging.info("🔄 서버 종료 중...")


# FastAPI 앱 생성
app = FastAPI(
    title="NAS Data Processing API",
    description="NAS에서 데이터 처리를 담당하는 API 서버",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/test-async")
async def test_async():
    """비동기 테스트용 엔드포인트"""
    async def long_task():
        await asyncio.sleep(5)  # 5초 대기
        return "완료"
    
    # 백그라운드에서 실행
    asyncio.create_task(long_task())
    
    # 즉시 응답
    return {"message": "백그라운드 작업 시작됨", "timestamp": datetime.now().isoformat()}

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
        logging.error(f"상태 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process", response_model=Dict)
async def process_pending_data(background_tasks: BackgroundTasks):
    """Pending 데이터 처리 (비동기)"""
    try:
        
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:19]}"
        
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
        logging.error(f"처리 요청 실패: {e}")
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
        return {"message": f"Job {job_id} deleted"}


async def run_processing_job(job_id: str):
    """백그라운드에서 실행할 처리 작업"""
    try:
        logging.info(f"🔄 처리 작업 시작: {job_id}")
        
        # 성공 시 작업 상태 업데이트
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 실제 처리를 별도 스레드에서 실행
            result = await loop.run_in_executor(
                executor, 
                processor.process_all_pending
            )
        async with job_lock:
            if job_id in current_jobs:
                current_jobs[job_id].status = "completed"
                current_jobs[job_id].completed_at = datetime.now().isoformat()
                current_jobs[job_id].result = result
        logging.info(f"✅ 처리 작업 완료: {job_id}, 결과: {result}")
        
    except Exception as e:
        # 실패 시 작업 상태 업데이트
        error_msg = str(e)
        logging.error(f"❌ 처리 작업 실패: {job_id}, 오류: {error_msg}")
        
        async with job_lock:
            if job_id in current_jobs:
                current_jobs[job_id].status = "failed"
                current_jobs[job_id].completed_at = datetime.now().isoformat()
                current_jobs[job_id].error = error_msg


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="NAS Data Processing API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    
    args = parser.parse_args()
    
    print(f"🚀 Starting NAS Data Processing API Server on {args.host}:{args.port}")
    
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        workers=args.workers,
        reload=False
    )