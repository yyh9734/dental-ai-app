# === main.py (프록시 업로드 버전) ===

import uvicorn
from fastapi import FastAPI, HTTPException, Request, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
import os
import uuid
from celery import Celery
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# --- 환경 변수 로드 ---
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
AWS_REGION = "ap-northeast-2"
S3_BUCKET_NAME = "dental-ai-recordings" # OOO님의 버킷 이름

# --- Boto3 클라이언트 초기화 ---
try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print("API-SERVER: Boto3 S3 클라이언트 초기화 성공")
except Exception as e:
    print(f"API-SERVER: Boto3 클라이언트 초기화 실패: {e}")
    s3_client = None

# --- FastAPI 앱 초기화 및 CORS 설정 ---
app = FastAPI()

origins = [
    "http://localhost:8080", # 프론트엔드
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
print("API-SERVER: CORS 설정 완료. http://localhost:8080 허용.")

# --- Celery 연결 ---
try:
    from tasks import process_audio_task
    print("API-SERVER: 'tasks.py'에서 'process_audio_task' 임포트 성공.")
except ImportError as e:
    print(f"API-SERVER: 'tasks.py' 임포트 실패! {e}")
except Exception as e:
    print(f"API-SERVER: 'tasks.py' 로드 중 알 수 없는 오류: {e}")


# --- (가상) DB ---
jobs_db = {}
print("API-SERVER: (가상) jobs_db 초기화 완료.")

# --- API 엔드포인트 ---

@app.get("/")
def read_root():
    return {"message": "AI Dental Assistant API"}

# (신규) /api/upload-audio 엔드포인트
@app.post("/api/upload-audio")
async def upload_audio(audio_file: UploadFile = File(...)):
    if not s3_client:
        raise HTTPException(status_code=503, detail="S3 서비스에 연결할 수 없습니다.")

    # 1. 파일 키 생성 (S3에 저장될 이름)
    file_extension = "webm" # 오디오 타입 고정 (필요시 수정)
    file_key = f"uploads/{uuid.uuid4()}.{file_extension}"
    
    print(f"API-SERVER: 파일 수신 시작: {audio_file.filename} -> S3 ({file_key})")
    
    try:
        # 2. (서버가 직접) S3로 파일 업로드
        s3_client.upload_fileobj(
            audio_file.file,  # FastAPI의 UploadFile 객체
            S3_BUCKET_NAME,
            file_key,
            ExtraArgs={'ContentType': audio_file.content_type}
        )
        print(f"API-SERVER: S3 업로드 성공: {file_key}")
    except Exception as e:
        print(f"API-SERVER: S3 업로드 실패: {e}")
        raise HTTPException(status_code=500, detail=f"S3 업로드 실패: {e}")
    finally:
        await audio_file.close() # 파일 핸들러 닫기
        
    # 3. S3 업로드 성공 후, Celery 작업 지시
    job_id = str(uuid.uuid4())
    jobs_db[job_id] = {"status": "pending", "file_key": file_key, "result": None}

    try:
        process_audio_task.delay(job_id, file_key)
        print(f"API-SERVER: Job {job_id}를 Celery 큐에 추가했습니다.")
        # 4. 프론트엔드에 job_id 반환
        return {"job_id": job_id, "status": "pending"}
    except Exception as e:
        print(f"API-SERVER: Celery 작업 지시 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Celery 작업 지시 실패: {e}")


@app.get("/api/get-status/{job_id}")
async def get_status(job_id: str):
    job = jobs_db.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다.")
    return job

# (신규) 워커가 상태를 업데이트할 내부 API
class JobUpdate(BaseModel):
    job_id: str
    status: str
    result: dict | None = None

@app.post("/api/update-job-status")
async def update_job_status(update: JobUpdate):
    if update.job_id not in jobs_db:
        print(f"API-SERVER: (경고) 존재하지 않는 Job ID({update.job_id})에 대한 업데이트 요청 수신")
        jobs_db[update.job_id] = {}
        
    jobs_db[update.job_id]["status"] = update.status
    if update.result:
        jobs_db[update.job_id]["result"] = update.result
    
    print(f"API-SERVER: (워커로부터 수신) Job {update.job_id} 상태 -> {update.status}")
    return {"message": "상태 업데이트 성공"}


# --- 서버 시작 로직 ---
if __name__ == "__main__":
    print("API-SERVER: FastAPI 서버를 uvicorn으로 시작합니다 (http://localhost:8000)")
    uvicorn.run(app, host="0.0.0.0", port=8000)