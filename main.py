# === main.py (Render 배포용 - 전체 텍스트) ===

import uvicorn
from fastapi import FastAPI, HTTPException, Request, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
import os
import uuid
from celery import Celery
from dotenv import load_dotenv

# .env 파일 로드 (Render 환경 변수가 우선함)
load_dotenv()

# --- 환경 변수 로드 (Render 대시보드에서 설정) ---
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
AWS_REGION = "ap-northeast-2"
S3_BUCKET_NAME = "dental-ai-recordings" 

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

# (★수정됨) OOO님의 Render 프론트엔드 URL을 허용
origins = [
    "https://dental-ai-app-xyj4.onrender.com" # OOO님의 실제 프론트엔드 URL
    # "http://localhost:8080" # (선택) 로컬 테스트용
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
print(f"API-SERVER: CORS 설정 완료. {origins} 허용.")

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

@app.post("/api/upload-audio")
async def upload_audio(audio_file: UploadFile = File(...)):
    if not s3_client:
        raise HTTPException(status_code=503, detail="S3 서비스에 연결할 수 없습니다.")

    file_extension = "webm" 
    file_key = f"uploads/{uuid.uuid4()}.{file_extension}"
    
    print(f"API-SERVER: 파일 수신 시작: {audio_file.filename} -> S3 ({file_key})")
    
    try:
        s3_client.upload_fileobj(
            audio_file.file,
            S3_BUCKET_NAME,
            file_key,
            ExtraArgs={'ContentType': audio_file.content_type}
        )
        print(f"API-SERVER: S3 업로드 성공: {file_key}")
    except Exception as e:
        print(f"API-SERVER: S3 업로드 실패: {e}")
        raise HTTPException(status_code=500, detail=f"S3 업로드 실패: {e}")
    finally:
        await audio_file.close() 
        
    job_id = str(uuid.uuid4())
    jobs_db[job_id] = {"status": "pending", "file_key": file_key, "result": None}

    try:
        process_audio_task.delay(job_id, file_key)
        print(f"API-SERVER: Job {job_id}를 Celery 큐에 추가했습니다.")
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


# --- 서버 시작 로직 (Render는 이 부분을 사용하지 않고 'Start Command'를 사용함) ---
if __name__ == "__main__":
    print("API-SERVER: (로컬 실행) FastAPI 서버를 uvicorn으로 시작합니다 (http://localhost:8000)")
    uvicorn.run(app, host="0.0.0.0", port=8000)