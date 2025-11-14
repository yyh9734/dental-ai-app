# === tasks.py (Render 배포용 - 전체 텍스트) ===
import os
import time
import boto3
import openai
from celery import Celery
import requests 
from dotenv import load_dotenv
import json 
import uuid 

# .env 파일 로드 (Render 환경 변수가 우선함)
load_dotenv()

# --- 환경 변수 로드 (Render 대시보드에서 설정) ---
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
AWS_REGION = "ap-northeast-2"
S3_BUCKET_NAME = "dental-ai-recordings"

# (★수정됨) API 서버 주소 (Render의 'Web Service' URL)
# [TODO] OOO님의 'Web Service (API 서버)' URL로 교체
API_SERVER_URL = "https://[OOO님의-API서버-URL].onrender.com"

# (★수정됨) Celery 설정 (Render의 'Key Value' URL)
# (Render 환경 변수에서 CELERY_BROKER_URL, CELERY_RESULT_BACKEND를 읽어옴)
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

celery_app = Celery('tasks', broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
celery_app.conf.update(task_track_started=True)
# print("CELERY-WORKER: Celery 앱 설정 완료.")

# --- Boto3 및 OpenAI 클라이언트 초기화 ---
try:
    s3_client = boto3.client(
        's3', 
        region_name=AWS_REGION, 
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    transcribe_client = boto3.client(
        'transcribe', 
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    # print("CELERY-WORKER: Boto3 클라이언트 초기화 성공.")
except Exception as e:
    print(f"CELERY-WORKER: Boto3 클라이언트 초기화 실패: {e}")
    s3_client = None
    transcribe_client = None

if OPENAI_API_KEY:
    try:
        openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
        print("CELERY-WORKER: OpenAI API 클라이언트 초기화 완료.")
    except Exception as e:
        print(f"CELERY-WORKER: OpenAI 클라이언트 초기화 실패: {e}")
        openai_client = None
else:
    print("CELERY-WORKER: 경고! OPENAI_API_KEY가 설정되지 않았습니다.")
    openai_client = None


# --- 상태 업데이트 헬퍼 함수 ---
def update_job_status(job_id, status, result=None):
    try:
        payload = {"job_id": job_id, "status": status, "result": result}
        # (★수정됨) API_SERVER_URL 사용
        requests.post(f"{API_SERVER_URL}/api/update-job-status", json=payload)
        print(f"CELERY-WORKER: Job {job_id} 상태 -> {status} (API 서버로 전송)")
    except Exception as e:
        print(f"CELERY-WORKER: (치명적) API 서버로 상태 업데이트 실패: {e}")

# =================================================================
# 실제 AWS Transcribe 호출 및 파싱 함수
# =================================================================

def parse_transcribe_json(result_json):
    try:
        items = result_json['results']['items']
    except KeyError:
        print("WORKER: Transcribe JSON 결과가 예상과 다릅니다. 'results' 또는 'items' 키 없음.")
        return "Transcribe JSON 파싱 오류"
    
    transcript_lines = []
    current_speaker = None
    current_line_words = []

    for item in items:
        speaker = item.get('speaker_label', None)
        content = item['alternatives'][0]['content']
        item_type = item.get('type', 'pronunciation') 
        
        if speaker is None and item_type != 'punctuation':
            speaker = current_speaker
        
        if current_speaker is None:
            current_speaker = speaker 

        if item_type == 'pronunciation':
            if speaker != current_speaker and current_speaker is not None and current_line_words:
                transcript_lines.append(f"[{current_speaker}]: {' '.join(current_line_words)}")
                current_line_words = [content]
                current_speaker = speaker
            else:
                current_line_words.append(content)
        elif item_type == 'punctuation':
            if current_line_words:
                current_line_words[-1] += content
    
    if current_line_words and current_speaker is not None:
        transcript_lines.append(f"[{current_speaker}]: {' '.join(current_line_words)}")

    final_transcript = "\n".join(transcript_lines)
    return final_transcript

def call_aws_transcribe_real(job_id, s3_file_key):
    if not transcribe_client or not s3_client:
        raise Exception("AWS 클라이언트가 초기화되지 않았습니다.")
    
    transcription_job_name = f"dental-ai-job-{uuid.uuid4().hex[:8]}-{job_id}"
    s3_media_uri = f"s3://{S3_BUCKET_NAME}/{s3_file_key}"
    output_key = f"results/{transcription_job_name}.json"
    
    print(f"CELERY-WORKER: Job {job_id} | 1. (REAL) AWS Transcribe Medical 작업 시작...")
    print(f"CELERY-WORKER: Job Name: {transcription_job_name}, Media URI: {s3_media_uri}")
    
    try:
        transcribe_client.start_medical_transcription_job(
            MedicalTranscriptionJobName=transcription_job_name,
            LanguageCode='ko-KR',
            Media={'MediaFileUri': s3_media_uri},
            OutputBucketName=S3_BUCKET_NAME,
            OutputKey=output_key, 
            Specialty='PRIMARYCARE',
            Type='CONVERSATION',
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 2 
            }
        )
    except Exception as e:
        if "ConflictException" in str(e):
            print(f"CELERY-WORKER: Job {transcription_job_name}이(가) 이미 존재합니다. 상태를 확인합니다.")
        else:
            print(f"CELERY-WORKER: Transcribe 작업 시작 실패: {e}")
            raise e

    while True:
        try:
            job = transcribe_client.get_medical_transcription_job(
                MedicalTranscriptionJobName=transcription_job_name
            )
            job_status = job['MedicalTranscriptionJob']['TranscriptionJobStatus']
            
            if job_status in ['COMPLETED', 'FAILED']:
                break
            
            print(f"CELERY-WORKER: Job {job_id} | 1. STT 진행 중... (AWS Status: {job_status})")
            update_job_status(job_id, f"processing_stt ({job_status})")
            time.sleep(10)
            
        except transcribe_client.exceptions.BadRequestException:
            print(f"CELERY-WORKER: Job {job_id} | 1. STT 작업 등록 대기 중...")
            time.sleep(5)

    if job_status == 'FAILED':
        failure_reason = job['MedicalTranscriptionJob'].get('FailureReason', '알 수 없는 이유')
        print(f"CELERY-WORKER: Job {job_id} | 1. STT 실패: {failure_reason}")
        raise Exception(f"AWS Transcribe 실패: {failure_reason}")

    print(f"CELERY-WORKER: Job {job_id} | 1. STT 완료. S3에서 결과 다운로드 중...")
    
    try:
        result_object = s3_client.get_object(
            Bucket=S3_BUCKET_NAME, 
            Key=output_key
        )
        result_content = result_object['Body'].read().decode('utf-8')
        result_json = json.loads(result_content)
        
    except Exception as e:
        print(f"CELERY-WORKER: S3에서 Transcribe 결과({output_key}) 다운로드 실패: {e}")
        raise e
    
    print(f"CELERY-WORKER: Job {job_id} | 1. 결과 파싱 중...")
    transcript_text = parse_transcribe_json(result_json)
    
    return transcript_text


# =================================================================
# (신규) 실제 OpenAI (GPT) 호출 함수
# =================================================================

def summarize_soap_real(job_id, full_transcript):
    print(f"CELERY-WORKER: Job {job_id} | 2. (REAL) OpenAI SOAP 요약 시작...")
    
    if not openai_client:
        raise Exception("OpenAI 클라이언트가 초기화되지 않았습니다. API 키를 확인하세요.")
    
    if not full_transcript or full_transcript == "Transcribe JSON 파싱 오류":
        raise Exception("SOAP 요약을 위한 대본이 유효하지 않습니다.")

    system_prompt = """
    당신은 치과 EMR 차트 작성을 돕는 전문 AI 어시스턴트입니다.
    
    [입력]
    [spk_0]와 [spk_1]로 화자가 구분된 치과 진료 대화 대본이 주어집니다.

    [과업]
    1. (화자 식별) 대화의 맥락(질문/답변, 전문 용어 사용)을 분석하여 누가 '환자'이고 누가 '술자'(치과의사)인지 식별하십시오.
    2. (SOAP 요약) 식별된 역할을 기반으로, 대화 내용을 한국어 SOAP 형식으로 요약하여 EMR에 바로 기입할 수 있도록 정리하십시오.

    [규칙]
    - S (Subjective): '환자'가 호소하는 주관적인 증상 (C.C, PI)
    - O (Objective): '술자'가 관찰한 객관적인 소견 (검사 결과, 구강 상태)
    - A (Assessment): '술자'가 내린 진단명 또는 평가
    - P (Plan): '술자'가 제시한 향후 치료 계획
    - '환자'의 발언은 'S'에만 포함되어야 합니다.
    - '술자'의 발언은 'O', 'A', 'P'에만 포함되어야 합니다.

    [출력 형식]
    반드시 유효한 JSON 객체만을 응답으로 반환하십시오. 다른 설명은 절대 추가하지 마십시오.
    {
      "s": "...",
      "o": "...",
      "a": "...",
      "p": "..."
    }
    """
    
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": full_transcript}
            ],
            temperature=0.3,
            response_format={"type": "json_object"} 
        )
        
        response_content = completion.choices[0].message.content
        print(f"CELERY-WORKER: Job {job_id} | 2. OpenAI 응답 수신. 파싱 중...")
        soap_json = json.loads(response_content)
        
        final_payload = {
            "s": soap_json.get("s", "AI 요약 실패"),
            "o": soap_json.get("o", "AI 요약 실패"),
            "a": soap_json.get("a", "AI 요약 실패"),
            "p": soap_json.get("p", "AI 요약 실패"),
            "transcript": full_transcript 
        }
        
        print(f"CELERY-WORKER: Job {job_id} | 2. SOAP 요약 완료 (REAL)")
        return final_payload
        
    except Exception as e:
        print(f"CELERY-WORKER: (치명적) OpenAI API 호출 또는 JSON 파싱 실패: {e}")
        failed_payload = {
            "s": "AI 요약 실패", "o": "AI 요약 실패", "a": "AI 요약 실패", "p": "AI 요약 실패",
            "transcript": f"대본:\n{full_transcript}\n\n[오류: OpenAI 요약에 실패했습니다: {e}]"
        }
        return failed_payload


# --- (★수정됨) Celery 메인 작업 함수 ---
@celery_app.task(name='process_audio_task')
def process_audio_task(job_id, file_key):
    """
    S3 파일 키를 받아 전체 AI 파이프라인을 실행하는 메인 태스크
    """
    print(f"CELERY-WORKER: Job {job_id} 작업 시작! (파일 키: {file_key})")
    
    try:
        update_job_status(job_id, "processing_stt")
        transcript_text = call_aws_transcribe_real(job_id, file_key)

        update_job_status(job_id, "processing_soap")
        final_result = summarize_soap_real(job_id, transcript_text)

        update_job_status(job_id, "completed", result=final_result)
        
        return f"Job {job_id} 완료"

    except Exception as e:
        print(f"CELERY-WORKER: (치명적) Job {job_id} 처리 실패: {e}")
        error_message = str(e)
        if "Transcribe" in error_message:
            error_message = f"음성 변환(Transcribe) 실패: {error_message}"
        elif "OpenAI" in error_message:
            error_message = f"AI 요약(OpenAI) 실패: {error_message}"
        
        update_job_status(job_id, "failed", result={"error_message": error_message})
        return f"Job {job_id} 실패: {e}"

print("CELERY-WORKER: 태스크 모듈 로드 완료.")