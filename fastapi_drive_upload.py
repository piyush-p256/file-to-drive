"""
FastAPI service: POST /upload
Accepts: multipart/form-data file (PDF) and folder_id (form field)
Uploads file to specified Google Drive folder and returns shareable link.

Supports two auth methods (choose one):
1) OAuth2 using client_id, client_secret, refresh_token (recommended if using a pre-authorized user)
   - Set env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
2) Service account JSON file
   - Set env var: GOOGLE_SERVICE_ACCOUNT_FILE (path to JSON file)

Requirements (pip):
fastapi, uvicorn, google-api-python-client, google-auth, google-auth-httplib2,
google-auth-oauthlib, aiofiles, python-multipart

Run: uvicorn fastapi_drive_upload:app --reload
"""

import os
import io
import uuid
import asyncio
import tempfile
from typing import Optional
from fastapi import FastAPI, UploadFile, Form, HTTPException, status
from fastapi.responses import JSONResponse
import aiofiles
from concurrent.futures import ThreadPoolExecutor
import dotenv
# Google libs
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.transport.requests import Request as GoogleRequest
dotenv.load_dotenv() 
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ✅ Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constants
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB
# ✅ Use system temp directory instead of hardcoding /tmp
UPLOAD_TMP_DIR = tempfile.gettempdir()
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

app = FastAPI(title="Drive PDF Uploader")

# ThreadPool for blocking Google Drive API calls
executor = ThreadPoolExecutor(max_workers=4)


def get_drive_service():
    """Build a Google Drive service using either service account or refresh-token OAuth2."""
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if sa_file and os.path.isfile(sa_file):
        creds = service_account.Credentials.from_service_account_file(sa_file, scopes=DRIVE_SCOPES)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    refresh_token = os.getenv("GOOGLE_REFRESH_TOKEN")

    if not (client_id and client_secret and refresh_token):
        raise RuntimeError(
            "Google credentials not configured. Set GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET/GOOGLE_REFRESH_TOKEN."
        )

    creds = OAuth2Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=DRIVE_SCOPES,
    )

    try:
        creds.refresh(GoogleRequest())
    except Exception as e:
        raise RuntimeError(f"Failed to refresh OAuth2 token: {e}")

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_file_to_drive_sync(local_path: str, filename: str, folder_id: str):
    """Blocking upload using googleapiclient."""
    service = get_drive_service()
    file_metadata = {"name": filename}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = MediaFileUpload(local_path, mimetype="application/pdf", resumable=True)
    try:
        request = service.files().create(body=file_metadata, media_body=media, fields="id")
        response = request.execute()
        file_id = response.get("id")
        if not file_id:
            raise RuntimeError("No file id returned from Drive API")
        return file_id
    except Exception as e:
        raise RuntimeError(f"Drive API upload failed: {e}")


async def save_upload_file(upload_file: UploadFile, dest_path: str):
    """Save UploadFile to disk while validating size and basic PDF signature."""
    total = 0

    async with aiofiles.open(dest_path, "wb") as out_file:
        chunk = await upload_file.read(4096)
        if not chunk:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        total += len(chunk)
        if total > MAX_FILE_SIZE:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File exceeds 20MB limit")

        # Validate PDF
        if not chunk.startswith(b"%PDF"):
            if not upload_file.filename.lower().endswith(".pdf"):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type: only PDF allowed")

        await out_file.write(chunk)

        while True:
            chunk = await upload_file.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_FILE_SIZE:
                raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File exceeds 20MB limit")
            await out_file.write(chunk)

    await upload_file.close()


@app.post("/upload")
async def upload(pdf: UploadFile, folder_id: Optional[str] = Form(None)):
    """Accept PDF file + Drive folder_id. Upload to Drive and return shareable link."""
    if not folder_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing folder_id form field")

    if pdf.content_type and pdf.content_type != "application/pdf":
        if not pdf.content_type.startswith("application/"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid content type: {pdf.content_type}")

    unique_name = f"upload_{uuid.uuid4().hex}.pdf"
    local_path = os.path.join(UPLOAD_TMP_DIR, unique_name)

    try:
        await save_upload_file(pdf, local_path)
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise HTTPException(status_code=500, detail=f"Failed to save uploaded file: {e}")

    loop = asyncio.get_running_loop()
    try:
        file_id = await loop.run_in_executor(executor, upload_file_to_drive_sync, local_path, pdf.filename or unique_name, folder_id)
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise HTTPException(status_code=502, detail=f"Drive upload failed: {e}")

    if os.path.exists(local_path):
        os.remove(local_path)

    shareable_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    return JSONResponse(status_code=201, content={"file_id": file_id, "shareable_url": shareable_url})

@app.get("/cron")
async def run_cron_task():
    # Example: just a heartbeat or scheduled cleanup
    print("Cron job triggered!")
    return {"status": "Cron executed successfully"}


@app.get("/")
async def root():
    return {"ok": True, "note": "POST /upload (multipart/form-data): fields 'pdf' (file) and 'folder_id' (form field)"}
