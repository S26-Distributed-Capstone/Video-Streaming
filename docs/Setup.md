# Setup and API Documentation

## Prerequisites

Ensure you have the following installed:
- Java 17+
- Maven
- Docker & Docker Compose
- FFmpeg (installed and available in system PATH)
- Postgres (installed and available in system PATH)

## Installing Dependencies

### FFmpeg Installation
The `UploadHandler` relies on `ffmpeg` and `ffprobe` binaries being available in the system PATH.

**macOS:**
```bash
brew install ffmpeg
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install ffmpeg
```

**Windows:**
1. Download the build from [gyan.dev](https://www.gyan.dev/ffmpeg/builds/) or [BtbN](https://github.com/BtbN/FFmpeg-Builds/releases).
2. Extract the zip file.
3. Add the `bin` folder (containing `ffmpeg.exe` and `ffprobe.exe`) to your System Path:
   - Search for "Edit the system environment variables".
   - Click "Environment Variables".
   - Under "System variables", select "Path" and click "Edit".
   - Click "New" and paste the path to the `bin` folder.
   - Click "OK" to save.

**Verification:**
Run these commands in a new terminal to verify installation:
```bash
ffmpeg -version
ffprobe -version
```

**Troubleshooting:**
If you receive an error like `command not found` or `'ffmpeg' is not recognized`:
1. **Restart Terminal:** After installing or updating the PATH, close and reopen your terminal for changes to take effect.
2. **Verify Path Configuration:**
   - **Windows:** Double-check that the entry in your System Environment Variables points exactly to the `bin` folder containing `ffmpeg.exe`.
   - **macOS/Linux:** If you downloaded binaries manually, add the folder to your PATH in `~/.zshrc` or `~/.bashrc`:
     ```bash
     export PATH=$PATH:/path/to/extracted/ffmpeg
     ```
     Then load the config: `source ~/.zshrc` or `source ~/.bashrc`.

## Setup Instructions

### Step 1: Start Object Storage (MinIO)

Start the MinIO service using Docker Compose:

```bash
docker-compose -f minio_docker_compose.yaml up -d
docker compose -f minio_docker_compose.yaml up -d
```

This will start a MinIO server accessible at `http://localhost:9000` with the following default credentials:
- **Username:** `minioadmin`
- **Password:** `minioadmin`

### Step 2: Run the Upload Service

Make sure you are in the **project root directory** (where the main `pom.xml` is located).

1. Install the project dependencies (including the `shared` module):
```bash
mvn install -DskipTests
```

2. Run the upload service:
```bash
mvn exec:java -pl upload-service -Dexec.mainClass="com.distributed26.videostreaming.upload.upload.UploadServiceApplication"
```

The service will start on **port 8080**.

### Step 3: Set Up Postgres (for Job/Task Tracking)

1. Install and start Postgres.
   **macOS (Homebrew):**
   ```bash
   brew install postgresql@16
   brew services start postgresql@16
   ```

2. Create the database:
   ```bash
   createdb videostreaming
   ```

3. Create the table:
   ```bash
   psql -d videostreaming -c "CREATE TABLE IF NOT EXISTS job_tasks (job_id TEXT PRIMARY KEY, num_tasks INTEGER NOT NULL);"
   ```

4. Add a test row:
   ```bash
   psql -d videostreaming -c "INSERT INTO job_tasks (job_id, num_tasks) VALUES ('job-123', 3) ON CONFLICT (job_id) DO UPDATE SET num_tasks = EXCLUDED.num_tasks;"
   ```
---

## API Documentation

### Upload Video

#### Endpoint
`POST /upload`

Uploads a video file, segments it using FFmpeg, and uploads the segments to the object storage service.

#### Request

- **Content-Type:** `multipart/form-data`
- **Body Parameters:**
  - `file`: The video file to upload (binary).

**Example Curl Request**
- curl -X POST http://localhost:8080/upload \
  -H "Accept: application/json" \
  -F "file=@/path/to/video.mp4"


#### Response

- **Status:** `201 Created`
- **Content-Type:** `application/json`
- **Body:** The generated UUID of the uploaded video.

**Example Response:**
```json
"550e8400-e29b-41d4-a716-446655440000"
```

#### Error Responses

| Status Code | Description |
|------------|-------------|
| `400 Bad Request` | If the `file` part is missing from the request. |
| `500 Internal Server Error` | If video processing or upload to object storage fails. |
