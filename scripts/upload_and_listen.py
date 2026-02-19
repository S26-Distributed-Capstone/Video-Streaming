#!/usr/bin/env python3
import argparse
import json
import os
import sys


def require(module_name, pip_name):
    try:
        return __import__(module_name)
    except Exception:
        print(
            f"Missing dependency: {module_name}. Install with: pip install {pip_name}",
            file=sys.stderr,
        )
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description="Upload a video and listen for WebSocket status updates."
    )
    parser.add_argument("file", help="Path to video file to upload")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8080",
        help="Upload service base URL (default: http://localhost:8080)",
    )
    args = parser.parse_args()

    requests = require("requests", "requests")
    websocket = require("websocket", "websocket-client")

    upload_url = args.base_url.rstrip("/") + "/upload"

    with open(args.file, "rb") as f:
        files = {"file": (os.path.basename(args.file), f, "video/mp4")}
        resp = requests.post(upload_url, files=files)

    if resp.status_code != 202:
        print(f"Upload failed: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    try:
        payload = resp.json()
    except Exception:
        payload = json.loads(resp.text)

    video_id = payload.get("videoId") or payload.get("video_id") or payload.get("id")
    upload_status_url = payload.get("uploadStatusUrl")

    if not video_id:
        print(
            "Upload response did not include a video ID; cannot coninue.",
            file=sys.stderr,
        )
        print(f"Response payload: {payload}", file=sys.stderr)
        sys.exit(1)

    info_url = args.base_url.rstrip("/") + "/upload-info/" + video_id
    if not upload_status_url:
        scheme = "ws" if args.base_url.startswith("http://") else "wss"
        host = args.base_url.split("://", 1)[-1]
        upload_status_url = f"{scheme}://{host}/upload-status?jobId={video_id}"

    print(f"videoId: {video_id}")
    print(f"WebSocket: {upload_status_url}")
    try:
        info_resp = requests.get(info_url)
        print(f"Upload info: {info_resp.status_code} {info_resp.text}")
        if info_resp.status_code == 200:
            info_json = info_resp.json()
            total_segments = info_json.get("totalSegments")
            #RIGHT NOW NOT PULLING ANY DATA BECUASE IT DOESN'T EXIST
            #55 DEFAULT FOR TANI'S TEST VIDEO
            if total_segments is None:
                total_segments = 55
            print(f"totalSegments: {total_segments}")
    except Exception as e:
        print(f"Upload info request failed: {e}", file=sys.stderr)

    ws = websocket.WebSocket()
    ws.connect(upload_status_url)
    if video_id:
        ws.send(f"job:{video_id}")
    print("Connected. Listening for status events...")

    try:
        while True:
            msg = ws.recv()
            if msg is None:
                break
            print(msg)
    except KeyboardInterrupt:
        pass
    finally:
        ws.close()


if __name__ == "__main__":
    main()
