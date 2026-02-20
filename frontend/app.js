const fileInput = document.getElementById("fileInput");
const uploadBtn = document.getElementById("uploadBtn");
const responseBox = document.getElementById("responseBox");
const infoBox = document.getElementById("infoBox");
const wsUrlLabel = document.getElementById("wsUrl");
const logEl = document.getElementById("log");
const uploadBar = document.getElementById("uploadBar");
const uploadPercent = document.getElementById("uploadPercent");
const processingBar = document.getElementById("processingBar");
const processingPercent = document.getElementById("processingPercent");
const processingTrack = document.getElementById("processingTrack");
const processingBlock = document.getElementById("processingBlock");
const doneMessage = document.getElementById("doneMessage");
const reconnectBtn = document.getElementById("reconnectBtn");

let ws = null;
let currentVideoId = null;
let totalSegments = null;
let completedSegments = 0;
let currentWsUrl = null;
let processingComplete = false;
let uploadInFlight = false;

function resetStateForNextUpload() {
  totalSegments = null;
  completedSegments = 0;
  processingComplete = false;
}

function appendLog(message, tone = "") {
  if (!logEl) {
    return;
  }
  const line = document.createElement("div");
  line.className = "line";
  const ts = new Date().toLocaleTimeString();
  line.textContent = `[${ts}] ${message}`;
  if (tone === "error") {
    line.style.color = "#fc8181";
  }
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function resetProgress() {
  uploadBar.style.width = "0%";
  uploadPercent.textContent = "";
  processingBar.style.width = "0%";
  processingPercent.textContent = "";
  if (processingTrack) {
    processingTrack.classList.add("indeterminate");
  }
  if (processingBlock) {
    processingBlock.classList.add("hidden");
  }
  if (doneMessage) {
    doneMessage.classList.add("hidden");
    doneMessage.textContent = "Upload complete.";
  }
  resetStateForNextUpload();
}

function resolveBaseUrl() {
  return window.location.origin;
}

function renderJson(target, payload) {
  target.textContent = JSON.stringify(payload, null, 2);
}

function deriveWsUrl(baseUrl, videoId) {
  const scheme = baseUrl.startsWith("https://") ? "wss" : "ws";
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const statusPort = "8081";
  return `${scheme}://${host}:${statusPort}/upload-status?jobId=${videoId}`;
}

function connectWebSocket(wsUrl, videoId) {
  if (ws) {
    ws.close();
  }
  currentWsUrl = wsUrl;
  if (wsUrlLabel) {
    wsUrlLabel.textContent = wsUrl;
  }

  ws = new WebSocket(wsUrl);

  ws.addEventListener("open", () => {
    appendLog("WebSocket connected");
    if (videoId) {
      ws.send(`job:${videoId}`);
    }
  });

  ws.addEventListener("message", (event) => {
    appendLog(event.data);
    try {
      const payload = JSON.parse(event.data);
      if (payload && payload.type === "meta" && typeof payload.totalSegments === "number") {
        totalSegments = payload.totalSegments;
        if (processingPercent) {
          processingPercent.textContent = `0% (0/${totalSegments})`;
        }
        if (processingTrack) {
          processingTrack.classList.remove("indeterminate");
        }
        return;
      }
      if (payload && payload.type === "progress" && typeof payload.completedSegments === "number") {
        completedSegments = payload.completedSegments;
        if (totalSegments) {
          const percent = Math.min(100, Math.round((completedSegments / totalSegments) * 100));
          processingBar.style.width = `${percent}%`;
          processingPercent.textContent = `${percent}% (${completedSegments}/${totalSegments})`;
          processingTrack.classList.remove("indeterminate");
          if (completedSegments >= totalSegments && doneMessage) {
            processingComplete = true;
            doneMessage.classList.remove("hidden");
            uploadBtn.disabled = false;
            uploadInFlight = false;
          }
        } else {
          processingPercent.textContent = `${completedSegments} events`;
        }
        return;
      }
      if (payload && payload.taskId) {
        completedSegments += 1;
        if (totalSegments) {
          const percent = Math.min(100, Math.round((completedSegments / totalSegments) * 100));
          processingBar.style.width = `${percent}%`;
          processingPercent.textContent = `${percent}% (${completedSegments}/${totalSegments})`;
          processingTrack.classList.remove("indeterminate");
          if (completedSegments >= totalSegments && doneMessage) {
            processingComplete = true;
            doneMessage.classList.remove("hidden");
            uploadBtn.disabled = false;
            uploadInFlight = false;
          }
        } else {
          processingPercent.textContent = `${completedSegments} events`;
        }
      }
    } catch (err) {
      // Non-JSON messages are fine.
    }
  });

  ws.addEventListener("close", () => {
    appendLog("WebSocket disconnected");
    if (!processingComplete) {
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      if (doneMessage) {
        doneMessage.textContent = "Upload failed.";
        doneMessage.classList.remove("hidden");
      }
    }
  });

  ws.addEventListener("error", () => {
    appendLog("WebSocket error", "error");
    if (!processingComplete) {
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      if (doneMessage) {
        doneMessage.textContent = "Upload failed.";
        doneMessage.classList.remove("hidden");
      }
    }
  });
}

async function fetchUploadInfo(baseUrl, videoId) {
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const statusPort = "8081";
  const infoUrl = `${baseUrl.startsWith("https://") ? "https" : "http"}://${host}:${statusPort}/upload-info/${videoId}`;
  try {
    const resp = await fetch(infoUrl);
    const text = await resp.text();
    if (!resp.ok) {
      if (infoBox) {
        infoBox.textContent = `${resp.status} ${text}`;
      }
      return;
    }
    const payload = JSON.parse(text);
    if (infoBox) {
      renderJson(infoBox, payload);
    }
    if (payload.totalSegments != null) {
      totalSegments = payload.totalSegments;
      if (processingPercent) {
        processingPercent.textContent = `0% (0/${totalSegments})`;
      }
      if (processingTrack) {
        processingTrack.classList.remove("indeterminate");
      }
    }
  } catch (err) {
    if (infoBox) {
      infoBox.textContent = `Upload info error: ${err}`;
    }
  }
}

function uploadFile() {
  if (uploadInFlight || uploadBtn.disabled) {
    return;
  }
  const file = fileInput.files[0];
  if (!file) {
    appendLog("Select a video file before uploading.", "error");
    return;
  }

  const baseUrl = resolveBaseUrl();
  const uploadUrl = `${baseUrl}/upload`;

  uploadBtn.disabled = true;
  uploadInFlight = true;
  resetProgress();
  if (responseBox) {
    responseBox.textContent = "—";
  }
  if (infoBox) {
    infoBox.textContent = "—";
  }
  if (logEl) {
    logEl.textContent = "";
  }

  const formData = new FormData();
  formData.append("file", file, file.name);

  const xhr = new XMLHttpRequest();
  xhr.open("POST", uploadUrl, true);

  xhr.upload.addEventListener("progress", (event) => {
    if (!event.lengthComputable) {
      return;
    }
    const percent = Math.round((event.loaded / event.total) * 100);
    uploadBar.style.width = `${percent}%`;
  });

  xhr.addEventListener("load", async () => {
    let payload;
    try {
      payload = JSON.parse(xhr.responseText);
    } catch (err) {
      payload = { raw: xhr.responseText };
    }
    if (responseBox) {
      renderJson(responseBox, payload);
    }

    if (xhr.status !== 202) {
      appendLog(`Upload failed: ${xhr.status}`, "error");
      if (doneMessage) {
        doneMessage.textContent = "Upload failed.";
        doneMessage.classList.remove("hidden");
      }
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      return;
    }

    currentVideoId = payload.videoId || payload.video_id || payload.id;
    if (!currentVideoId) {
      appendLog("Upload response missing videoId", "error");
      if (doneMessage) {
        doneMessage.textContent = "Upload failed.";
        doneMessage.classList.remove("hidden");
      }
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      return;
    }

    if (processingBlock) {
      processingBlock.classList.remove("hidden");
    }
    if (processingPercent) {
      processingPercent.textContent = "0%";
    }

    await fetchUploadInfo(baseUrl, currentVideoId);

    const wsUrl = payload.uploadStatusUrl || deriveWsUrl(baseUrl, currentVideoId);
    connectWebSocket(wsUrl, currentVideoId);
  });

  xhr.addEventListener("error", () => {
    uploadBtn.disabled = false;
    uploadInFlight = false;
    appendLog("Upload failed due to a network error.", "error");
    resetStateForNextUpload();
    if (doneMessage) {
      doneMessage.textContent = "Upload failed.";
      doneMessage.classList.remove("hidden");
    }
  });

  xhr.send(formData);
}

uploadBtn.addEventListener("click", uploadFile);
if (reconnectBtn) {
  reconnectBtn.addEventListener("click", reconnect);
}
