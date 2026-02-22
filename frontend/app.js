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
let failureTerminal = false;
let containerDeathRetries = 0;
let retryInFlight = false;
let wsToken = 0;
let retryTimerId = null;
let retryCountdownId = null;
const RETRY_TOTAL_SECONDS = 10;
const RETRY_INTERVAL_MS = 1000;
let retrySecondsLeft = 0;

function clearRetryTimers() {
  if (retryTimerId) {
    clearTimeout(retryTimerId);
    retryTimerId = null;
  }
  if (retryCountdownId) {
    clearInterval(retryCountdownId);
    retryCountdownId = null;
  }
}

function updateRetryCountdown() {
  if (!doneMessage) {
    return;
  }
  const start = Date.now();
  const targetSeconds = retrySecondsLeft;
  const initialSeconds = Math.max(1, targetSeconds);
  setDoneMessage(`Retrying... ${initialSeconds}s`, { success: false });
  if (retryCountdownId) {
    clearInterval(retryCountdownId);
  }
  retryCountdownId = setInterval(() => {
    const elapsed = Date.now() - start;
    const remaining = Math.max(1, targetSeconds - Math.floor(elapsed / 1000));
    setDoneMessage(`Retrying... ${remaining}s`, { success: false });
    if (remaining <= 1) {
      clearInterval(retryCountdownId);
      retryCountdownId = null;
    }
  }, 250);
}

function scheduleRetry(reason) {
  if (retrySecondsLeft <= 0) {
    retrySecondsLeft = RETRY_TOTAL_SECONDS;
  }
  if (retrySecondsLeft < 1) {
    retryInFlight = false;
    clearRetryTimers();
    if (doneMessage) {
      setDoneMessage("Upload failed.", { success: false });
    }
    return;
  }
  updateRetryCountdown();
  const nextDelay = RETRY_INTERVAL_MS;
  clearRetryTimers();
  retryTimerId = setTimeout(() => {
    uploadFile({ preserveLog: true, isRetry: true });
  }, nextDelay);
  retrySecondsLeft -= 1;
  appendLog(`Retry scheduled in ${Math.ceil(nextDelay / 1000)}s (${reason})`, "error");
}

function resetStateForNextUpload() {
  totalSegments = null;
  completedSegments = 0;
  processingComplete = false;
  failureTerminal = false;
  retryInFlight = false;
  retrySecondsLeft = 0;
  clearRetryTimers();
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

function setDoneMessage(text, { success = false, hidden = false } = {}) {
  if (!doneMessage) {
    return;
  }
  doneMessage.textContent = text;
  doneMessage.classList.toggle("success", success);
  if (hidden) {
    doneMessage.classList.add("hidden");
  } else {
    doneMessage.classList.remove("hidden");
  }
}

function resetProgress({ preserveRetry } = {}) {
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
  if (doneMessage && !preserveRetry) {
    setDoneMessage("Upload complete.", { success: true, hidden: true });
  }
  if (!preserveRetry) {
    resetStateForNextUpload();
  }
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

function deriveUploadInfoUrl(baseUrl, videoId, uploadStatusUrl) {
  if (uploadStatusUrl) {
    try {
      const httpUrl = uploadStatusUrl.replace(/^ws/, "http");
      const url = new URL(httpUrl);
      url.pathname = `/upload-info/${videoId}`;
      url.search = "";
      return url.toString();
    } catch (err) {
      // fall back to default if URL parsing fails
    }
  }
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const statusPort = "8081";
  return `${baseUrl.startsWith("https://") ? "https" : "http"}://${host}:${statusPort}/upload-info/${videoId}`;
}

function connectWebSocket(wsUrl, videoId) {
  if (ws) {
    ws.close();
  }
  currentWsUrl = wsUrl;
  wsToken += 1;
  const token = wsToken;
  console.log("[upload-ui] connectWebSocket", { wsUrl, videoId, token });
  if (wsUrlLabel) {
    wsUrlLabel.textContent = wsUrl;
  }

  ws = new WebSocket(wsUrl);

  ws.addEventListener("open", () => {
    if (token !== wsToken) {
      return;
    }
    appendLog("WebSocket connected");
    retryInFlight = false;
    clearRetryTimers();
    setDoneMessage("Upload complete.", { success: true, hidden: true });
    console.log("[upload-ui] ws open", { wsUrl, videoId, token });
    if (videoId) {
      ws.send(`job:${videoId}`);
    }
  });

  ws.addEventListener("message", (event) => {
    if (token !== wsToken) {
      return;
    }
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
            setDoneMessage("Upload complete.", { success: true });
            uploadBtn.disabled = false;
            uploadInFlight = false;
          }
        } else {
          processingPercent.textContent = `${completedSegments} events`;
        }
        return;
      }
      if (payload && payload.type === "failed") {
        const reason = `${payload.reason || ""}`.trim();
        const normalizedReason = reason.toLowerCase().replace(/\s+/g, "_");
        const isContainerDied =
          normalizedReason === "container_died" ||
          (normalizedReason.includes("container") && normalizedReason.includes("die"));
        if (isContainerDied) {
          containerDeathRetries += 1;
          retryInFlight = true;
          uploadBtn.disabled = false;
          uploadInFlight = false;
          retrySecondsLeft = RETRY_TOTAL_SECONDS;
          appendLog(`Container died. Retrying upload (${containerDeathRetries})...`, "error");
          console.log("[upload-ui] scheduling retry", { containerDeathRetries, reason });
          scheduleRetry("container_died");
          return;
        }
        failureTerminal = true;
        processingComplete = true;
        uploadBtn.disabled = false;
        uploadInFlight = false;
        resetStateForNextUpload();
        if (doneMessage) {
          setDoneMessage("Upload failed.", { success: false });
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
            setDoneMessage("Upload complete.", { success: true });
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
    if (token !== wsToken) {
      return;
    }
    appendLog("WebSocket disconnected");
    if (!processingComplete && !failureTerminal && !retryInFlight) {
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      if (doneMessage) {
        setDoneMessage("Upload failed.", { success: false });
      }
    }
  });

  ws.addEventListener("error", () => {
    if (token !== wsToken) {
      return;
    }
    appendLog("WebSocket error", "error");
    if (!processingComplete && !failureTerminal && !retryInFlight) {
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      if (doneMessage) {
        setDoneMessage("Upload failed.", { success: false });
      }
    }
  });
}

async function fetchUploadInfo(baseUrl, videoId, uploadStatusUrl) {
  const infoUrl = deriveUploadInfoUrl(baseUrl, videoId, uploadStatusUrl);
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

function uploadFile({ preserveLog, isRetry } = {}) {
  if (uploadInFlight || uploadBtn.disabled) {
    return;
  }
  console.log("[upload-ui] uploadFile", { isRetry, preserveLog });
  const file = fileInput.files[0];
  if (!file) {
    appendLog("Select a video file before uploading.", "error");
    return;
  }

  const baseUrl = resolveBaseUrl();
  const uploadUrl = `${baseUrl}/upload`;

  uploadBtn.disabled = true;
  uploadInFlight = true;
  resetProgress({ preserveRetry: isRetry });
  if (!isRetry) {
    containerDeathRetries = 0;
    retryInFlight = false;
    retrySecondsLeft = 0;
    clearRetryTimers();
  } else {
    retryInFlight = true;
  }
  if (responseBox) {
    responseBox.textContent = "—";
  }
  if (infoBox) {
    infoBox.textContent = "—";
  }
  if (logEl && !preserveLog) {
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
    console.log("[upload-ui] upload response", { status: xhr.status, payload });
    if (responseBox) {
      renderJson(responseBox, payload);
    }

    if (xhr.status !== 202) {
      retryInFlight = false;
      clearRetryTimers();
      appendLog(`Upload failed: ${xhr.status}`, "error");
      if (doneMessage) {
        setDoneMessage("Upload failed.", { success: false });
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
        setDoneMessage("Upload failed.", { success: false });
      }
      uploadBtn.disabled = false;
      uploadInFlight = false;
      retryInFlight = false;
      clearRetryTimers();
      resetStateForNextUpload();
      return;
    }

    if (doneMessage) {
      setDoneMessage("Upload complete.", { success: true, hidden: true });
    }
    clearRetryTimers();
    retryInFlight = false;

    if (processingBlock) {
      processingBlock.classList.remove("hidden");
    }
    if (processingPercent) {
      processingPercent.textContent = "0%";
    }

    await fetchUploadInfo(baseUrl, currentVideoId, payload.uploadStatusUrl);

    const wsUrl = payload.uploadStatusUrl || deriveWsUrl(baseUrl, currentVideoId);
    connectWebSocket(wsUrl, currentVideoId);
  });

  xhr.addEventListener("error", () => {
    uploadBtn.disabled = false;
    uploadInFlight = false;
    if (retryInFlight) {
      scheduleRetry("network_error");
      return;
    }
    appendLog("Upload failed due to a network error.", "error");
    clearRetryTimers();
    resetStateForNextUpload();
    if (doneMessage) {
      setDoneMessage("Upload failed.", { success: false });
    }
  });

  xhr.send(formData);
}

uploadBtn.addEventListener("click", uploadFile);
if (reconnectBtn) {
  reconnectBtn.addEventListener("click", reconnect);
}
