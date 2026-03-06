const fileInput = document.getElementById("fileInput");
const videoNameInput = document.getElementById("videoName");
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
const transcodeBlock = document.getElementById("transcodeBlock");
const transcodeLowBar = document.getElementById("transcodeLowBar");
const transcodeLowPercent = document.getElementById("transcodeLowPercent");
const transcodeLowTrack = document.getElementById("transcodeLowTrack");
const transcodeMediumBar = document.getElementById("transcodeMediumBar");
const transcodeMediumPercent = document.getElementById("transcodeMediumPercent");
const transcodeMediumTrack = document.getElementById("transcodeMediumTrack");
const transcodeHighBar = document.getElementById("transcodeHighBar");
const transcodeHighPercent = document.getElementById("transcodeHighPercent");
const transcodeHighTrack = document.getElementById("transcodeHighTrack");
const doneMessage = document.getElementById("doneMessage");
const reconnectBtn = document.getElementById("reconnectBtn");
const player = document.getElementById("player");
const playerStatus = document.getElementById("playerStatus");
const playBtn = document.getElementById("playBtn");
const refreshReadyBtn = document.getElementById("refreshReadyBtn");
const readyList = document.getElementById("readyList");

let ws = null;
let currentVideoId = null;
let totalSegments = null;
let completedSegments = 0;
let sourceSegmentsComplete = false;
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
let hlsInstance = null;
let selectedVideoId = null;
const transcodeProfiles = {
  low: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  medium: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  high: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() }
};

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
  if (retrySecondsLeft <= 1) {
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
  sourceSegmentsComplete = false;
  processingComplete = false;
  failureTerminal = false;
  retryInFlight = false;
  retrySecondsLeft = 0;
  ["low", "medium", "high"].forEach((profile) => {
    const state = transcodeProfiles[profile];
    state.done = 0;
    state.transcoding = 0;
    state.uploading = 0;
    state.failed = 0;
    state.segments = new Map();
  });
  clearRetryTimers();
  teardownPlayer();
}

function getTranscodeDom(profile) {
  if (profile === "low") {
    return { bar: transcodeLowBar, percent: transcodeLowPercent, track: transcodeLowTrack };
  }
  if (profile === "medium") {
    return { bar: transcodeMediumBar, percent: transcodeMediumPercent, track: transcodeMediumTrack };
  }
  return { bar: transcodeHighBar, percent: transcodeHighPercent, track: transcodeHighTrack };
}

function recalcTranscodeCounters(profile) {
  const state = transcodeProfiles[profile];
  let transcoding = 0;
  let uploading = 0;
  let failed = 0;
  state.segments.forEach((status) => {
    if (status === "TRANSCODING") transcoding += 1;
    if (status === "UPLOADING") uploading += 1;
    if (status === "FAILED") failed += 1;
  });
  state.transcoding = transcoding;
  state.uploading = uploading;
  state.failed = failed;
}

function updateTranscodeProfileUi(profile) {
  const state = transcodeProfiles[profile];
  const dom = getTranscodeDom(profile);
  if (!dom.bar || !dom.percent || !dom.track) {
    return;
  }
  if (totalSegments) {
    const percent = Math.min(100, Math.round((state.done / totalSegments) * 100));
    dom.bar.style.width = `${percent}%`;
    dom.percent.textContent = `${percent}% (${state.done}/${totalSegments})`;
    dom.track.classList.remove("indeterminate");
  } else {
    dom.percent.textContent = `${state.done} done`;
  }
}

function allProfilesDone() {
  if (!totalSegments) {
    return false;
  }
  return ["low", "medium", "high"].every((profile) => transcodeProfiles[profile].done >= totalSegments);
}

function tryFinalizeSuccess() {
  if (processingComplete || !sourceSegmentsComplete || !allProfilesDone()) {
    return;
  }
  processingComplete = true;
  setDoneMessage("Upload and transcoding complete.", { success: true });
  uploadBtn.disabled = false;
  uploadInFlight = false;
  refreshReadyList();
  setPlayerStatus("Ready to play. Select a video and press Play.", { success: true });
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

function setPlayerStatus(text, { success = false, hidden = false } = {}) {
  if (!playerStatus) {
    return;
  }
  playerStatus.textContent = text;
  playerStatus.classList.toggle("success", success);
  if (hidden) {
    playerStatus.classList.add("hidden");
  } else {
    playerStatus.classList.remove("hidden");
  }
}

function deriveStreamingUrl(baseUrl, videoId) {
  const scheme = baseUrl.startsWith("https://") ? "https" : "http";
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const streamingPort = "8083";
  return `${scheme}://${host}:${streamingPort}/stream/${videoId}/manifest`;
}

function deriveReadyListUrl(baseUrl) {
  const scheme = baseUrl.startsWith("https://") ? "https" : "http";
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const streamingPort = "8083";
  return `${scheme}://${host}:${streamingPort}/stream/ready`;
}

function teardownPlayer() {
  if (hlsInstance) {
    hlsInstance.destroy();
    hlsInstance = null;
  }
  if (player) {
    player.removeAttribute("src");
    player.load();
  }
  setPlayerStatus("", { hidden: true });
}

function startStreamingPlayback(videoId) {
  if (!player) {
    return;
  }
  const baseUrl = resolveBaseUrl();
  const manifestUrl = deriveStreamingUrl(baseUrl, videoId);
  teardownPlayer();
    setPlayerStatus("", { hidden: true });

  if (window.Hls && window.Hls.isSupported()) {
    hlsInstance = new window.Hls({
      // VOD ABR tuning: keep startup buffer small to avoid aggressive prefetch.
      lowLatencyMode: false,
      enableWorker: true,
      autoStartLoad: true,
      startFragPrefetch: false,
      backBufferLength: 30,
      maxBufferLength: 12,
      maxMaxBufferLength: 18,
      maxBufferSize: 24 * 1000 * 1000,
      capLevelToPlayerSize: true,
      startLevel: -1
    });
    hlsInstance.loadSource(manifestUrl);
    hlsInstance.attachMedia(player);
    hlsInstance.on(window.Hls.Events.MANIFEST_PARSED, () => {
      player.play().catch(() => {});
      setPlayerStatus("", { hidden: true });
    });
    hlsInstance.on(window.Hls.Events.ERROR, () => {
      setPlayerStatus("Streaming error.", { success: false });
    });
    return;
  }

  if (player.canPlayType("application/vnd.apple.mpegurl")) {
    player.src = manifestUrl;
    player.addEventListener("loadedmetadata", () => {
      // Ensure playback starts from the beginning.
      try {
        player.currentTime = 0;
      } catch (_) {
        // Ignore seek failures and still try to play.
      }
      player.play().catch(() => {});
      setPlayerStatus("", { hidden: true });
    }, { once: true });
    player.addEventListener("error", () => {
      setPlayerStatus("Streaming error.", { success: false });
    }, { once: true });
    return;
  }

  setPlayerStatus("Streaming not supported in this browser.", { success: false });
}

function setSelectedVideoId(videoId) {
  selectedVideoId = videoId;
  if (!readyList) {
    return;
  }
  Array.from(readyList.querySelectorAll("li")).forEach((item) => {
    item.classList.toggle("selected", item.dataset.videoId === videoId);
  });
}

function renderReadyList(videos) {
  if (!readyList) {
    return;
  }
  readyList.textContent = "";
  if (!Array.isArray(videos) || videos.length === 0) {
    const empty = document.createElement("li");
    empty.className = "muted";
    empty.textContent = "No completed videos yet.";
    readyList.appendChild(empty);
    return;
  }
  const normalized = videos.map((item) => {
    if (typeof item === "string") {
      return { videoId: item, videoName: item };
    }
    return {
      videoId: item.videoId || item.video_id || item.id,
      videoName: item.videoName || item.video_name || item.name
    };
  }).filter((item) => item.videoId);

  normalized.forEach((video) => {
    const item = document.createElement("li");
    item.dataset.videoId = video.videoId;
    item.textContent = video.videoName || video.videoId;
    item.addEventListener("click", () => setSelectedVideoId(video.videoId));
    readyList.appendChild(item);
  });

  const ids = normalized.map((video) => video.videoId);
  if (currentVideoId && ids.includes(currentVideoId)) {
    setSelectedVideoId(currentVideoId);
  } else if (!selectedVideoId && ids.length > 0) {
    setSelectedVideoId(ids[0]);
  }
}

async function refreshReadyList() {
  const baseUrl = resolveBaseUrl();
  const readyUrl = deriveReadyListUrl(baseUrl);
  try {
    const resp = await fetch(readyUrl);
    if (!resp.ok) {
      setPlayerStatus(`Failed to load ready videos (${resp.status})`, { success: false });
      return;
    }
    const videos = await resp.json();
    renderReadyList(videos);
  } catch (err) {
    setPlayerStatus("Failed to load ready videos.", { success: false });
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
  if (transcodeBlock) {
    transcodeBlock.classList.add("hidden");
  }
  ["low", "medium", "high"].forEach((profile) => {
    const dom = getTranscodeDom(profile);
    if (dom.bar) {
      dom.bar.style.width = "0%";
    }
    if (dom.percent) {
      dom.percent.textContent = "";
    }
    if (dom.track) {
      dom.track.classList.add("indeterminate");
    }
  });
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
    console.log("[upload-ui] ws message raw", event.data);
    appendLog(event.data);
    try {
      const payload = JSON.parse(event.data);
      if (payload && payload.type === "meta" && typeof payload.totalSegments === "number") {
        totalSegments = payload.totalSegments;
        if (processingTrack) {
          processingTrack.classList.remove("indeterminate");
        }
        if (processingPercent) {
          const pct = totalSegments > 0 ? Math.min(100, Math.round((completedSegments / totalSegments) * 100)) : 0;
          processingBar.style.width = `${pct}%`;
          processingPercent.textContent = `${pct}% (${completedSegments}/${totalSegments})`;
        }
        ["low", "medium", "high"].forEach((profile) => updateTranscodeProfileUi(profile));
        return;
      }
      if (payload && payload.type === "progress" && typeof payload.completedSegments === "number") {
        completedSegments = payload.completedSegments;
        if (totalSegments) {
          const percent = Math.min(100, Math.round((completedSegments / totalSegments) * 100));
          processingBar.style.width = `${percent}%`;
          processingPercent.textContent = `${percent}% (${completedSegments}/${totalSegments})`;
          processingTrack.classList.remove("indeterminate");
          if (completedSegments >= totalSegments) {
            sourceSegmentsComplete = true;
            tryFinalizeSuccess();
          }
        } else {
          processingPercent.textContent = `${completedSegments} events`;
        }
        return;
      }
      if (payload && payload.type === "transcode_progress" && payload.profile) {
        const profile = `${payload.profile}`.toLowerCase();
        const state = transcodeProfiles[profile];
        if (!state) {
          return;
        }
        if (typeof payload.doneSegments === "number") {
          state.done = Math.max(state.done, payload.doneSegments);
        }
        if (typeof payload.totalSegments === "number" && payload.totalSegments > 0) {
          totalSegments = payload.totalSegments;
        }
        if (typeof payload.segmentNumber === "number" && payload.segmentNumber >= 0 && payload.state) {
          state.segments.set(payload.segmentNumber, payload.state);
          recalcTranscodeCounters(profile);
        }
        updateTranscodeProfileUi(profile);
        tryFinalizeSuccess();
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
          if (completedSegments >= totalSegments) {
            sourceSegmentsComplete = true;
            tryFinalizeSuccess();
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
    if (!processingComplete && !failureTerminal) {
      if (!retryInFlight) {
        retryInFlight = true;
        uploadBtn.disabled = false;
        uploadInFlight = false;
        retrySecondsLeft = RETRY_TOTAL_SECONDS;
        appendLog("Upload service disconnected. Retrying...", "error");
        scheduleRetry("ws_disconnected");
      }
    }
  });

  ws.addEventListener("error", () => {
    if (token !== wsToken) {
      return;
    }
    appendLog("WebSocket error", "error");
    if (!processingComplete && !failureTerminal) {
      if (!retryInFlight) {
        retryInFlight = true;
        uploadBtn.disabled = false;
        uploadInFlight = false;
        retrySecondsLeft = RETRY_TOTAL_SECONDS;
        appendLog("Upload service error. Retrying...", "error");
        scheduleRetry("ws_error");
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
  const videoName = videoNameInput ? videoNameInput.value.trim() : "";
  if (!videoName) {
    appendLog("Enter a video name before uploading.", "error");
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
  formData.append("name", videoName);
  if (isRetry && currentVideoId) {
    formData.append("videoId", currentVideoId);
    appendLog(`Retrying with existing videoId ${currentVideoId}`);
  }

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
    if (transcodeBlock) {
      transcodeBlock.classList.remove("hidden");
    }
    if (processingPercent) {
      processingPercent.textContent = "0%";
    }
    ["low", "medium", "high"].forEach((profile) => updateTranscodeProfileUi(profile));

    await fetchUploadInfo(baseUrl, currentVideoId, payload.uploadStatusUrl);
    setPlayerStatus("Ready list updates when processing completes.", { success: true });

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
if (playBtn) {
  playBtn.addEventListener("click", () => {
    if (!selectedVideoId) {
      setPlayerStatus("Select a video ID to play.", { success: false });
      return;
    }
    startStreamingPlayback(selectedVideoId);
  });
}
if (refreshReadyBtn) {
  refreshReadyBtn.addEventListener("click", refreshReadyList);
}

refreshReadyList();
