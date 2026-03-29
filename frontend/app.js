const fileInput = document.getElementById("fileInput");
const fileNameLabel = document.getElementById("fileNameLabel");
const videoNameInput = document.getElementById("videoName");
const uploadBtn = document.getElementById("uploadBtn");
const uploadTabBtn = document.getElementById("uploadTabBtn");
const streamTabBtn = document.getElementById("streamTabBtn");
const uploadTab = document.getElementById("uploadTab");
const streamTab = document.getElementById("streamTab");
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
const processingRouteBanner = document.getElementById("processingRouteBanner");
const processingRouteLabel = document.getElementById("processingRouteLabel");
const player = document.getElementById("player");
const playerStatus = document.getElementById("playerStatus");
const playBtn = document.getElementById("playBtn");
const refreshReadyBtn = document.getElementById("refreshReadyBtn");
const readyList = document.getElementById("readyList");

let ws = null;
let currentVideoId = null;
let currentUploadFile = null;
let currentUploadName = "";
let totalSegments = null;
let completedSegments = 0;
let sourceSegmentsComplete = false;
let processingComplete = false;
let uploadInFlight = false;
let uploadRetryCount = 0;
let wsToken = 0;
let progressRefreshTimerId = null;
let wsReconnectTimerId = null;
let uploadRetryTimerId = null;
let currentWsUrl = null;
let reconnectAttempts = 0;
let retryingMinioConnection = false;
const maxReconnectAttempts = 6;
const reconnectBaseDelayMs = 1000;
const maxUploadRetryAttempts = 5;
let hlsInstance = null;
let selectedVideoId = null;
let playbackAttemptToken = 0;
let playbackRetryTimerId = null;
const statusUrlStoragePrefix = "upload-status-url:";
const transcodeProfiles = {
  low: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  medium: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  high: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() }
};

function setPlayerVisible(visible) {
  if (!player) {
    return;
  }
  player.classList.toggle("hidden", !visible);
}

function getProcessingRouteVideoId() {
  const match = window.location.pathname.match(/^\/processing\/([^/]+)$/);
  if (!match) {
    return null;
  }
  try {
    return decodeURIComponent(match[1]);
  } catch (_) {
    return match[1];
  }
}

function getStatusUrlStorageKey(videoId) {
  return `${statusUrlStoragePrefix}${videoId}`;
}

function persistStatusUrl(videoId, uploadStatusUrl) {
  if (!videoId || !uploadStatusUrl || !window.sessionStorage) {
    return;
  }
  try {
    window.sessionStorage.setItem(getStatusUrlStorageKey(videoId), uploadStatusUrl);
  } catch (_) {
    // Ignore storage failures and continue with in-memory state.
  }
}

function readPersistedStatusUrl(videoId) {
  if (!videoId || !window.sessionStorage) {
    return "";
  }
  try {
    return window.sessionStorage.getItem(getStatusUrlStorageKey(videoId)) || "";
  } catch (_) {
    return "";
  }
}

function getProcessingRouteStatusUrl(videoId) {
  try {
    const params = new URLSearchParams(window.location.search);
    const statusUrl = params.get("statusUrl") || "";
    if (statusUrl) {
      persistStatusUrl(videoId, statusUrl);
      return statusUrl;
    }
  } catch (_) {
    // Ignore malformed query strings and fall back to session storage.
  }
  return readPersistedStatusUrl(videoId);
}

function enterProcessingRoute(videoId) {
  if (!videoId) {
    return;
  }
  document.body.classList.add("processing-route");
  if (processingRouteBanner) {
    processingRouteBanner.classList.remove("hidden");
  }
  if (processingRouteLabel) {
    processingRouteLabel.textContent = currentUploadName
      ? `Processing video: ${currentUploadName}`
      : "Processing video\u2026";
  }
  uploadBar.style.width = "100%";
  uploadPercent.textContent = "100%";
  if (processingBlock) {
    processingBlock.classList.remove("hidden");
  }
  if (transcodeBlock) {
    transcodeBlock.classList.remove("hidden");
  }
  setDoneMessage("Upload accepted. Processing in progress.", { success: true });
}

function setActiveTab(tab) {
  const isUpload = tab === "upload";
  if (uploadTab) {
    uploadTab.classList.toggle("hidden", !isUpload);
  }
  if (streamTab) {
    streamTab.classList.toggle("hidden", isUpload);
  }
  if (uploadTabBtn) {
    uploadTabBtn.classList.toggle("active", isUpload);
    uploadTabBtn.setAttribute("aria-selected", String(isUpload));
  }
  if (streamTabBtn) {
    streamTabBtn.classList.toggle("active", !isUpload);
    streamTabBtn.setAttribute("aria-selected", String(!isUpload));
  }
}

function clearProgressRefreshTimer() {
  if (progressRefreshTimerId) {
    clearTimeout(progressRefreshTimerId);
    progressRefreshTimerId = null;
  }
}

function clearTimer(timerId) {
  if (timerId) {
    clearTimeout(timerId);
  }
  return null;
}

function clearWsReconnectTimer() {
  wsReconnectTimerId = clearTimer(wsReconnectTimerId);
}

function clearUploadRetryTimer() {
  uploadRetryTimerId = clearTimer(uploadRetryTimerId);
}

function clearPlaybackRetryTimer() {
  playbackRetryTimerId = clearTimer(playbackRetryTimerId);
}

function resetWsReconnectState() {
  clearWsReconnectTimer();
  reconnectAttempts = 0;
}

function disconnectWebSocket() {
  const priorWs = ws;
  wsToken += 1;
  clearWsReconnectTimer();
  clearUploadRetryTimer();
  ws = null;
  currentWsUrl = null;
  if (priorWs) {
    priorWs.close();
  }
}

function resetUploadRetryState() {
  clearUploadRetryTimer();
  uploadRetryCount = 0;
}

function isUploadRetryableFailure(payload) {
  if (!payload || payload.type !== "failed") {
    return false;
  }
  const reason = String(payload.reason || "").toLowerCase();
  return reason.includes("upload");
}

function shouldIgnoreUploadRetryableFailure(payload) {
  if (!isUploadRetryableFailure(payload)) {
    return false;
  }
  return Boolean(sourceSegmentsComplete && totalSegments && completedSegments >= totalSegments);
}

async function persistTerminalUploadFailure(videoId) {
  if (!videoId) {
    return;
  }
  const baseUrl = resolveBaseUrl();
  const failUrl = `${baseUrl}/upload/${videoId}/fail`;
  try {
    const resp = await fetch(failUrl, { method: "POST" });
    if (!resp.ok) {
      appendLog(`Failed to persist terminal upload failure (${resp.status})`, "error");
    }
  } catch (err) {
    appendLog("Failed to persist terminal upload failure.", "error");
  }
}

async function exhaustUploadRetries(message) {
  const failedVideoId = currentVideoId;
  processingComplete = true;
  resetWsReconnectState();
  disconnectWebSocket();
  uploadBtn.disabled = false;
  uploadInFlight = false;
  resetStateForNextUpload();
  await persistTerminalUploadFailure(failedVideoId);
  if (doneMessage) {
    setDoneMessage(message || "Upload failed.", { success: false });
  }
}

function triggerUploadRetry(reason) {
  if (!currentVideoId || !currentUploadFile || !currentUploadName) {
    void exhaustUploadRetries("Upload failed.");
    return;
  }
  processingComplete = false;
  resetWsReconnectState();
  disconnectWebSocket();
  uploadInFlight = false;
  uploadBtn.disabled = true;
  const videoId = currentVideoId;
  const reasonSuffix = reason ? ` (${reason})` : "";

  uploadRetryTimerId = scheduleRetry({
    activeToken: videoId,
    expectedToken: currentVideoId,
    attempt: uploadRetryCount,
    maxAttempts: maxUploadRetryAttempts,
    baseDelayMs: reconnectBaseDelayMs,
    timerId: uploadRetryTimerId,
    clearTimerFn: clearUploadRetryTimer,
    isCanceled: () => processingComplete || currentVideoId !== videoId,
    onExhausted: () => {
      appendLog(`Upload retry limit reached for videoId ${videoId}`, "error");
      void exhaustUploadRetries("Upload failed.");
    },
    beforeSchedule: ({ attempt, maxAttempts, delayMs }) => {
      uploadRetryCount = attempt;
      appendLog(
        `Upload interrupted${reasonSuffix}. Retrying ${attempt}/${maxAttempts} in ${Math.round(delayMs / 1000)}s with videoId ${videoId}`
      );
      setDoneMessage(`Upload interrupted. Retrying ${attempt}/${maxAttempts}...`, { success: false });
    },
    run: () => {
      uploadFile({ preserveLog: true, isRetry: true });
    }
  });
}

function exponentialBackoffDelayMs(attempt, baseDelayMs = 1000) {
  return baseDelayMs * (2 ** Math.max(0, attempt - 1));
}

function scheduleRetry({
  activeToken,
  expectedToken,
  attempt,
  maxAttempts,
  baseDelayMs = 1000,
  timerId,
  clearTimerFn,
  isCanceled = () => false,
  onExhausted,
  beforeSchedule,
  run
}) {
  if (activeToken !== expectedToken || isCanceled()) {
    return null;
  }
  if (timerId) {
    return timerId;
  }
  if (attempt >= maxAttempts) {
    if (onExhausted) {
      onExhausted();
    }
    return null;
  }
  const nextAttempt = attempt + 1;
  const delayMs = exponentialBackoffDelayMs(nextAttempt, baseDelayMs);
  if (beforeSchedule) {
    beforeSchedule({ attempt: nextAttempt, maxAttempts, delayMs });
  }
  if (clearTimerFn) {
    clearTimerFn();
  }
  return setTimeout(() => {
    if (activeToken !== expectedToken || isCanceled()) {
      return;
    }
    run({ attempt: nextAttempt, delayMs });
  }, delayMs);
}

function resetStateForNextUpload() {
  totalSegments = null;
  completedSegments = 0;
  sourceSegmentsComplete = false;
  processingComplete = false;
  retryingMinioConnection = false;
  clearProgressRefreshTimer();
  resetWsReconnectState();
  currentWsUrl = null;
  ["low", "medium", "high"].forEach((profile) => {
    const state = transcodeProfiles[profile];
    state.done = 0;
    state.transcoding = 0;
    state.uploading = 0;
    state.failed = 0;
    state.segments = new Map();
  });
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

function applyUploadInfoSnapshot(payload) {
  if (!payload || typeof payload !== "object") {
    return;
  }

  if (payload.videoName && processingRouteLabel) {
    processingRouteLabel.textContent = `Processing video: ${payload.videoName}`;
  }

  updateStorageRetryUi({
    retrying: payload.retryingMinioConnection,
    message: payload.statusMessage
  });

  if (typeof payload.totalSegments === "number" && payload.totalSegments >= 0) {
    totalSegments = payload.totalSegments;
  }

  if (typeof payload.uploadedSegments === "number") {
    completedSegments = payload.uploadedSegments;
    if (processingBlock) {
      processingBlock.classList.remove("hidden");
    }
    if (totalSegments) {
      const percent = Math.min(100, Math.round((completedSegments / totalSegments) * 100));
      processingBar.style.width = `${percent}%`;
      processingPercent.textContent = `${percent}% (${completedSegments}/${totalSegments})`;
      processingTrack.classList.remove("indeterminate");
      sourceSegmentsComplete = completedSegments >= totalSegments;
    } else {
      processingPercent.textContent = `${completedSegments} source chunks`;
    }
  }

  const transcode = payload.transcode || {};
  if (typeof transcode.lowDone === "number") {
    transcodeProfiles.low.done = transcode.lowDone;
  }
  if (typeof transcode.mediumDone === "number") {
    transcodeProfiles.medium.done = transcode.mediumDone;
  }
  if (typeof transcode.highDone === "number") {
    transcodeProfiles.high.done = transcode.highDone;
  }
  if (transcodeBlock) {
    transcodeBlock.classList.remove("hidden");
  }
  ["low", "medium", "high"].forEach((profile) => updateTranscodeProfileUi(profile));
  tryFinalizeSuccess();
}

function scheduleProgressRefresh() {
  if (!currentVideoId || progressRefreshTimerId) {
    return;
  }
  progressRefreshTimerId = setTimeout(async () => {
    progressRefreshTimerId = null;
    await fetchUploadInfo(resolveBaseUrl(), currentVideoId, currentWsUrl);
  }, 150);
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
  updateStorageRetryUi({ retrying: false });
  resetUploadRetryState();
  setDoneMessage("Upload, chunking, and transcoding complete.", { success: true });
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

function updateStorageRetryUi({ retrying, message } = {}) {
  if (typeof retrying !== "boolean") {
    return;
  }
  const nextMessage = message || "Retrying MinIO connection";
  if (retrying === retryingMinioConnection) {
    if (retrying && doneMessage && !processingComplete) {
      setDoneMessage(nextMessage, { success: false });
    }
    return;
  }
  retryingMinioConnection = retrying;
  if (retrying) {
    appendLog(nextMessage);
    if (!processingComplete) {
      setDoneMessage(nextMessage, { success: false });
    }
    return;
  }
  appendLog("MinIO connection restored");
  if (!processingComplete) {
    setDoneMessage("Upload complete.", { success: true, hidden: true });
  }
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
  playerStatus.textContent = "";
  playerStatus.classList.add("hidden");
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
  playbackAttemptToken += 1;
  clearPlaybackRetryTimer();
  if (hlsInstance) {
    hlsInstance.destroy();
    hlsInstance = null;
  }
  if (player) {
    player.controls = false;
    player.removeAttribute("src");
    player.load();
  }
  setPlayerVisible(false);
  setPlayerStatus("", { hidden: true });
}

function schedulePlaybackRetry({ token, videoId, attempt, maxAttempts }) {
  playbackRetryTimerId = scheduleRetry({
    activeToken: token,
    expectedToken: playbackAttemptToken,
    attempt,
    maxAttempts,
    timerId: playbackRetryTimerId,
    clearTimerFn: clearPlaybackRetryTimer,
    onExhausted: () => setPlayerStatus("Streaming service unavailable. Please try again.", { success: false }),
    beforeSchedule: () => setPlayerStatus("Streaming service unavailable. Retrying...", { success: false }),
    run: ({ attempt: nextAttempt }) => {
      startStreamingPlayback(videoId, { attempt: nextAttempt, maxAttempts, reuseSession: true });
    }
  });
}

function isRetriableStartupHlsError(data) {
  if (!data || !data.fatal) {
    return false;
  }
  const retriableDetails = new Set([
    "manifestLoadError",
    "manifestLoadTimeOut",
    "levelLoadError",
    "levelLoadTimeOut"
  ]);
  return retriableDetails.has(data.details);
}

function startStreamingPlayback(videoId, { attempt = 1, maxAttempts = 5, reuseSession = false } = {}) {
  if (!player) {
    return;
  }
  const baseUrl = resolveBaseUrl();
  const manifestUrl = deriveStreamingUrl(baseUrl, videoId);
  if (!reuseSession) {
    teardownPlayer();
  } else {
    clearPlaybackRetryTimer();
    if (hlsInstance) {
      hlsInstance.destroy();
      hlsInstance = null;
    }
    player.removeAttribute("src");
    player.load();
    setPlayerVisible(false);
  }
  const token = playbackAttemptToken;
  setPlayerStatus("", { hidden: true });

  if (window.Hls && window.Hls.isSupported()) {
    let startupComplete = false;
    setPlayerVisible(true);
    player.controls = true;
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
      if (token !== playbackAttemptToken) {
        return;
      }
      startupComplete = true;
      player.play().catch(() => {});
      setPlayerStatus("", { hidden: true });
    });
    hlsInstance.on(window.Hls.Events.ERROR, (_, data) => {
      if (token !== playbackAttemptToken) {
        return;
      }
      if (!data || !data.fatal) {
        return;
      }
      if (!startupComplete && isRetriableStartupHlsError(data)) {
        schedulePlaybackRetry({ token, videoId, attempt, maxAttempts });
        return;
      }
      setPlayerStatus("Streaming error.", { success: false });
    });
    return;
  }

  if (player.canPlayType("application/vnd.apple.mpegurl")) {
    let startupComplete = false;
    setPlayerVisible(true);
    player.controls = true;
    player.src = manifestUrl;
    const onLoadedMetadata = () => {
      if (token !== playbackAttemptToken) {
        return;
      }
      startupComplete = true;
      player.removeEventListener("error", onStartupError);
      // Ensure playback starts from the beginning.
      try {
        player.currentTime = 0;
      } catch (_) {
        // Ignore seek failures and still try to play.
      }
      player.play().catch(() => {});
      setPlayerStatus("", { hidden: true });
    };
    const onStartupError = () => {
      if (token !== playbackAttemptToken) {
        return;
      }
      if (!startupComplete) {
        schedulePlaybackRetry({ token, videoId, attempt, maxAttempts });
        return;
      }
      setPlayerStatus("Streaming error.", { success: false });
    };
    player.addEventListener("loadedmetadata", onLoadedMetadata, { once: true });
    player.addEventListener("error", onStartupError, { once: true });
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
  if (preserveRetry) {
    return;
  }
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
  resetStateForNextUpload();
}

function resolveBaseUrl() {
  return window.location.origin;
}

function deriveProcessingPageUrl(videoId, uploadStatusUrl) {
  const url = new URL(`${resolveBaseUrl()}/processing/${encodeURIComponent(videoId)}`);
  if (uploadStatusUrl) {
    url.searchParams.set("statusUrl", uploadStatusUrl);
  }
  return url.toString();
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

function scheduleWebSocketReconnect(wsUrl, videoId, token) {
  if (!wsUrl || !videoId) {
    return;
  }
  wsReconnectTimerId = scheduleRetry({
    activeToken: token,
    expectedToken: wsToken,
    attempt: reconnectAttempts,
    maxAttempts: maxReconnectAttempts,
    baseDelayMs: reconnectBaseDelayMs,
    timerId: wsReconnectTimerId,
    clearTimerFn: clearWsReconnectTimer,
    isCanceled: () => processingComplete,
    onExhausted: () => {
      uploadBtn.disabled = false;
      uploadInFlight = false;
      setDoneMessage("Status service unavailable. Please reconnect or retry.", { success: false });
      appendLog("WebSocket reconnect exhausted", "error");
    },
    beforeSchedule: ({ attempt, maxAttempts, delayMs }) => {
      const retryWord = attempt === maxAttempts ? "final retry" : `retry ${attempt}/${maxAttempts}`;
      reconnectAttempts = attempt;
      setDoneMessage(`Status service connection lost. Reconnecting (${retryWord})...`, { success: false });
      appendLog(`WebSocket reconnect scheduled in ${Math.round(delayMs / 1000)}s`);
    },
    run: () => {
      connectWebSocket(wsUrl, videoId, { isReconnect: true });
    }
  });
}

function connectWebSocket(wsUrl, videoId, { isReconnect = false } = {}) {
  const priorWs = ws;
  currentWsUrl = wsUrl;
  persistStatusUrl(videoId, wsUrl);
  wsToken += 1;
  const token = wsToken;
  if (priorWs) {
    ws = null;
    priorWs.close();
  }
  clearWsReconnectTimer();
  if (ws) {
    ws.close();
  }
  console.log("[upload-ui] connectWebSocket", { wsUrl, videoId, token });
  if (wsUrlLabel) {
    wsUrlLabel.textContent = wsUrl;
  }

  ws = new WebSocket(wsUrl);

  ws.addEventListener("open", () => {
    if (token !== wsToken) {
      return;
    }
    resetWsReconnectState();
    appendLog("WebSocket connected");
    setDoneMessage("Upload complete.", { success: true, hidden: true });
    console.log("[upload-ui] ws open", { wsUrl, videoId, token });
    if (videoId) {
      ws.send(`job:${videoId}`);
      fetchUploadInfo(resolveBaseUrl(), videoId, wsUrl).catch(() => {});
    }
    if (isReconnect) {
      appendLog("WebSocket reconnected");
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
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "progress" && typeof payload.completedSegments === "number") {
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "transcode_progress" && payload.profile) {
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "storage_status") {
        updateStorageRetryUi({
          retrying: payload.state === "WAITING",
          message: payload.state === "WAITING" ? "Retrying MinIO connection" : ""
        });
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "failed") {
        if (isUploadRetryableFailure(payload)) {
          if (shouldIgnoreUploadRetryableFailure(payload)) {
            appendLog("Upload-service interruption detected after source upload completed. Continuing to monitor transcoding.");
            scheduleProgressRefresh();
            return;
          }
          triggerUploadRetry(payload.reason);
          return;
        }
        void exhaustUploadRetries("Upload failed.");
        return;
      }
      if (payload && payload.taskId) {
        scheduleProgressRefresh();
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
    if (!processingComplete) {
      scheduleWebSocketReconnect(wsUrl, videoId, token);
    }
  });

  ws.addEventListener("error", () => {
    if (token !== wsToken) {
      return;
    }
    appendLog("WebSocket error", "error");
    if (!processingComplete) {
      scheduleWebSocketReconnect(wsUrl, videoId, token);
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
    applyUploadInfoSnapshot(payload);
  } catch (err) {
    if (infoBox) {
      infoBox.textContent = `Upload info error: ${err}`;
    }
  }
}

function uploadFile({ preserveLog, isRetry } = {}) {
  if (uploadInFlight || (uploadBtn.disabled && !isRetry)) {
    return;
  }
  console.log("[upload-ui] uploadFile", { isRetry, preserveLog });
  const file = isRetry ? currentUploadFile : fileInput.files[0];
  if (!file) {
    console.error("[upload-ui] uploadFile requires a selected file");
    appendLog("Select a video file before uploading.", "error");
    return;
  }
  const videoName = isRetry ? currentUploadName : (videoNameInput ? videoNameInput.value.trim() : "");
  if (!videoName) {
    console.error("[upload-ui] uploadFile requires videoName");
    appendLog("Enter a video name before uploading.", "error");
    return;
  }

  if (!isRetry) {
    currentUploadFile = file;
    currentUploadName = videoName;
    currentVideoId = null;
    resetUploadRetryState();
  }

  const baseUrl = resolveBaseUrl();
  const uploadUrl = `${baseUrl}/upload`;

  uploadBtn.disabled = true;
  uploadInFlight = true;
  resetProgress({ preserveRetry: isRetry });
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
    uploadPercent.textContent = `${percent}%`;
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
      appendLog(`Upload failed: ${xhr.status}`, "error");
      if (isRetry && currentVideoId) {
        triggerUploadRetry(`HTTP ${xhr.status}`);
        return;
      }
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
      if (isRetry) {
        triggerUploadRetry("missing videoId");
        return;
      }
      if (doneMessage) {
        setDoneMessage("Upload failed.", { success: false });
      }
      uploadBtn.disabled = false;
      uploadInFlight = false;
      resetStateForNextUpload();
      return;
    }

    if (doneMessage) {
      setDoneMessage("Upload complete.", { success: true, hidden: true });
    }

    updateStorageRetryUi({
      retrying: Boolean(payload.retryingMinioConnection),
      message: payload.statusMessage
    });

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

    const uploadStatusUrl = payload.uploadStatusUrl || payload.upload_status_url || "";
    if (uploadStatusUrl) {
      persistStatusUrl(currentVideoId, uploadStatusUrl);
    }
    window.location.assign(deriveProcessingPageUrl(currentVideoId, uploadStatusUrl));
  });

  xhr.addEventListener("error", () => {
    uploadInFlight = false;
    uploadBtn.disabled = false;
    appendLog("Upload failed due to a network error.", "error");
    if (isRetry && currentVideoId) {
      triggerUploadRetry("network error");
      return;
    }
    resetStateForNextUpload();
    if (doneMessage) {
      setDoneMessage("Upload failed.", { success: false });
    }
  });

  xhr.send(formData);
}

uploadBtn.addEventListener("click", uploadFile);
if (fileInput) {
  fileInput.addEventListener("change", () => {
    const file = fileInput.files && fileInput.files[0];
    if (fileNameLabel) {
      fileNameLabel.textContent = file ? file.name : "No file selected";
    }
  });
}
if (uploadTabBtn) {
  uploadTabBtn.addEventListener("click", () => setActiveTab("upload"));
}
if (streamTabBtn) {
  streamTabBtn.addEventListener("click", () => setActiveTab("stream"));
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
const routeVideoId = getProcessingRouteVideoId();
if (routeVideoId) {
  currentVideoId = routeVideoId;
  setActiveTab("upload");
  enterProcessingRoute(routeVideoId);
  const routeStatusUrl = getProcessingRouteStatusUrl(routeVideoId);
  const baseUrl = resolveBaseUrl();
  const wsUrl = routeStatusUrl || deriveWsUrl(baseUrl, routeVideoId);
  fetchUploadInfo(baseUrl, routeVideoId, wsUrl).catch(() => {});
  connectWebSocket(wsUrl, routeVideoId);
} else {
  setActiveTab("upload");
}
setPlayerVisible(false);
