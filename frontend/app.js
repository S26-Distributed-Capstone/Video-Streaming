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
const cancelProcessingBtn = document.getElementById("cancelProcessingBtn");
const processingRouteState = document.getElementById("processingRouteState");
const processingRouteStateTitle = document.getElementById("processingRouteStateTitle");
const processingRouteStateBody = document.getElementById("processingRouteStateBody");
const processingRouteStateStreamBtn = document.getElementById("processingRouteStateStreamBtn");
const player = document.getElementById("player");
const playerStatus = document.getElementById("playerStatus");
const playBtn = document.getElementById("playBtn");
const refreshReadyBtn = document.getElementById("refreshReadyBtn");
const deleteSelectedBtn = document.getElementById("deleteSelectedBtn");
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
let processingRouteBootTimerId = null;
let currentWsUrl = null;
let reconnectAttempts = 0;
let processingRouteBootAttempts = 0;
let hasEverConnectedWebSocket = false;
let retryingMinioConnection = false;
const maxReconnectAttempts = 6;
const reconnectBaseDelayMs = 1000;
const maxUploadRetryAttempts = 5;
const maxProcessingRouteBootAttempts = 8;
let hlsInstance = null;
let selectedVideoId = null;
let readyVideosState = [];
let selectedReadyVideoIds = new Set();
let readyListBusy = false;
let playbackAttemptToken = 0;
let playbackRetryTimerId = null;
const statusUrlStoragePrefix = "upload-status-url:";
const transcodeProfiles = {
  low: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  medium: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() },
  high: { done: 0, transcoding: 0, uploading: 0, failed: 0, segments: new Map() }
};
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function setPlayerVisible(visible) {
  if (!player) {
    return;
  }
  player.classList.toggle("hidden", !visible);
}

function isValidVideoId(videoId) {
  return typeof videoId === "string" && uuidPattern.test(videoId);
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
  document.body.classList.remove("processing-route-terminal");
  if (processingRouteBanner) {
    processingRouteBanner.classList.remove("hidden");
  }
  if (processingRouteLabel) {
    processingRouteLabel.textContent = currentUploadName
      ? `Processing video: ${currentUploadName}`
      : "Processing video\u2026";
  }
  if (cancelProcessingBtn) {
    cancelProcessingBtn.classList.remove("hidden");
    cancelProcessingBtn.disabled = false;
  }
  uploadBar.style.width = "100%";
  uploadPercent.textContent = "100%";
  if (processingBlock) {
    processingBlock.classList.remove("hidden");
  }
  if (transcodeBlock) {
    transcodeBlock.classList.remove("hidden");
  }
  setDoneMessage("", { success: true, hidden: true });
  hideRouteState();
}

function hideRouteState() {
  if (processingRouteState) {
    processingRouteState.classList.add("hidden");
    processingRouteState.classList.remove("error");
  }
  if (processingRouteStateTitle) {
    processingRouteStateTitle.textContent = "";
  }
  if (processingRouteStateBody) {
    processingRouteStateBody.textContent = "";
  }
  if (processingRouteStateStreamBtn) {
    processingRouteStateStreamBtn.classList.add("hidden");
  }
}

function showRouteState(title, body, { tone = "", showStreamButton = false } = {}) {
  document.body.classList.add("processing-route-terminal");
  hideCancelButton();
  disconnectWebSocket();
  if (processingRouteState) {
    processingRouteState.classList.remove("hidden");
    processingRouteState.classList.toggle("error", tone === "error");
  }
  if (processingRouteStateTitle) {
    processingRouteStateTitle.textContent = title;
  }
  if (processingRouteStateBody) {
    processingRouteStateBody.textContent = body;
  }
  if (processingRouteStateStreamBtn) {
    processingRouteStateStreamBtn.classList.toggle("hidden", !showStreamButton);
  }
  setDoneMessage("", { hidden: true });
}

function showRouteTransientState(title, body, { tone = "" } = {}) {
  document.body.classList.remove("processing-route-terminal");
  if (processingRouteState) {
    processingRouteState.classList.remove("hidden");
    processingRouteState.classList.toggle("error", tone === "error");
  }
  if (processingRouteStateTitle) {
    processingRouteStateTitle.textContent = title;
  }
  if (processingRouteStateBody) {
    processingRouteStateBody.textContent = body;
  }
  if (processingRouteStateStreamBtn) {
    processingRouteStateStreamBtn.classList.add("hidden");
  }
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

function clearProcessingRouteBootTimer() {
  processingRouteBootTimerId = clearTimer(processingRouteBootTimerId);
}

function resetWsReconnectState() {
  clearWsReconnectTimer();
  reconnectAttempts = 0;
}

function resetProcessingRouteBootState() {
  clearProcessingRouteBootTimer();
  processingRouteBootAttempts = 0;
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

function hideCancelButton() {
  if (cancelProcessingBtn) {
    cancelProcessingBtn.classList.add("hidden");
    cancelProcessingBtn.disabled = true;
  }
}

function restoreCancelButton() {
  if (cancelProcessingBtn) {
    cancelProcessingBtn.disabled = false;
    cancelProcessingBtn.textContent = "Cancel";
    cancelProcessingBtn.classList.remove("hidden");
  }
}

async function cancelProcessing() {
  if (!currentVideoId) {
    return;
  }
  if (cancelProcessingBtn) {
    cancelProcessingBtn.disabled = true;
    cancelProcessingBtn.textContent = "Cancelling…";
  }
  const videoId = currentVideoId;
  const baseUrl = resolveBaseUrl();
  try {
    const resp = await fetch(`${baseUrl}/upload/${videoId}/fail?reason=user_cancelled`, { method: "POST" });
    if (resp.ok || resp.status === 204) {
      appendLog(`Processing cancelled for video ${videoId}`);
      setDoneMessage("Processing cancelled.", { success: false });
      hideCancelButton();
    } else if (resp.status === 409) {
      appendLog("Video is no longer in an active processing state.", "error");
      setDoneMessage("Video is already completed or failed.", { success: false });
      hideCancelButton();
    } else {
      appendLog(`Cancel request returned ${resp.status}`, "error");
      restoreCancelButton();
    }
  } catch (err) {
    appendLog("Failed to cancel processing.", "error");
    restoreCancelButton();
  }
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
    if (!resp.ok && resp.status !== 409) {
      appendLog(`Failed to persist terminal upload failure (${resp.status})`, "error");
    }
  } catch (err) {
    appendLog("Failed to persist terminal upload failure.", "error");
  }
}

async function exhaustUploadRetries(message, { skipPersist = false } = {}) {
  const failedVideoId = currentVideoId;
  processingComplete = true;
  hideCancelButton();
  resetWsReconnectState();
  disconnectWebSocket();
  uploadBtn.disabled = false;
  uploadInFlight = false;
  resetStateForNextUpload();
  if (!skipPersist) {
    await persistTerminalUploadFailure(failedVideoId);
  }
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
  hasEverConnectedWebSocket = false;
  retryingMinioConnection = false;
  clearProgressRefreshTimer();
  resetWsReconnectState();
  resetProcessingRouteBootState();
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

function applyLiveProgressEvent(payload) {
  if (!payload || typeof payload !== "object") {
    return;
  }
  if (typeof payload.totalSegments === "number" && payload.totalSegments > 0) {
    totalSegments = Math.max(totalSegments || 0, payload.totalSegments);
  }
  if (typeof payload.completedSegments === "number") {
    completedSegments = Math.max(completedSegments, payload.completedSegments);
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
  tryFinalizeSuccess();
}

function applyLiveTranscodeEvent(payload) {
  if (!payload || typeof payload !== "object" || !payload.profile || !transcodeProfiles[payload.profile]) {
    return;
  }
  if (typeof payload.totalSegments === "number" && payload.totalSegments > 0) {
    totalSegments = Math.max(totalSegments || 0, payload.totalSegments);
  }
  const state = transcodeProfiles[payload.profile];
  if (typeof payload.doneSegments === "number") {
    state.done = Math.max(state.done, payload.doneSegments);
  }
  if (typeof payload.segmentNumber === "number" && payload.segmentNumber >= 0 && payload.state) {
    state.segments.set(payload.segmentNumber, payload.state);
    recalcTranscodeCounters(payload.profile);
  }
  if (transcodeBlock) {
    transcodeBlock.classList.remove("hidden");
  }
  updateTranscodeProfileUi(payload.profile);
  tryFinalizeSuccess();
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
    totalSegments = Math.max(totalSegments || 0, payload.totalSegments);
  }

  if (typeof payload.uploadedSegments === "number") {
    completedSegments = Math.max(completedSegments, payload.uploadedSegments);
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
    transcodeProfiles.low.done = Math.max(transcodeProfiles.low.done, transcode.lowDone);
  }
  if (typeof transcode.mediumDone === "number") {
    transcodeProfiles.medium.done = Math.max(transcodeProfiles.medium.done, transcode.mediumDone);
  }
  if (typeof transcode.highDone === "number") {
    transcodeProfiles.high.done = Math.max(transcodeProfiles.high.done, transcode.highDone);
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
  hideRouteState();
  hideCancelButton();
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
  const nextMessage = message || "MinIO is down. Waiting for it to come back up.";
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
  playerStatus.textContent = text;
  playerStatus.classList.toggle("success", success);
  if (hidden || !text) {
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

function deriveDeleteVideoUrl(baseUrl, videoId) {
  const scheme = baseUrl.startsWith("https://") ? "https" : "http";
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const streamingPort = "8083";
  return `${scheme}://${host}:${streamingPort}/stream/${encodeURIComponent(videoId)}`;
}

function deriveBulkDeleteUrl(baseUrl) {
  const scheme = baseUrl.startsWith("https://") ? "https" : "http";
  const host = baseUrl.replace(/^https?:\/\//, "").replace(/:\d+$/, "");
  const streamingPort = "8083";
  return `${scheme}://${host}:${streamingPort}/stream`;
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

function setReadyListBusy(isBusy) {
  readyListBusy = isBusy;
  if (refreshReadyBtn) {
    refreshReadyBtn.disabled = isBusy;
  }
  updateDeleteSelectedButton();
  if (!readyList) {
    return;
  }
  Array.from(readyList.querySelectorAll("input, button")).forEach((control) => {
    control.disabled = isBusy;
  });
}

function updateDeleteSelectedButton() {
  if (!deleteSelectedBtn) {
    return;
  }
  const hasSelection = selectedReadyVideoIds.size > 0;
  deleteSelectedBtn.classList.toggle("hidden", !hasSelection);
  deleteSelectedBtn.disabled = readyListBusy || !hasSelection;
}

function createTrashIcon() {
  const template = document.createElement("template");
  template.innerHTML = `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v8h-2V9zm4 0h2v8h-2V9zM7 9h2v8H7V9zm1 12a2 2 0 0 1-2-2V8h12v11a2 2 0 0 1-2 2H8z"></path>
    </svg>
  `;
  return template.content.firstElementChild;
}

function normalizeReadyVideos(videos) {
  if (!Array.isArray(videos)) {
    return [];
  }
  return videos.map((item) => {
    if (typeof item === "string") {
      return { videoId: item, videoName: item };
    }
    return {
      videoId: item.videoId || item.video_id || item.id,
      videoName: item.videoName || item.video_name || item.name
    };
  }).filter((item) => item.videoId);
}

function applyReadyVideoRemoval(videoIds) {
  const removed = new Set(videoIds);
  readyVideosState = readyVideosState.filter((video) => !removed.has(video.videoId));
  selectedReadyVideoIds = new Set(Array.from(selectedReadyVideoIds).filter((videoId) => !removed.has(videoId)));
  if (selectedVideoId && removed.has(selectedVideoId)) {
    selectedVideoId = null;
    teardownPlayer();
  }
}

function renderReadyList(videos) {
  if (!readyList) {
    return;
  }
  readyVideosState = normalizeReadyVideos(videos);
  selectedReadyVideoIds = new Set(Array.from(selectedReadyVideoIds)
    .filter((videoId) => readyVideosState.some((video) => video.videoId === videoId)));
  readyList.textContent = "";
  if (readyVideosState.length === 0) {
    const empty = document.createElement("li");
    empty.className = "muted";
    empty.textContent = "No completed videos yet.";
    readyList.appendChild(empty);
    updateDeleteSelectedButton();
    return;
  }

  readyVideosState.forEach((video) => {
    const item = document.createElement("li");
    item.dataset.videoId = video.videoId;

    const row = document.createElement("div");
    row.className = "ready-list-row";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "ready-list-checkbox";
    checkbox.checked = selectedReadyVideoIds.has(video.videoId);
    checkbox.setAttribute("aria-label", `Select ${video.videoName || video.videoId}`);
    checkbox.addEventListener("click", (event) => event.stopPropagation());
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) {
        selectedReadyVideoIds.add(video.videoId);
      } else {
        selectedReadyVideoIds.delete(video.videoId);
      }
      updateDeleteSelectedButton();
    });

    const label = document.createElement("button");
    label.type = "button";
    label.className = "ready-list-label";
    label.addEventListener("click", () => setSelectedVideoId(video.videoId));

    const name = document.createElement("span");
    name.className = "ready-list-name";
    name.textContent = video.videoName || video.videoId;

    label.appendChild(name);

    const deleteBtn = document.createElement("button");
    deleteBtn.type = "button";
    deleteBtn.className = "icon-button";
    deleteBtn.setAttribute("aria-label", `Delete ${video.videoName || video.videoId}`);
    deleteBtn.appendChild(createTrashIcon());
    deleteBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      await deleteSingleVideo(video);
    });

    row.appendChild(checkbox);
    row.appendChild(label);
    row.appendChild(deleteBtn);
    item.appendChild(row);
    readyList.appendChild(item);
  });

  const ids = readyVideosState.map((video) => video.videoId);
  if (currentVideoId && ids.includes(currentVideoId)) {
    setSelectedVideoId(currentVideoId);
  } else if (selectedVideoId && !ids.includes(selectedVideoId)) {
    setSelectedVideoId(ids[0] || null);
  } else if (!selectedVideoId && ids.length > 0) {
    setSelectedVideoId(ids[0]);
  }
  updateDeleteSelectedButton();
  setReadyListBusy(readyListBusy);
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

async function deleteSingleVideo(video) {
  const label = video.videoName || video.videoId;
  if (!window.confirm(`Delete "${label}" from object storage?`)) {
    return;
  }
  setReadyListBusy(true);
  try {
    const resp = await fetch(deriveDeleteVideoUrl(resolveBaseUrl(), video.videoId), {
      method: "DELETE"
    });
    if (!resp.ok) {
      await refreshReadyList();
      setPlayerStatus(`Failed to delete video (${resp.status})`, { success: false });
      return;
    }
    applyReadyVideoRemoval([video.videoId]);
    renderReadyList(readyVideosState);
  } catch (_) {
    await refreshReadyList();
    setPlayerStatus("Failed to delete video.", { success: false });
  } finally {
    setReadyListBusy(false);
  }
}

async function deleteSelectedVideos() {
  const videoIds = Array.from(selectedReadyVideoIds);
  if (videoIds.length === 0) {
    return;
  }
  const message = videoIds.length === 1
    ? "Delete the selected video from object storage?"
    : `Delete ${videoIds.length} selected videos from object storage?`;
  if (!window.confirm(message)) {
    return;
  }
  setReadyListBusy(true);
  try {
    const resp = await fetch(deriveBulkDeleteUrl(resolveBaseUrl()), {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ videoIds })
    });
    if (!resp.ok) {
      await refreshReadyList();
      setPlayerStatus(`Failed to delete videos (${resp.status})`, { success: false });
      return;
    }
    applyReadyVideoRemoval(videoIds);
    renderReadyList(readyVideosState);
  } catch (_) {
    await refreshReadyList();
    setPlayerStatus("Failed to delete videos.", { success: false });
  } finally {
    setReadyListBusy(false);
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
  const shouldNotifyUser = hasEverConnectedWebSocket;
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
      if (shouldNotifyUser) {
        setDoneMessage("Status service unavailable. Please reconnect or retry.", { success: false });
      }
      appendLog("WebSocket reconnect exhausted", "error");
    },
    beforeSchedule: ({ attempt, maxAttempts, delayMs }) => {
      const retryWord = attempt === maxAttempts ? "final retry" : `retry ${attempt}/${maxAttempts}`;
      reconnectAttempts = attempt;
      if (shouldNotifyUser) {
        setDoneMessage(`Status service connection lost. Reconnecting (${retryWord})...`, { success: false });
      }
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
    hasEverConnectedWebSocket = true;
    resetWsReconnectState();
    appendLog("WebSocket connected");
    setDoneMessage("", { success: true, hidden: true });
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
        totalSegments = Math.max(totalSegments || 0, payload.totalSegments);
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "progress" && typeof payload.completedSegments === "number") {
        applyLiveProgressEvent(payload);
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "transcode_progress" && payload.profile) {
        applyLiveTranscodeEvent(payload);
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "storage_status") {
        updateStorageRetryUi({
          retrying: payload.state === "WAITING",
          message: payload.state === "WAITING" ? "MinIO is down. Waiting for it to come back up." : ""
        });
        scheduleProgressRefresh();
        return;
      }
      if (payload && payload.type === "failed") {
        if (payload.reason === "user_cancelled") {
          void exhaustUploadRetries("Processing cancelled.", { skipPersist: true });
          return;
        }
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
      return { ok: false, status: resp.status, text };
    }
    const payload = JSON.parse(text);
    if (infoBox) {
      renderJson(infoBox, payload);
    }
    if (getProcessingRouteVideoId() === videoId) {
      if (!handleProcessingRouteStatus(payload)) {
        return { ok: true, status: resp.status, payload };
      }
      hideRouteState();
      resetProcessingRouteBootState();
    }
    applyUploadInfoSnapshot(payload);
    return { ok: true, status: resp.status, payload };
  } catch (err) {
    if (infoBox) {
      infoBox.textContent = `Upload info error: ${err}`;
    }
    return { ok: false, status: 0, text: String(err) };
  }
}

function handleProcessingRouteLoadFailure(videoId, result) {
  const status = result ? result.status : 0;
  if (status === 400 || !isValidVideoId(videoId)) {
    showRouteState(
      "Invalid video ID",
      "This processing link is malformed. Check the URL and try again.",
      { tone: "error" }
    );
    return;
  }
  if (status === 404) {
    showRouteState(
      "Video not found",
      "No video exists for this processing link. It may have been deleted or never created.",
      { tone: "error" }
    );
    return;
  }
  showRouteState(
    "Processing status unavailable",
    "The page could not load the current video state. Try refreshing when the status service is available.",
    { tone: "error" }
  );
}

function handleProcessingRouteStatus(payload) {
  const status = String(payload.status || "").toUpperCase();
  if (status === "FAILED") {
    processingComplete = true;
    showRouteState(
      "Processing failed",
      "This video is in a terminal failed state and cannot continue processing from this page.",
      { tone: "error" }
    );
    return false;
  }
  hideRouteState();
  return true;
}

function isTransientProcessingRouteFailure(result) {
  return !result || result.status === 0 || result.status >= 500;
}

function scheduleProcessingRouteBootstrapRetry(videoId, wsUrl) {
  processingRouteBootTimerId = scheduleRetry({
    activeToken: videoId,
    expectedToken: currentVideoId,
    attempt: processingRouteBootAttempts,
    maxAttempts: maxProcessingRouteBootAttempts,
    baseDelayMs: reconnectBaseDelayMs,
    timerId: processingRouteBootTimerId,
    clearTimerFn: clearProcessingRouteBootTimer,
    isCanceled: () => processingComplete || currentVideoId !== videoId,
    onExhausted: () => {
      showRouteState(
        "Processing status unavailable",
        "The page could not load the current video state. Try refreshing when the status service is available.",
        { tone: "error" }
      );
    },
    beforeSchedule: ({ attempt, delayMs }) => {
      processingRouteBootAttempts = attempt;
      showRouteTransientState(
        "Connecting to status service",
        `Waiting for the latest processing state. Retrying in ${Math.round(delayMs / 1000)}s...`
      );
    },
    run: () => {
      fetchUploadInfo(resolveBaseUrl(), videoId, wsUrl).then((result) => {
        if (!result || result.ok) {
          return;
        }
        if (!isTransientProcessingRouteFailure(result)) {
          handleProcessingRouteLoadFailure(videoId, result);
          return;
        }
        scheduleProcessingRouteBootstrapRetry(videoId, wsUrl);
      }).catch(() => {
        scheduleProcessingRouteBootstrapRetry(videoId, wsUrl);
      });
    }
  });
}

async function bootProcessingRoute(routeVideoId) {
  currentVideoId = routeVideoId;
  setActiveTab("upload");
  enterProcessingRoute(routeVideoId);

  if (!isValidVideoId(routeVideoId)) {
    handleProcessingRouteLoadFailure(routeVideoId, { status: 400 });
    return;
  }

  const routeStatusUrl = getProcessingRouteStatusUrl(routeVideoId);
  const baseUrl = resolveBaseUrl();
  const wsUrl = routeStatusUrl || deriveWsUrl(baseUrl, routeVideoId);
  connectWebSocket(wsUrl, routeVideoId);
  const result = await fetchUploadInfo(baseUrl, routeVideoId, wsUrl);
  if (!result || !result.ok) {
    if (isTransientProcessingRouteFailure(result)) {
      scheduleProcessingRouteBootstrapRetry(routeVideoId, wsUrl);
      return;
    }
    handleProcessingRouteLoadFailure(routeVideoId, result);
    return;
  }

  if (!handleProcessingRouteStatus(result.payload)) {
    return;
  }
  resetProcessingRouteBootState();
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
if (cancelProcessingBtn) {
  cancelProcessingBtn.addEventListener("click", cancelProcessing);
}
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
if (deleteSelectedBtn) {
  deleteSelectedBtn.addEventListener("click", deleteSelectedVideos);
}
if (processingRouteStateStreamBtn) {
  processingRouteStateStreamBtn.addEventListener("click", () => setActiveTab("stream"));
}

refreshReadyList();
const routeVideoId = getProcessingRouteVideoId();
if (routeVideoId) {
  bootProcessingRoute(routeVideoId).catch(() => {
    handleProcessingRouteLoadFailure(routeVideoId, { status: 0 });
  });
} else {
  setActiveTab("upload");
}
setPlayerVisible(false);
