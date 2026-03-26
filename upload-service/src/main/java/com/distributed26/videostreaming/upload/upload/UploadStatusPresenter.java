package com.distributed26.videostreaming.upload.upload;

final class UploadStatusPresenter {
    private UploadStatusPresenter() {
    }

    static boolean isRetryingMinioConnection(String status) {
        return "WAITING_FOR_STORAGE".equalsIgnoreCase(status);
    }

    static String statusMessage(String status) {
        if ("WAITING_FOR_STORAGE".equalsIgnoreCase(status)) {
            return "Retrying MinIO connection";
        }
        if ("PROCESSING".equalsIgnoreCase(status)) {
            return "Processing upload";
        }
        if ("COMPLETED".equalsIgnoreCase(status)) {
            return "Upload completed";
        }
        if ("FAILED".equalsIgnoreCase(status)) {
            return "Upload failed";
        }
        return status == null || status.isBlank() ? "Unknown upload status" : status;
    }
}
