package com.distributed26.videostreaming.upload.upload;

import com.distributed26.videostreaming.shared.upload.JobTaskBus;
import com.distributed26.videostreaming.shared.upload.JobTaskEvent;
import com.distributed26.videostreaming.shared.upload.JobTaskListener;
import com.distributed26.videostreaming.upload.db.JobTaskRecord;
import com.distributed26.videostreaming.upload.db.JobTaskRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class JobTaskTracker {
    private final JobTaskRepository repository;
    private final JobTaskBus bus;
    private final Set<String> completedTaskIds = Collections.synchronizedSet(new HashSet<>());
    private JobTaskListener listener;
    private String jobId;
    private int numTasks;

    public JobTaskTracker(JobTaskRepository repository, JobTaskBus bus) {
        this.repository = Objects.requireNonNull(repository, "repository is null");
        this.bus = Objects.requireNonNull(bus, "bus is null");
    }

    public void start(String jobId) {
        this.jobId = Objects.requireNonNull(jobId, "jobId is null");
        Optional<JobTaskRecord> record = repository.findByJobId(jobId);
        if (record.isEmpty()) {
            throw new IllegalStateException("No job_tasks row for jobId: " + jobId);
        }
        this.numTasks = record.get().getNumTasks();

        this.listener = this::handleEvent;
        bus.subscribe(jobId, listener);
    }

    public void stop() {
        if (jobId != null && listener != null) {
            bus.unsubscribe(jobId, listener);
        }
        listener = null;
        jobId = null;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public int getCompletedCount() {
        return completedTaskIds.size();
    }

    public boolean isComplete() {
        return numTasks > 0 && getCompletedCount() >= numTasks;
    }

    private void handleEvent(JobTaskEvent event) {
        if (event == null) {
            return;
        }

        String currentJobId = this.jobId;
        if(!Objects.equals(event.getJobId(), currentJobId)){
            return;
        }
        
        String taskId = event.getTaskId();
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        completedTaskIds.add(taskId);
    }
}
