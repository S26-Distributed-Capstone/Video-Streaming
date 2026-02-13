package com.distributed26.videostreaming.upload.db;

import com.distributed26.videostreaming.upload.db.JobTaskRecord;
import com.distributed26.videostreaming.upload.db.JobTaskRepository;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JobTaskRepositoryTest {

    @Test
    void findByJobId() {
        String jobId = "job-123";
        JobTaskRepository repo = JobTaskRepository.fromEnv();
        Optional<JobTaskRecord> record = repo.findByJobId(jobId);
        assertTrue(record.isPresent(), "No row found for jobId: " + jobId);
    }
}
