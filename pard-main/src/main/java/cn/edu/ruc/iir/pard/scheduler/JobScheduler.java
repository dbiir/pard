package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.commons.config.PardUserConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * pard job scheduler.
 * server level.
 *
 * @author guodong
 */
public class JobScheduler
{
    private final Map<String, Job> currentJobs;
    private final List<Job> doneJobs;
    private final List<Job> abortedJobs;
    private final List<Job> failedJobs;
    private final AtomicLong currentJobSeq;
    private final String nodeName = PardUserConfiguration.INSTANCE().getNodeName();

    public enum JobState
    {
        ABORTED(0), BEGIN(1), PARSING(2), PLANNING(3), SCHEDULING(4), EXECUTING(5), DONE(6), FAILED(-1);

        private int stateId;

        JobState(int stateId)
        {
            this.stateId = stateId;
        }

        @Override
        public String toString()
        {
            return String.valueOf(stateId);
        }

        public JobState getNext()
        {
            if (this.stateId == 1) {
                return PARSING;
            }
            if (this.stateId == 2) {
                return PLANNING;
            }
            if (this.stateId == 3) {
                return SCHEDULING;
            }
            if (this.stateId == 4) {
                return EXECUTING;
            }
            if (this.stateId == 5 || this.stateId == 6) {
                return DONE;
            }
            if (this.stateId == -1) {
                return FAILED;
            }
            return ABORTED;
        }
    }

    private JobScheduler()
    {
        this.currentJobs = new HashMap<>();
        this.doneJobs = new ArrayList<>();
        this.abortedJobs = new ArrayList<>();
        this.failedJobs = new ArrayList<>();
        this.currentJobSeq = new AtomicLong(0L);
    }

    private static final class JobSchedulerHolder
    {
        private static final JobScheduler instance = new JobScheduler();
    }

    public static final JobScheduler INSTANCE()
    {
        return JobSchedulerHolder.instance;
    }

    private String generateJobId()
    {
        return String.format("%s-%d", nodeName, currentJobSeq.getAndIncrement());
    }

    public synchronized Job newJob()
    {
        Job job = new Job(generateJobId());
        currentJobs.putIfAbsent(job.getJobId(), job);
        return job;
    }

    public synchronized void updateJob(String jobId)
    {
        Job job = currentJobs.get(jobId);
        if (job != null) {
            JobState nextState = job.getJobState().getNext();
            job.setJobState(nextState);
            if (nextState == JobState.DONE) {
                doneJobs.add(currentJobs.remove(jobId));
            }
        }
    }

    public synchronized void abortJob(String jobId)
    {
        Job job = currentJobs.get(jobId);
        if (job != null) {
            job.setJobState(JobState.ABORTED);
            abortedJobs.add(currentJobs.remove(jobId));
        }
    }

    public synchronized void failJob(String jobId)
    {
        Job job = currentJobs.get(jobId);
        if (job != null) {
            job.setJobState(JobState.FAILED);
            failedJobs.add(currentJobs.remove(jobId));
        }
    }

    public synchronized Job getJob(String jobId)
    {
        return currentJobs.get(jobId);
    }

    public synchronized List<Job> getDoneJobs()
    {
        return doneJobs;
    }

    public synchronized List<Job> getAbortedJobs()
    {
        return abortedJobs;
    }

    public synchronized List<Job> getFailedJobs()
    {
        return failedJobs;
    }
}
