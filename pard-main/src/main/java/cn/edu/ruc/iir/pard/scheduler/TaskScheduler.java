package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.commons.utils.PardResultSet;
import cn.edu.ruc.iir.pard.server.PardStartupHook;

/**
 * pard task scheduler.
 * server level.
 *
 * This is just a simple execution controller for job.
 * Cannot do scheduling at all ...
 *
 * @author guodong
 */
public class TaskScheduler
        implements PardStartupHook
{
    private TaskScheduler()
    {}

    @Override
    public void startup() throws RuntimeException
    {
        this.INSTANCE();
    }

    private static final class TaskSchedulerHolder
    {
        private static final TaskScheduler instance = new TaskScheduler();
    }

    public static final TaskScheduler INSTANCE()
    {
        return TaskSchedulerHolder.instance;
    }

    public PardResultSet executeJob(Job job)
    {
        if (job.getJobState() != JobScheduler.JobState.EXECUTING) {
            return new PardResultSet(PardResultSet.ResultStatus.EXECUTING_ERR);
        }
        return new PardResultSet(PardResultSet.ResultStatus.OK);
    }
}
