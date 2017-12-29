package cn.edu.ruc.iir.pard.scheduler;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * pard
 *
 * @author guodong
 */
public class JobSchedulerTest
{
    @Test
    public void testJobState()
    {
        JobScheduler.JobState begin = JobScheduler.JobState.BEGIN;
        JobScheduler.JobState parse = JobScheduler.JobState.PARSING;
        JobScheduler.JobState plan = JobScheduler.JobState.PLANNING;
        JobScheduler.JobState schedule = JobScheduler.JobState.SCHEDULING;
        JobScheduler.JobState execute = JobScheduler.JobState.EXECUTING;
        JobScheduler.JobState done = JobScheduler.JobState.DONE;
        JobScheduler.JobState abort = JobScheduler.JobState.ABORTED;
        JobScheduler.JobState fail = JobScheduler.JobState.FAILED;
        assertEquals(begin.getNext(), parse);
        assertEquals(parse.getNext(), plan);
        assertEquals(plan.getNext(), schedule);
        assertEquals(schedule.getNext(), execute);
        assertEquals(execute.getNext(), done);
        assertEquals(abort.getNext(), abort);
        assertEquals(fail.getNext(), fail);
    }
}
