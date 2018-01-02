package cn.edu.ruc.iir.pard.scheduler;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
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

    @Test
    public void testConsumerAsync()
    {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(8 * 1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return 1;
            });
            future.exceptionally(throwable -> -1);
            future.thenAccept(result::add);
        }
        while (true) {
            if (result.size() == 5) {
                System.out.println("Result: " + result.size());
                for (int i : result) {
                    System.out.println(i);
                }
                break;
            }
            else {
                System.out.println(result.size());
            }
        }
    }

    @Test
    public void testToStringHelper()
    {
        List<String> res = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            res.add(String.valueOf(i));
        }
        String print = toStringHelper(this)
                .add("res", res).toString();
        System.out.println(print);
    }
}
