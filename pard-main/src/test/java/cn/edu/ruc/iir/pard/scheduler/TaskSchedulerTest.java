package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.planner.PardPlanner;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.scheduler.JobScheduler.JobState;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;
import cn.edu.ruc.iir.pard.sql.tree.Statement;
import net.sf.json.JSONArray;
import org.testng.annotations.Test;

import java.util.List;

public class TaskSchedulerTest
{
    SqlParser parser = new SqlParser();
    @Test
    public void test2()
    {
        String sql = "select * from book.publisher";
        Statement stmt = parser.createStatement(sql);
        PardPlanner planner = new PardPlanner();
        Plan plan = planner.plan(stmt);
        plan.setJobId("aa");
        List<Task> task = TaskScheduler.INSTANCE().generateTasks(plan);
        System.out.println(JSONArray.fromObject(task).toString(1));
        Job job = JobScheduler.INSTANCE().newJob();
        task.forEach(job::addTask);
        job.setJobState(JobState.EXECUTING);
        job.setPlan(plan);
        TaskScheduler.INSTANCE().executeJob(job);
    }
}
