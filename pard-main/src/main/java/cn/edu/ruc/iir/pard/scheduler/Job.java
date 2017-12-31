package cn.edu.ruc.iir.pard.scheduler;

import cn.edu.ruc.iir.pard.executor.connector.Task;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * pard
 *
 * @author guodong
 */
public class Job
{
    private final String jobId;
    private String sql;
    private Statement statement;
    private Plan plan;
    private List<Task> tasks;
    private JobScheduler.JobState jobState;

    public Job(String jobId)
    {
        this.jobId = jobId;
        this.jobState = JobScheduler.JobState.BEGIN;
        this.tasks = new ArrayList<>();
    }

    public String getJobId()
    {
        return jobId;
    }

    public void addTask(Task task)
    {
        tasks.add(task);
    }

    public String getSql()
    {
        return sql;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public void setStatement(Statement statement)
    {
        this.statement = statement;
    }

    public Plan getPlan()
    {
        return plan;
    }

    public void setPlan(Plan plan)
    {
        this.plan = plan;
    }

    public JobScheduler.JobState getJobState()
    {
        return jobState;
    }

    public void setJobState(JobScheduler.JobState jobState)
    {
        this.jobState = jobState;
    }

    public List<Task> getTasks()
    {
        return tasks;
    }
}
