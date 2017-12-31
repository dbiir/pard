package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
{
    protected String taskId;
    protected String site;

    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public String getTaskId()
    {
        return this.taskId;
    }

    public String getSite()
    {
        return this.site;
    }
}
