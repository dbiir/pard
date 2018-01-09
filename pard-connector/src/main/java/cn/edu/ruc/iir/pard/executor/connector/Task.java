package cn.edu.ruc.iir.pard.executor.connector;

import java.io.Serializable;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
        implements Serializable
{
    private static final long serialVersionUID = 2852783693682417924L;

    private String taskId;
    protected final String site;

    public Task(String site)
    {
        this.site = site;
    }

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
