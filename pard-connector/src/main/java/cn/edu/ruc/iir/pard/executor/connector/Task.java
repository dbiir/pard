package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
{
    private String taskId;
    protected String site;

    public String getSite()
    {
        return this.site;
    }
}
