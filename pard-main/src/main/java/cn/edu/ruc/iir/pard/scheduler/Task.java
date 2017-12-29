package cn.edu.ruc.iir.pard.scheduler;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
{
    private final String taskId;
    private int nodeId;

    public Task(String taskId)
    {
        this.taskId = taskId;
    }

    public int getNodeId()
    {
        return nodeId;
    }
}
