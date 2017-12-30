package cn.edu.ruc.iir.pard.executor.connector;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
{
    private String taskId;
    private int nodeId;

    public int getNodeId()
    {
        return nodeId;
    }
}
