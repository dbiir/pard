package cn.edu.ruc.iir.pard.scheduler;

/**
 * pard
 *
 * @author guodong
 */
public abstract class Task
{
    private final int nodeId;

    public Task(int nodeId)
    {
        this.nodeId = nodeId;
    }

    public int getNodeId()
    {
        return nodeId;
    }
}
