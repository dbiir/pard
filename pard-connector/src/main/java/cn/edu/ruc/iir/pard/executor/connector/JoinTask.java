package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;

public class JoinTask
        extends Task
{
    /**
     *
     */
    private static final long serialVersionUID = -5881505715821305268L;
    private String taskId;
    private PlanNode node;
    private String tmpTableName;
    public JoinTask(String site)
    {
        super(site);
        this.tmpTableName = null;
        this.node = null;
    }
    public JoinTask(String site, PlanNode node)
    {
        super(site);
        this.node = node;
        this.tmpTableName = null;
    }
    public JoinTask(String site, PlanNode node, String tmpTableName)
    {
        super(site);
        this.node = node;
        this.tmpTableName = tmpTableName;
    }
    public String getTaskId()
    {
        return taskId;
    }
    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }
    public PlanNode getNode()
    {
        return node;
    }
    public void setNode(PlanNode node)
    {
        this.node = node;
    }
    public String getTmpTableName()
    {
        return tmpTableName;
    }
    public void setTmpTableName(String tmpTableName)
    {
        this.tmpTableName = tmpTableName;
    }
}
