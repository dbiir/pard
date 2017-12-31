package cn.edu.ruc.iir.pard.executor.connector;

import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;

/**
 * pard
 *
 * @author guodong
 */
public class QueryTask
        extends Task
{
    private final PlanNode planNode;

    public QueryTask(PlanNode planNode)
    {
        this(null, planNode);
    }

    public QueryTask(String site, PlanNode planNode)
    {
        super(site);
        this.planNode = planNode;
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }
}
