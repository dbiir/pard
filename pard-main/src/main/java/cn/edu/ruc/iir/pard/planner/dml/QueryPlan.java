package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.planner.PlanNode;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

/**
 * Query Plan
 * A query statement can be translated to a query plan.
 * @author hagen
 */
public class QueryPlan
        extends Plan
{
    private PlanNode node;
    public QueryPlan(Statement stmt)
    {
        super(stmt);
    }
    public QueryPlan(Statement stmt, PlanNode planNode)
    {
        super(stmt);
        node = planNode;
    }
    public PlanNode getNode()
    {
        return node;
    }
    @Override
    public ErrorMessage semanticAnalysis()
    {
        // TODO semantic analysis
        return null;
    }
    @Override
    public boolean beforeExecution()
    {
        // TODO Auto-generated method stub
        return false;
    }
    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        // TODO Auto-generated method stub
        return false;
    }
    @Override
    public boolean isAlreadyDone()
    {
        // TODO Auto-generated method stub
        return false;
    }
}
