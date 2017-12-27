package cn.edu.ruc.iir.pard.planner;

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
    private Statement statement;
    public QueryPlan(Statement stmt)
    {
        super(stmt);
        statement = stmt;
    }
    public PlanNode getNode()
    {
        return node;
    }
    public Statement getStatement()
    {
        return statement;
    }
    @Override
    public ErrorMessage semanticAnalysis()
    {
        // TODO semantic analysis
        return null;
    }

    @Override
    public boolean isAlreadyDone()
    {
        return false;
    }

    @Override
    public boolean beforeExecution()
    {
        return false;
    }

    @Override
    public boolean afterExecution(boolean executeSuccess)
    {
        return false;
    }
}
