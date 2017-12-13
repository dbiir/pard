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
    public QueryPlan(Statement stmt, PlanNode planNode)
    {
        statement = stmt;
        node = planNode;
    }
    public PlanNode getNode()
    {
        return node;
    }
    public Statement getStatement()
    {
        return statement;
    }
}
