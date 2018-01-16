package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.commons.exception.ErrorMessage;
import cn.edu.ruc.iir.pard.executor.connector.node.PlanNode;
import cn.edu.ruc.iir.pard.sql.parser.SqlParser;

public class QueryTestPlan
        extends QueryPlan
{
    private PlanNode node;
    public QueryTestPlan(PlanNode node, String info)
    {
        super(new SqlParser().createStatement("select * from " + info));
        this.node = node;
    }

    @Override
    public ErrorMessage semanticAnalysis()
    {
        return new ErrorMessage();
    }

    public PlanNode getPlan()
    {
        return node;
    }
}
