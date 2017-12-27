package cn.edu.ruc.iir.pard.planner.dml;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.Plan;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

public class InsertPlan
        extends Plan
{
    public InsertPlan(Statement stmt)
    {
        super(stmt);
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

    @Override
    public ErrorMessage semanticAnalysis()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
