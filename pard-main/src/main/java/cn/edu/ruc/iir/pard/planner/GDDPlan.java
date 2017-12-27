package cn.edu.ruc.iir.pard.planner;

import cn.edu.ruc.iir.pard.sql.tree.Statement;

public abstract class GDDPlan
        extends Plan implements EarlyStopPlan
{
    public GDDPlan(Statement stmt)
    {
        super(stmt);
    }
    public boolean beforeExecution()
    {
        return true;
    }
    public boolean afterExecution(boolean executeSuccess)
    {
        return true;
    }
}
