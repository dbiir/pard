package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.GDDPlan;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

public abstract class TablePlan
        extends GDDPlan
{
    public TablePlan(Statement stmt)
    {
        super(stmt);
    }
    protected boolean alreadyDone = false;
    @Override
    public abstract ErrorMessage semanticAnalysis();
}
