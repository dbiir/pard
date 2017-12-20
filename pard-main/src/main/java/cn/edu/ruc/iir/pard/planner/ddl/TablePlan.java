package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.planner.ErrorMessage;
import cn.edu.ruc.iir.pard.planner.GDDPlan;
import cn.edu.ruc.iir.pard.planner.Plan;

public abstract class TablePlan
        extends Plan implements GDDPlan
{
    protected boolean alreadyDone = false;
    @Override
    public abstract ErrorMessage semanticAnalysis();
}
