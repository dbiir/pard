package cn.edu.ruc.iir.pard.planner.ddl;

import cn.edu.ruc.iir.pard.planner.GDDPlan;
import cn.edu.ruc.iir.pard.sql.tree.Statement;

public abstract class SchemaPlan
        extends GDDPlan
{
    public SchemaPlan(Statement stmt)
    {
        super(stmt);
        // TODO Auto-generated constructor stub
    }
    protected boolean alreadyDone = false;
    public GDDPlan getGDDPlan()
    {
        return this;
    }
    public boolean isAlreadyDone()
    {
        return alreadyDone;
    }
}
