package cn.edu.ruc.iir.pard.planner;

public abstract class SchemaPlan
        extends Plan implements GDDPlan
{
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
