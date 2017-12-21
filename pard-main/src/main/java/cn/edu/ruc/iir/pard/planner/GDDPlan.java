package cn.edu.ruc.iir.pard.planner;

public interface GDDPlan
{
    public boolean beforeExecution();
    public boolean afterExecution(boolean executeSuccess);
}
